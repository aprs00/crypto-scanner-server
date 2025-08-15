import redis
import numpy as np
import msgpack
from django.utils import timezone

from filters.constants import tf_options
from exchange_connections.selectors import (
    get_exchange_symbols,
    get_symbol_kline_data,
    get_historical_kline_data,
)
from exchange_connections.constants import KLINE_FIELD_MAP
from core.constants import RedisPubMessages
from zscore.models import ZScoreHistory
from exchange_connections.models import Symbol, Exchange, ContractType

r = redis.Redis(host="redis")


class IncrementalZScore:
    """Calculates Z-score over an incremental window of data points."""

    def __init__(self, window_size):
        self.window_size = window_size
        self.sum = 0
        self.sum_squared = 0
        self.count = 0
        self.current_value = None

    def initialize_from_data(self, data):
        if data:
            self.count = len(data)
            self.sum = sum(data)
            self.sum_squared = sum(x * x for x in data)
            if data:
                self.current_value = data[-1]

    def remove_data_point(self, value):
        if self.count > 0:
            self.sum -= value
            self.sum_squared -= value * value
            self.count -= 1

    def add_data_point(self, value):
        self.sum += value
        self.sum_squared += value * value
        self.count += 1
        self.current_value = value

    def update_data_point(self, old_value, new_value):
        self.remove_data_point(old_value)
        self.add_data_point(new_value)

    def get_z_score(self):
        if self.count == 0 or self.current_value is None:
            return 0

        mean = self.sum / self.count

        variance = (self.sum_squared / self.count) - (mean * mean)

        if variance <= 0:
            return 0

        std_dev = np.sqrt(variance)

        return (self.current_value - mean) / std_dev


class ZScoreProcessor:
    def __init__(self):
        self.symbols = get_exchange_symbols()
        self.hours_options = list(tf_options["zscore"].values())
        self.incremental_zscores = self.initialize_zscores()

    def initialize_zscores(self):
        """Initialize Z-score objects using historical kline data from DB"""
        dict = {}
        data_by_hours = {
            hours: get_historical_kline_data(hours=hours, symbols=self.symbols)
            for hours in self.hours_options
        }

        for symbol in self.symbols:
            for data_type in KLINE_FIELD_MAP.keys():
                for hours in self.hours_options:
                    dict.setdefault(symbol, {}).setdefault(data_type, {})[hours] = (
                        IncrementalZScore(hours * 60)
                    )
                    series = data_by_hours[hours].get(symbol, {}).get(data_type, [])
                    if series is not None and len(series) > 0:
                        dict[symbol][data_type][hours].initialize_from_data(
                            series.tolist()
                        )
        return dict

    def update_zscores(self):
        """Update incremental Z-scores by removing oldest values and adding new ones"""
        newest_values = get_symbol_kline_data(
            symbols=self.symbols, exchange="binance", contract_type="perpetual"
        )

        for hours in self.hours_options:
            oldest_values = get_symbol_kline_data(
                symbols=self.symbols,
                hours=hours,
                exchange="binance",
                contract_type="perpetual",
            )

            for symbol in self.symbols:
                for data_type in KLINE_FIELD_MAP.keys():
                    zscore_obj = self.incremental_zscores[symbol][data_type][hours]
                    new_value = newest_values[symbol][data_type]
                    old_value = oldest_values.get(symbol, {}).get(data_type)

                    if old_value:
                        zscore_obj.update_data_point(old_value, new_value)
                    else:
                        zscore_obj.add_data_point(new_value)

    def create_z_score_results(self):
        return {
            hours: {
                symbol: {
                    data_type: round(
                        self.incremental_zscores[symbol][data_type][
                            hours
                        ].get_z_score(),
                        2,
                    )
                    for data_type in self.incremental_zscores[symbol].keys()
                }
                for symbol in self.incremental_zscores.keys()
            }
            for hours in self.hours_options
        }

    @staticmethod
    def store_z_score_results(results):
        """Store calculated Z-scores in DB (pulls from `results` dict)"""
        current_time = timezone.now()
        print("STORING ZSCORES")

        try:
            exchange_obj = Exchange.objects.get(name="binance")
            contract_type_obj = ContractType.objects.get(name="perpetual")
        except (Exchange.DoesNotExist, ContractType.DoesNotExist) as e:
            print("Exchange or ContractType not found:", e)
            return

        symbol_objs = {
            s.name: s
            for s in Symbol.objects.filter(
                exchange=exchange_obj, contract_type=contract_type_obj
            )
        }

        db_entries = []
        timeframe_key = 1
        tf_data = results.get(timeframe_key, {})

        for symbol_name, symbol_obj in symbol_objs.items():
            zscores = tf_data.get(symbol_name, {})
            db_entries.append(
                ZScoreHistory(
                    symbol=symbol_obj,
                    volume=zscores.get("volume", 0),
                    price=zscores.get("price", 0),
                    trades=zscores.get("trades", 0),
                    calculated_at=current_time,
                )
            )

        if db_entries:
            ZScoreHistory.objects.bulk_create(db_entries, ignore_conflicts=True)

    def run(self):
        """Main processing loop listening for Redis updates"""
        pubsub = r.pubsub()
        pubsub.subscribe(RedisPubMessages.KLINE_SAVED_TO_DB.value)
        pubsub.get_message()

        for message in pubsub.listen():
            if (
                message["type"] == "message"
                and message["channel"] == RedisPubMessages.KLINE_SAVED_TO_DB.value
            ):
                print(f'ZSCORE {message["channel"]}')
                self.update_zscores()
                results = self.create_z_score_results()

                pipeline = r.pipeline()
                for tf, tf_data in results.items():
                    pipeline.execute_command(
                        "SET",
                        f"zscore:binance:perpetual:{tf}",
                        msgpack.packb(tf_data),
                    )
                self.store_z_score_results(results)
                pipeline.execute()


def initialize_incremental_zscore():
    processor = ZScoreProcessor()
    processor.run()
