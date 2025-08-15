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
    """
    Calculates Z-score over an incremental window of data points.
    """

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


def initialize_zscores(symbols, hours_options):
    """
    Initialize Z-score objects using historical kline data from the database.
    Returns nested dictionary: {symbol: {data_type: {timeframe: ZScore}}}
    """
    dict = {}

    data_by_hours = {
        hours: get_historical_kline_data(hours=hours, symbols=symbols)
        for hours in hours_options
    }

    for symbol in symbols:
        for data_type in KLINE_FIELD_MAP.keys():
            for hours in hours_options:
                dict.setdefault(symbol, {}).setdefault(data_type, {})[hours] = (
                    IncrementalZScore(hours * 60)
                )

                series = data_by_hours[hours].get(symbol, {}).get(data_type, [])
                if series is not None and len(series) > 0:
                    dict[symbol][data_type][hours].initialize_from_data(series.tolist())

    return dict


def update_zscores(incremental_zscores, symbols, hours_options):
    """
    Update incremental Z-scores by removing oldest values and adding new ones.
    """
    newest_values = get_symbol_kline_data(symbols=symbols)

    for symbol in symbols:
        for hours in hours_options:
            oldest_values = get_symbol_kline_data(symbols=symbols, hours=hours)

            for data_type in KLINE_FIELD_MAP.keys():
                zscore_obj = incremental_zscores[symbol][data_type][hours]
                new_value = newest_values[symbol][data_type]

                if oldest_values:
                    old_value = oldest_values[symbol][data_type]
                    zscore_obj.update_data_point(old_value, new_value)
                else:
                    zscore_obj.add_data_point(new_value)


def create_z_score_results(incremental_zscores, hours_options):
    return {
        hours: {
            symbol: {
                data_type: round(
                    incremental_zscores[symbol][data_type][hours].get_z_score(), 2
                )
                for data_type in incremental_zscores[symbol].keys()
            }
            for symbol in incremental_zscores.keys()
        }
        for hours in hours_options
    }


def store_z_score_results(results, symbols, tf):
    """
    Store the calculated Z-score results in the database.
    """
    db_entries = []
    current_time = timezone.now()

    print("store to db invoked")

    for symbol in symbols:
        try:
            symbol_obj = Symbol.objects.get(
                name=symbol,
                exchange=Exchange.objects.get(name="binance"),
                contract_type=ContractType.objects.get(name="perpetual"),
            )

            zscores = results[tf][symbol]

            db_entries.append(
                ZScoreHistory(
                    symbol=symbol_obj,
                    volume=zscores.get("volume", 0),
                    price=zscores.get("price", 0),
                    trades=zscores.get("trades", 0),
                    calculated_at=current_time,
                )
            )
        except (
            Symbol.DoesNotExist,
            Exchange.DoesNotExist,
            ContractType.DoesNotExist,
        ) as e:
            print("Error occurred while storing Z-score results:", e)
            continue

    if db_entries:
        print(f"Storing {len(db_entries)} Z-score entries to the database.")
        ZScoreHistory.objects.bulk_create(db_entries, ignore_conflicts=True)


def initialize_incremental_zscore():
    """
    Calculate and cache Z-scores for all combinations of hours, data types, and symbols.
    """
    symbols = get_exchange_symbols()
    hours_options = list(tf_options["zscore"].values())

    incremental_zscores = initialize_zscores(symbols, hours_options)

    pubsub = r.pubsub()
    pubsub.subscribe(RedisPubMessages.KLINE_SAVED_TO_DB.value)
    pubsub.get_message()

    for message in pubsub.listen():
        if (
            message["type"] == "message"
            and message["channel"] == RedisPubMessages.KLINE_SAVED_TO_DB.value
        ):
            print(f'ZSCORE {message["channel"]}')
            update_zscores(incremental_zscores, symbols, hours_options)

            results = create_z_score_results(incremental_zscores, hours_options)

            pipeline = r.pipeline()

            for tf, tf_data in results.items():
                pipeline.execute_command(
                    "SET",
                    f"zscore:binance:perpetual:{tf}",
                    msgpack.packb(tf_data),
                )

                store_z_score_results(results, symbols, tf)

            pipeline.execute()
