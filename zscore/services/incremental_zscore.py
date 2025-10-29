import json
import redis
import numpy as np
import msgpack
import time
from typing import Dict, Optional
from django.utils import timezone
from django.conf import settings

from exchange_connections.selectors import (
    get_exchange_symbols,
    get_symbol_kline_data,
    get_historical_kline_data,
)
from zscore.selectors.zscore import get_zscore_history_data
from exchange_connections.constants import KLINE_FIELD_MAP
from core.constants import RedisPubMessages, tf_options
from zscore.models import ZScoreHistory
from exchange_connections.models import Symbol, Exchange, ContractType
from core.redis_config import get_redis_connection
from core.notifications import notification_service
from zscore.services.db_utils import cleanup_old_zscore_data

r = get_redis_connection()


class IncrementalZScore:
    """Calculates Z-score over an incremental window of data points."""

    def __init__(self, window_size):
        self.window_size = window_size
        self.count = 0
        self.mean = 0.0
        self.M2 = 0.0
        self.current_value = None

    def initialize_from_data(self, data):
        arr = np.array(data, dtype=np.float64)

        if arr.size > 0:
            self.count = arr.size
            self.mean = np.mean(arr)
            self.M2 = np.sum((arr - self.mean) ** 2)
            self.current_value = arr[-1]

    def remove_data_point(self, value):
        if self.count <= 1:
            self.count = 0
            self.mean = 0.0
            self.M2 = 0.0

        old_count = self.count
        self.count -= 1
        delta = value - self.mean
        self.mean = (old_count * self.mean - value) / self.count
        self.M2 -= delta * (value - self.mean)

    def add_data_point(self, value):
        self.count += 1
        delta = value - self.mean
        self.mean += delta / self.count
        delta2 = value - self.mean
        self.M2 += delta * delta2
        self.current_value = value

    def update_data_point(self, old_value, new_value):
        if self.count == 0:
            self.add_data_point(new_value)
            return

        delta_old = old_value - self.mean
        self.mean += (new_value - old_value) / self.count
        delta_new = new_value - self.mean
        self.M2 += (new_value - old_value) * (delta_old + delta_new)
        self.current_value = new_value

    def get_z_score(self):
        if self.count < 2 or self.current_value is None:
            return 0

        variance = self.M2 / self.count
        if variance <= 1e-9:
            return 0

        std_dev = np.sqrt(variance)
        z_score = (self.current_value - self.mean) / std_dev

        return z_score


class ZScoreProcessor:
    def __init__(self):
        self.symbols = get_exchange_symbols()
        self.hours_options = list(tf_options["zscore"].values())
        self.incremental_zscores = self.initialize_zscores()

    def initialize_zscores(self):
        """Initialize Z-score objects using historical kline data from DB"""
        zscore_dict = {}
        data_by_hours = {
            hours: get_historical_kline_data(hours=hours, symbols=self.symbols)
            for hours in self.hours_options
        }

        for symbol in self.symbols:
            for data_type in KLINE_FIELD_MAP.keys():
                for hours in self.hours_options:
                    zscore_dict.setdefault(symbol, {}).setdefault(data_type, {})[
                        hours
                    ] = IncrementalZScore(hours * 60)

                    series = data_by_hours[hours].get(symbol, {}).get(data_type, [])

                    if series is not None and len(series) > 0:
                        zscore_dict[symbol][data_type][hours].initialize_from_data(
                            series
                        )
        return zscore_dict

    def update_zscores(
        self, newest_values: Optional[Dict[str, Dict[str, float]]] = None
    ):
        """Update incremental Z-scores with new data"""
        if newest_values is None:
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
                    symbol_new_values = newest_values.get(symbol)
                    if not symbol_new_values or data_type not in symbol_new_values:
                        continue
                    new_val = symbol_new_values[data_type]
                    old_val = oldest_values.get(symbol, {}).get(data_type)

                    if old_val is not None:
                        zscore_obj.update_data_point(old_val, new_val)
                    else:
                        zscore_obj.add_data_point(new_val)

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
                    for data_type in KLINE_FIELD_MAP.keys()
                }
                for symbol in self.symbols
            }
            for hours in self.hours_options
        }

    @staticmethod
    def store_z_score_results(results):
        """Store calculated Z-scores in DB"""
        current_time = timezone.now()
        print("STORING ZSCORES")

        try:
            exchange = Exchange.objects.get(name="binance")
            contract_type = ContractType.objects.get(name="perpetual")
            symbols = Symbol.objects.filter(
                exchange=exchange, contract_type=contract_type
            )
            symbol_map = {s.name: s for s in symbols}
        except (Exchange.DoesNotExist, ContractType.DoesNotExist) as e:
            print("Exchange or ContractType not found:", e)
            return

        db_entries = []

        for hours in results:
            for symbol_name, data in results[hours].items():
                if symbol_name not in symbol_map:
                    continue

                db_entries.append(
                    ZScoreHistory(
                        symbol=symbol_map[symbol_name],
                        volume=data.get("volume", 0),
                        price=data.get("price", 0),
                        trades=data.get("trades", 0),
                        hours=hours,
                        calculated_at=current_time,
                    )
                )

        if db_entries and settings.STORE_TO_DB:
            ZScoreHistory.objects.bulk_create(db_entries, ignore_conflicts=True)

            cleanup_old_zscore_data(retention_hours=25)

    def add_new_symbol(self, symbol_name):
        """Add a new symbol to zscore tracking"""
        if symbol_name in self.symbols:
            print(f"Symbol {symbol_name} already being tracked")
            return

        print(f"Adding {symbol_name} to zscore tracking")

        self.symbols.append(symbol_name)

        for data_type in KLINE_FIELD_MAP.keys():
            for hours in self.hours_options:
                self.incremental_zscores.setdefault(symbol_name, {}).setdefault(
                    data_type, {}
                )[hours] = IncrementalZScore(hours * 60)

                historical_data = get_historical_kline_data(
                    hours=hours, symbols=[symbol_name]
                )
                series = historical_data.get(symbol_name, {}).get(data_type, [])

                if series and len(series) > 0:
                    self.incremental_zscores[symbol_name][data_type][
                        hours
                    ].initialize_from_data(series)

    def remove_symbol(self, symbol_name):
        """Remove a symbol from zscore tracking"""
        if symbol_name not in self.symbols:
            print(f"Symbol {symbol_name} not being tracked")
            return

        print(f"Removing symbol {symbol_name} from zscore tracking")

        self.symbols.remove(symbol_name)

        if symbol_name in self.incremental_zscores:
            del self.incremental_zscores[symbol_name]

    def fetch_and_store_zscore_history_data(self, redis_pipeline):
        for hours in self.hours_options:
            zscore_heatmap_data = get_zscore_history_data(hours)

            redis_pipeline.execute_command(
                "SET",
                f"zscore:heatmap:binance:perpetual:{hours}",
                msgpack.packb(zscore_heatmap_data),
            )

        print("ZSCORE: Stored zscore history data to redis")

    def run(self):
        """Main processing loop listening for Redis updates"""
        retries = 0

        while True:
            try:
                print("Subscribing to Redis channels...")
                pubsub = r.pubsub()
                pubsub.subscribe(
                    RedisPubMessages.KLINE_SAVED_TO_DB.value,
                    RedisPubMessages.SYMBOL_ADDED.value,
                    RedisPubMessages.SYMBOL_DELISTED.value,
                )
                pubsub.get_message()

                for message in pubsub.listen():
                    if message["type"] == "message":
                        channel = message["channel"]

                        if channel == RedisPubMessages.KLINE_SAVED_TO_DB.value:
                            print(f'ZSCORE {message["channel"]}')
                            data_raw = message.get("data")
                            if data_raw is None:
                                print("ZSCORE: Received empty payload")
                                continue

                            if isinstance(data_raw, bytes):
                                data_text = data_raw.decode("utf-8")
                            elif isinstance(data_raw, str):
                                data_text = data_raw
                            else:
                                print(
                                    f"ZSCORE: Unexpected payload type {type(data_raw)}"
                                )
                                continue

                            try:
                                payload = json.loads(data_text)
                            except (TypeError, json.JSONDecodeError):
                                print("ZSCORE: Failed to decode newest values payload")
                                continue

                            newest_values = payload.get("newest_values")
                            if not isinstance(newest_values, dict):
                                print("ZSCORE: No newest_values found in payload")
                                continue

                            self.update_zscores(newest_values=newest_values)
                            results = self.create_z_score_results()

                            pipeline = r.pipeline()
                            for tf, tf_data in results.items():
                                pipeline.execute_command(
                                    "SET",
                                    f"zscore:binance:perpetual:{tf}",
                                    msgpack.packb(tf_data),
                                )
                            self.store_z_score_results(results)
                            self.fetch_and_store_zscore_history_data(
                                redis_pipeline=pipeline
                            )
                            pipeline.execute()

                            notification_service.send_zscore_update()

                        elif channel == RedisPubMessages.SYMBOL_ADDED.value:
                            data = message.get("data", b"").decode("utf-8")
                            print(f"ZSCORE: Received symbol added event: {data}")
                            symbol_name = data.split(":")[0]
                            self.add_new_symbol(symbol_name)

                        elif channel == RedisPubMessages.SYMBOL_DELISTED.value:
                            data = message.get("data", b"").decode("utf-8")
                            print(f"ZSCORE: Received symbol delisted event: {data}")
                            symbol_name = data.split(":")[0]
                            self.remove_symbol(symbol_name)

                retries = 0

            except (redis.ConnectionError, redis.TimeoutError) as e:
                retries += 1
                wait = min(2**retries, 40)
                print(f"[ZScore] Disconnected from Redis: {e}. Retrying in {wait}s...")
                time.sleep(wait)

            except Exception as e:
                print(f"[ZScore] Unexpected error: {e}")
                time.sleep(5)
