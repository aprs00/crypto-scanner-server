import numpy as np
import msgpack
import threading
from typing import Any, Dict
from django.utils import timezone
from django.conf import settings

from exchange_connections.selectors import (
    get_exchange_symbols,
    get_symbol_kline_data,
    get_historical_kline_data,
)
from zscore.selectors.zscore import get_zscore_history_data
from exchange_connections.constants import KLINE_FIELD_MAP
from core.constants import RedisStreamKeys, EXCHANGE_CONFIG, Exchange
from zscore.models import ZScoreHistory
from exchange_connections.models import Symbol, Exchange as ExchangeModel, ContractType
from core.redis_config import get_redis_connection
from core.redis_streams import StreamConsumer
from core.notifications import notification_service

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
            return

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
    def __init__(self, exchange: Exchange, contract_type: str = "perpetual"):
        self.exchange = exchange
        self.contract_type = contract_type
        self.symbols = get_exchange_symbols(
            exchange=self.exchange, contract_type=self.contract_type
        )
        self.hours_options = list(
            EXCHANGE_CONFIG[self.exchange]["hours_options"]["zscore"].values()
        )
        self.incremental_zscores = self.initialize_zscores()

    def initialize_zscores(self):
        """Initialize Z-score objects using historical kline data from DB"""
        zscore_dict = {}
        data_by_hours = {
            hours: get_historical_kline_data(
                hours=hours, symbols=self.symbols, exchange=self.exchange
            )
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
        self,
        newest_values: Dict[str, Dict[str, float]],
        oldest_values: Dict[int, Dict[str, Dict[str, float]]],
    ):
        """Update incremental Z-scores with new data"""
        if newest_values is None:
            newest_values = get_symbol_kline_data(
                symbols=self.symbols,
                exchange=self.exchange,
                contract_type=self.contract_type,
            )

        oldest_by_hours = oldest_values or {}

        for hours in self.hours_options:
            oldest_for_hours = oldest_by_hours.get(hours, {})

            for symbol in self.symbols:
                for data_type in KLINE_FIELD_MAP.keys():
                    zscore_obj = self.incremental_zscores[symbol][data_type][hours]
                    symbol_new_values = newest_values.get(symbol)
                    if not symbol_new_values or data_type not in symbol_new_values:
                        continue
                    new_val = symbol_new_values[data_type]
                    old_val = oldest_for_hours.get(symbol, {}).get(data_type)

                    if old_val is not None:
                        zscore_obj.update_data_point(old_val, new_val)
                    else:
                        zscore_obj.add_data_point(new_val)

    def create_z_score_results(self):
        return {
            hours: {
                symbol: {
                    data_type: self.incremental_zscores[symbol][data_type][
                        hours
                    ].get_z_score()
                    for data_type in KLINE_FIELD_MAP.keys()
                }
                for symbol in self.symbols
            }
            for hours in self.hours_options
        }

    def store_z_score_results(self, results):
        """Store calculated Z-scores in DB"""
        current_time = timezone.now()
        print(f"[{self.exchange}] STORING ZSCORES")

        try:
            exchange = ExchangeModel.objects.get(name=self.exchange)
            contract_type = ContractType.objects.get(name=self.contract_type)
            symbols = Symbol.objects.filter(
                exchange=exchange, contract_type=contract_type
            )
            symbol_map = {s.name: s for s in symbols}
        except (ExchangeModel.DoesNotExist, ContractType.DoesNotExist) as e:
            print(f"[{self.exchange}] Exchange or ContractType not found:", e)
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

    def add_new_symbol(self, symbol_name):
        """Add a new symbol to zscore tracking"""
        if symbol_name in self.symbols:
            print(f"[{self.exchange}] Symbol {symbol_name} already being tracked")
            return

        print(f"[{self.exchange}] Adding {symbol_name} to zscore tracking")

        self.symbols.append(symbol_name)

        for data_type in KLINE_FIELD_MAP.keys():
            for hours in self.hours_options:
                self.incremental_zscores.setdefault(symbol_name, {}).setdefault(
                    data_type, {}
                )[hours] = IncrementalZScore(hours * 60)

                historical_data = get_historical_kline_data(
                    hours=hours, symbols=[symbol_name], exchange=self.exchange
                )
                series = historical_data.get(symbol_name, {}).get(data_type, [])

                if series and len(series) > 0:
                    self.incremental_zscores[symbol_name][data_type][
                        hours
                    ].initialize_from_data(series)

    def remove_symbol(self, symbol_name):
        """Remove a symbol from zscore tracking"""
        if symbol_name not in self.symbols:
            print(f"[{self.exchange}] Symbol {symbol_name} not being tracked")
            return

        print(f"[{self.exchange}] Removing symbol {symbol_name} from zscore tracking")

        self.symbols.remove(symbol_name)

        if symbol_name in self.incremental_zscores:
            del self.incremental_zscores[symbol_name]

    def fetch_and_store_zscore_history_data(self, redis_pipeline):
        for hours in self.hours_options:
            zscore_heatmap_data = get_zscore_history_data(
                hours=hours,
                exchange=self.exchange,
                contract_type=self.contract_type,
            )

            redis_pipeline.execute_command(
                "SET",
                f"zscore:heatmap:{self.exchange}:{self.contract_type}:{hours}",
                msgpack.packb(zscore_heatmap_data),
            )

        print(f"[{self.exchange}] ZSCORE: Stored zscore history data to redis")

    def _handle_kline_message(self, _msg_id: str, msg_data: Dict[str, Any]) -> bool:
        """
        Handle a single kline message from Redis Stream.

        Returns:
            True if processed successfully (will be ACKed)
            False if processing failed (will retry)
        """
        try:
            # Filter by exchange
            msg_exchange = msg_data.get("exchange", str(Exchange.BINANCE))
            if msg_exchange != str(self.exchange):
                return True  # ACK, not for us

            print(f"[{self.exchange}] ZSCORE: Processing update")

            newest_values = msg_data.get("newest_values")
            if not isinstance(newest_values, dict):
                print(f"[{self.exchange}] ZSCORE: No newest_values found in payload")
                return True  # ACK to skip invalid message

            oldest_values = msg_data.get("oldest_values", {})
            if not isinstance(oldest_values, dict):
                oldest_values = {}

            # Update zscores
            self.update_zscores(
                newest_values=newest_values,
                oldest_values=oldest_values,
            )
            results = self.create_z_score_results()

            # Store to Redis and DB
            pipeline = r.pipeline()
            for tf, tf_data in results.items():
                pipeline.execute_command(
                    "SET",
                    f"zscore:{self.exchange}:{self.contract_type}:{tf}",
                    msgpack.packb(tf_data),
                )
            self.store_z_score_results(results)
            self.fetch_and_store_zscore_history_data(redis_pipeline=pipeline)
            pipeline.execute()

            notification_service.send_zscore_update()

            return True  # Success

        except Exception as e:
            print(f"[{self.exchange}] ZSCORE: Error processing message: {e}")
            return False  # Retry

    def _handle_symbol_message(self, _msg_id: str, msg_data: Dict[str, Any]) -> bool:
        """
        Handle a single symbol event message from Redis Stream.

        Returns:
            True if processed successfully (will be ACKed)
            False if processing failed (will retry)
        """
        try:
            # Filter by exchange
            if msg_data.get("exchange") != str(self.exchange):
                return True  # ACK, not for us

            symbol = msg_data.get("symbol")
            event = msg_data.get("event")

            if event == "ADDED":
                print(
                    f"[{self.exchange}] ZSCORE: Received symbol added event: {symbol}"
                )
                self.add_new_symbol(symbol)
            elif event == "DELISTED":
                print(
                    f"[{self.exchange}] ZSCORE: Received symbol delisted event: {symbol}"
                )
                self.remove_symbol(symbol)

            return True  # Success

        except Exception as e:
            print(f"[{self.exchange}] ZSCORE: Error processing symbol message: {e}")
            return True  # ACK anyway for symbol events to avoid blocking

    def run(self):
        """Main processing loop listening for Redis Streams"""
        print(f"[{self.exchange}] Starting ZScore processor with Redis Streams...")

        # Set up stream consumers
        klines_stream = RedisStreamKeys.klines(self.exchange)
        symbols_stream = RedisStreamKeys.symbols(self.exchange)
        group_name = RedisStreamKeys.consumer_group("zscore", self.exchange)

        klines_consumer = StreamConsumer(r, klines_stream, group_name)
        symbols_consumer = StreamConsumer(r, symbols_stream, group_name)

        # Create consumer groups - start from latest to only process new messages
        klines_consumer.create_consumer_group(start_id="$")
        symbols_consumer.create_consumer_group(start_id="$")

        print(f"[{self.exchange}] ZScore stream consumers initialized")

        # Start consuming in separate threads
        klines_thread = threading.Thread(
            target=lambda: klines_consumer.start_consuming(
                message_handler=self._handle_kline_message,
                count=10,
                block_ms=2000,
            ),
            daemon=True,
        )
        symbols_thread = threading.Thread(
            target=lambda: symbols_consumer.start_consuming(
                message_handler=self._handle_symbol_message,
                count=10,
                block_ms=1000,
            ),
            daemon=True,
        )

        klines_thread.start()
        symbols_thread.start()

        print(f"[{self.exchange}] Ready for real-time zscore updates from streams")

        try:
            klines_thread.join()
            symbols_thread.join()
        except KeyboardInterrupt:
            print(f"[{self.exchange}] Shutting down...")
            klines_consumer.stop()
            symbols_consumer.stop()
