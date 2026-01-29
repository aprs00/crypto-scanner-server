import time
import numpy as np
import msgpack
from typing import Dict, cast
from django.utils import timezone
from django.conf import settings

from exchange_connections.selectors import (
    get_exchange_symbols,
    get_symbol_kline_data,
    get_historical_kline_data,
)
from zscore.selectors.zscore import get_zscore_history_data
from exchange_connections.constants import KLINE_FIELD_MAP
from core.constants import EXCHANGE_CONFIG, Exchange
from zscore.models import ZScoreHistory
from exchange_connections.models import Symbol, Exchange as ExchangeModel, ContractType
from core.redis_config import get_redis_connection
from core.notifications import notification_service
from core.redis_streams import (
    get_market_stream_key,
    ensure_consumer_group,
    decode_stream_fields,
    is_timestamp_processed,
    mark_timestamp_processed,
    get_stream_last_id,
    compare_stream_ids,
    get_consumer_group_last_id,
)

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
        if arr.size == 0:
            return

        valid = np.isfinite(arr)
        if not np.any(valid):
            return

        arr = arr[valid]
        if arr.size > self.window_size:
            arr = arr[-self.window_size :]

        self.count = arr.size
        self.mean = float(np.mean(arr))
        self.M2 = float(np.sum((arr - self.mean) ** 2))
        self.current_value = float(arr[-1])

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
        # NOTE: initialization moved to run() to ensure we capture stream position first
        self.incremental_zscores: dict = {}
        self.initialized = False

    def initialize_zscores(self):
        """Initialize Z-score objects using historical kline data from DB"""
        zscore_dict = {}
        data_by_hours = {
            hours: get_historical_kline_data(
                hours=hours,
                symbols=self.symbols,
                exchange=self.exchange,
                contract_type=self.contract_type,
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
        oldest_values: Dict[int | str, Dict[str, Dict[str, float]]],
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
            # Handle both int and string keys (JSON decoding produces string keys)
            oldest_for_hours = oldest_by_hours.get(hours) or oldest_by_hours.get(
                str(hours), {}
            )

            for symbol in self.symbols:
                for data_type in KLINE_FIELD_MAP.keys():
                    zscore_obj = self.incremental_zscores[symbol][data_type][hours]
                    symbol_new_values = newest_values.get(symbol)
                    if not symbol_new_values or data_type not in symbol_new_values:
                        continue
                    new_val = symbol_new_values[data_type]
                    if new_val is None or not np.isfinite(new_val):
                        continue
                    old_val = oldest_for_hours.get(symbol, {}).get(data_type)

                    if old_val is not None and np.isfinite(old_val):
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
                    hours=hours,
                    symbols=[symbol_name],
                    exchange=self.exchange,
                    contract_type=self.contract_type,
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

    def run(self):
        """Initialize and process Z-scores using Redis streams."""
        print(f"[{self.exchange}] Starting ZScore processor (streams enabled)")

        # BEFORE initialization - capture stream position to avoid losing messages
        # published during the (potentially long) init phase
        stream_key = get_market_stream_key(self.exchange, self.contract_type)
        pre_init_stream_id = get_stream_last_id(r, stream_key)
        print(
            f"[{self.exchange}] Captured stream position before init: {pre_init_stream_id}"
        )

        print(f"[{self.exchange}] Initializing zscore trackers...")
        self.incremental_zscores = self.initialize_zscores()
        self.initialized = True
        print(f"[{self.exchange}] ZScore initialization complete")

        newest_values = get_symbol_kline_data(
            symbols=self.symbols,
            exchange=self.exchange,
            contract_type=self.contract_type,
        )
        self.update_zscores(newest_values=newest_values, oldest_values={})
        results = self.create_z_score_results()

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

        # Mark the initial timestamp as processed to prevent duplicate processing
        current_ts = (int(time.time() * 1000) // 60000) * 60000 - 60000
        mark_timestamp_processed(
            r, "zscore", self.exchange, self.contract_type, current_ts
        )

        notification_service.send_zscore_update()
        print(f"[{self.exchange}] ZScore snapshot complete (marked ts={current_ts})")

        # Resume from captured position to process messages published during init
        self._consume_stream(resume_from_id=pre_init_stream_id)

    def _consume_stream(self, resume_from_id: str = "$"):
        stream_key = get_market_stream_key(self.exchange, self.contract_type)
        group_name = f"zscore:{self.exchange}:{self.contract_type}"
        # Use fixed consumer name to take over pending messages on restart
        consumer_name = f"zscore-{self.exchange}-worker"

        created = ensure_consumer_group(r, stream_key, group_name)

        current_last_id = get_consumer_group_last_id(r, stream_key, group_name)
        target_id = resume_from_id
        if current_last_id and compare_stream_ids(current_last_id, resume_from_id) > 0:
            target_id = current_last_id

        if created or current_last_id is None or compare_stream_ids(target_id, current_last_id) != 0:
            r.xgroup_setid(stream_key, group_name, target_id)
            current_last_id = target_id
        print(
            f"[{self.exchange}] Listening to stream {stream_key} as {consumer_name} (resuming from {resume_from_id}, group last id {current_last_id})"
        )

        while True:
            messages = cast(
                list,
                r.xreadgroup(
                    group_name,
                    consumer_name,
                    {stream_key: ">"},
                    count=10,
                    block=5000,
                ),
            )

            if not messages:
                continue

            for _, entries in messages:
                for msg_id, fields in entries:
                    self._process_message(msg_id, fields, stream_key, group_name)

    def _process_message(
        self, msg_id: bytes, fields: dict, stream_key: str, group_name: str
    ):
        """Process a single stream message and ACK only on success."""
        decoded = decode_stream_fields(fields)
        event_type = decoded.get("event_type")
        payload = decoded.get("payload") or {}

        try:
            if event_type == "kline":
                timestamp_ms = payload.get("timestamp_ms")
                if timestamp_ms is None:
                    # ACK invalid messages to prevent infinite redelivery
                    r.xack(stream_key, group_name, msg_id)
                    return
                timestamp_ms = int(timestamp_ms)

                # Idempotency check - skip if already processed
                if is_timestamp_processed(
                    r, "zscore", self.exchange, self.contract_type, timestamp_ms
                ):
                    print(
                        f"[{self.exchange}] Skipping duplicate zscore timestamp {timestamp_ms}"
                    )
                    r.xack(stream_key, group_name, msg_id)
                    return

                # Use data from stream payload
                newest = payload.get("newest_values") or {}
                oldest = payload.get("oldest_values") or {}
                oldest = cast(Dict[int | str, Dict[str, Dict[str, float]]], oldest)

                self.update_zscores(
                    newest_values=newest,
                    oldest_values=oldest,
                )
                results = self.create_z_score_results()

                pipeline = r.pipeline()
                for tf, tf_data in results.items():
                    pipeline.execute_command(
                        "SET",
                        f"zscore:{self.exchange}:{self.contract_type}:{tf}",
                        msgpack.packb(tf_data),
                    )
                save_to_db = payload.get("source") != "backfill"
                if save_to_db:
                    self.store_z_score_results(results)
                self.fetch_and_store_zscore_history_data(redis_pipeline=pipeline)
                pipeline.execute()

                # Mark as processed after successful update
                mark_timestamp_processed(
                    r, "zscore", self.exchange, self.contract_type, timestamp_ms
                )

                notification_service.send_zscore_update()
            elif event_type == "symbol_update":
                added = payload.get("added") or []
                removed = payload.get("removed") or []

                for symbol in added:
                    self.add_new_symbol(symbol)
                for symbol in removed:
                    self.remove_symbol(symbol)

            # ACK only on success
            r.xack(stream_key, group_name, msg_id)
        except Exception as e:
            print(
                f"[{self.exchange}] ERROR: Stream handler failed: {e}, message will be redelivered"
            )
