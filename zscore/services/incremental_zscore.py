import time
import numpy as np
import msgpack
from datetime import datetime, timezone as dt_timezone
from typing import Dict, Optional, cast
from django.utils import timezone
from django.conf import settings
from django.db import close_old_connections, connection
from redis.exceptions import RedisError

from exchange_connections.selectors import (
    get_exchange_symbols,
    get_symbol_kline_data,
    get_historical_kline_data,
)
from zscore.selectors.zscore import get_zscore_history_data
from exchange_connections.constants import KLINE_FIELD_MAP, get_btc_symbol, get_sol_symbol
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

    def get_state(self):
        if self.count <= 0 or self.current_value is None:
            return {
                "count": float(self.count),
                "mean": float(self.mean),
                "variance": 0.0,
                "std_dev": 0.0,
                "current_value": 0.0,
                "z_score": 0.0,
            }

        variance = self.M2 / self.count
        std_dev = np.sqrt(max(variance, 0.0))
        return {
            "count": float(self.count),
            "mean": float(self.mean),
            "variance": float(variance),
            "std_dev": float(std_dev),
            "current_value": float(self.current_value),
            "z_score": float(self.get_z_score()),
        }


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
        self._validation_symbols = self._get_validation_symbols()
        self._last_processed_kline_timestamp_ms: Optional[int] = None
        self._last_processed_stream_id: Optional[str] = None
        self._throttled_log_at: Dict[str, float] = {}
        self._last_validation_inputs: Dict[str, Dict[str, Optional[float]]] = {}

    def _get_validation_symbols(self):
        btc = get_btc_symbol(self.exchange)
        sol = get_sol_symbol(self.exchange)
        return [btc, sol]

    def _log_throttled(self, key: str, every_seconds: int, message: str):
        now = time.time()
        last = self._throttled_log_at.get(key, 0.0)
        if now - last >= every_seconds:
            print(message)
            self._throttled_log_at[key] = now

    def _log_message_ordering(self, timestamp_ms: int, source: str, msg_id: str):
        previous = self._last_processed_kline_timestamp_ms
        if previous is None:
            return

        if timestamp_ms <= previous:
            delta_ms = timestamp_ms - previous
            self._log_throttled(
                "out_of_order_kline",
                10,
                f"[{self.exchange}][WARN] Out-of-order zscore kline message: ts={timestamp_ms}, "
                f"prev_processed_ts={previous}, delta_ms={delta_ms}, source={source}, msg_id={msg_id}",
            )

    def _log_payload_health(self, timestamp_ms: int, source: str, newest: Dict, oldest: Dict):
        expected = len(self.symbols)
        newest_count = len(newest)
        oldest_counts = {
            int(hours): len(oldest.get(hours) or oldest.get(str(hours), {}))
            for hours in self.hours_options
        }

        if expected and newest_count < expected:
            missing = [s for s in self.symbols if s not in newest][:5]
            self._log_throttled(
                "payload_newest_incomplete",
                20,
                f"[{self.exchange}][WARN] Incomplete zscore newest_values at ts={timestamp_ms} "
                f"(source={source}): {newest_count}/{expected} symbols, sample missing={missing}",
            )

        for hours, count in oldest_counts.items():
            if expected and count < expected:
                self._log_throttled(
                    f"payload_oldest_incomplete_{hours}",
                    20,
                    f"[{self.exchange}][WARN] Incomplete zscore oldest_values at ts={timestamp_ms} "
                    f"(source={source}, hours={hours}): {count}/{expected} symbols",
                )

    def _log_btc_sol_state_snapshot(self, timestamp_ms: int, source: str):
        if len(self._validation_symbols) < 2:
            return
        if 1 not in self.hours_options:
            return

        snapshots = []
        for symbol in self._validation_symbols[:2]:
            zscore_obj = (
                self.incremental_zscores.get(symbol, {})
                .get("price", {})
                .get(1)
            )
            if zscore_obj is None:
                continue
            state = zscore_obj.get_state()
            snapshots.append((symbol, state))

            invalid = (
                not np.isfinite(state["z_score"])
                or state["variance"] < -1e-9
                or abs(state["z_score"]) > 50
            )
            if invalid:
                self._log_throttled(
                    f"zscore_invalid_state_{symbol}",
                    10,
                    f"[{self.exchange}][ERROR] Invalid zscore tracker state for {symbol} at ts={timestamp_ms} source={source}: "
                    f"z={state['z_score']:.6f} count={state['count']:.0f} mean={state['mean']:.6f} "
                    f"variance={state['variance']:.6e} current={state['current_value']:.6f}",
                )

        if snapshots:
            def _fmt(v):
                return f"{v:.6f}" if isinstance(v, float) else "None"

            snapshot_parts = []
            for symbol, state in snapshots:
                inputs = self._last_validation_inputs.get(symbol, {})
                snapshot_parts.append(
                    f"{symbol}: z={state['z_score']:.6f} count={state['count']:.0f} "
                    f"std={state['std_dev']:.6e} current={state['current_value']:.6f} "
                    f"new={_fmt(inputs.get('new'))} old={_fmt(inputs.get('old'))}"
                )
            # Per-tick log so a drift event can be bracketed by adjacent ticks.
            print(
                f"[{self.exchange}][DEBUG] ZScore tick ts={timestamp_ms} source={source}: "
                + " | ".join(snapshot_parts)
            )

    @staticmethod
    def _manual_zscore(values: list) -> tuple[float, int, float]:
        arr = np.array(values, dtype=np.float64)
        valid = arr[np.isfinite(arr)]
        if valid.size < 2:
            return 0.0, int(valid.size), 0.0
        variance = float(np.var(valid))
        if variance <= 1e-9:
            return 0.0, int(valid.size), variance
        mean = float(np.mean(valid))
        z = float((valid[-1] - mean) / np.sqrt(variance))
        return z, int(valid.size), variance

    def _validate_btc_sol_zscores(self, timestamp_ms: int, source: str):
        try:
            if len(self._validation_symbols) < 2:
                return
            if 1 not in self.hours_options:
                return

            btc_sym, sol_sym = self._validation_symbols[:2]
            data = get_historical_kline_data(
                hours=1,
                symbols=[btc_sym, sol_sym],
                exchange=self.exchange,
                contract_type=self.contract_type,
            )

            validation_parts = []
            for symbol in (btc_sym, sol_sym):
                zscore_obj = (
                    self.incremental_zscores.get(symbol, {})
                    .get("price", {})
                    .get(1)
                )
                if zscore_obj is None:
                    continue

                series = data.get(symbol, {}).get("price", [])
                manual_z, count, variance = self._manual_zscore(series)
                incremental_z = float(zscore_obj.get_z_score())
                diff = abs(incremental_z - manual_z)
                validation_parts.append(
                    f"{symbol}: inc={incremental_z:.6f} numpy={manual_z:.6f} diff={diff:.6f} points={count} variance={variance:.6e}"
                )

                if diff > 0.25:
                    self._log_throttled(
                        f"zscore_validation_drift_{symbol}",
                        10,
                        f"[{self.exchange}][ERROR] ZScore drift for {symbol} at ts={timestamp_ms} source={source}: "
                        f"incremental={incremental_z:.6f} numpy={manual_z:.6f} diff={diff:.6f} points={count} variance={variance:.6e}",
                    )

            if validation_parts:
                print(
                    f"[{self.exchange}][VALIDATION] ZScore 1h price ts={timestamp_ms} source={source} "
                    + " | ".join(validation_parts)
                )
        except Exception as exc:
            print(f"[{self.exchange}][VALIDATION] ZScore validation failed: {exc}")

    def initialize_zscores(self, end_time: datetime | None = None):
        """Initialize Z-score objects using historical kline data from DB"""
        zscore_dict = {}
        data_by_hours = {
            hours: get_historical_kline_data(
                hours=hours,
                symbols=self.symbols,
                exchange=self.exchange,
                contract_type=self.contract_type,
                end_time=end_time,
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
        newest_values: Optional[Dict[str, Dict[str, float]]],
        oldest_values: Optional[Dict[int | str, Dict[str, Dict[str, float]]]],
    ):
        """Update incremental Z-scores with new data"""
        if newest_values is None:
            newest_values = get_symbol_kline_data(
                symbols=self.symbols,
                exchange=self.exchange,
                contract_type=self.contract_type,
            )

        oldest_by_hours = oldest_values or {}
        stats_1h_price = {
            "missing_new": 0,
            "added_without_old": 0,
            "updated_with_old": 0,
        }

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
                        if hours == 1 and data_type == "price":
                            stats_1h_price["missing_new"] += 1
                        continue
                    new_val = symbol_new_values[data_type]
                    if new_val is None or not np.isfinite(new_val):
                        if hours == 1 and data_type == "price":
                            stats_1h_price["missing_new"] += 1
                        continue
                    old_val = oldest_for_hours.get(symbol, {}).get(data_type)

                    if old_val is not None and np.isfinite(old_val):
                        zscore_obj.update_data_point(old_val, new_val)
                        if hours == 1 and data_type == "price":
                            stats_1h_price["updated_with_old"] += 1
                    else:
                        zscore_obj.add_data_point(new_val)
                        if hours == 1 and data_type == "price":
                            stats_1h_price["added_without_old"] += 1

                    if (
                        hours == 1
                        and data_type == "price"
                        and symbol in self._validation_symbols
                    ):
                        self._last_validation_inputs[symbol] = {
                            "new": float(new_val),
                            "old": float(old_val)
                            if old_val is not None and np.isfinite(old_val)
                            else None,
                        }

        expected = len(self.symbols)
        if expected and (
            stats_1h_price["missing_new"] > 0 or stats_1h_price["added_without_old"] > 0
        ):
            self._log_throttled(
                "zscore_window_integrity",
                20,
                f"[{self.exchange}][WARN] 1h price window integrity: "
                f"updated_with_old={stats_1h_price['updated_with_old']}/{expected}, "
                f"added_without_old={stats_1h_price['added_without_old']}, "
                f"missing_new={stats_1h_price['missing_new']}",
            )

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

        print(
            f"[{self.exchange}] Adding {symbol_name} to zscore tracking "
            f"(last_processed_ms={self._last_processed_kline_timestamp_ms}; "
            f"new symbol's window will be initialized through now-1m)"
        )

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

        snapshot_time = time.time()
        snapshot_end_time = datetime.fromtimestamp(
            snapshot_time, tz=dt_timezone.utc
        ).replace(second=0, microsecond=0)
        snapshot_last_complete_minute_ms = (
            int(snapshot_end_time.timestamp() * 1000) - 60000
        )

        print(f"[{self.exchange}] Initializing zscore trackers...")
        self.incremental_zscores = self.initialize_zscores(end_time=snapshot_end_time)
        self.initialized = True
        print(f"[{self.exchange}] ZScore initialization complete")

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
        mark_timestamp_processed(
            r,
            "zscore",
            self.exchange,
            self.contract_type,
            snapshot_last_complete_minute_ms,
        )

        notification_service.send_zscore_update()
        print(
            f"[{self.exchange}] ZScore snapshot complete (marked ts={snapshot_last_complete_minute_ms})"
        )

        # Resume from captured position to process messages published during init
        while True:
            try:
                self._consume_stream(resume_from_id=pre_init_stream_id)
            except RedisError as exc:
                print(f"[{self.exchange}] ERROR: Redis stream read failed: {exc}. Retrying in 2s")
                time.sleep(2)

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
            # Drop any stale/aged DB connection so the next query reconnects.
            # Long-running consumers never hit Django's request signals, so this
            # is the only place CONN_MAX_AGE recycling happens.
            close_old_connections()

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
                source = str(payload.get("source") or "unknown")
                msg_id_str = msg_id.decode("utf-8")
                self._log_message_ordering(
                    timestamp_ms=timestamp_ms,
                    source=source,
                    msg_id=msg_id_str,
                )

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
                self._log_payload_health(
                    timestamp_ms=timestamp_ms,
                    source=source,
                    newest=newest,
                    oldest=oldest,
                )

                self.update_zscores(
                    newest_values=newest,
                    oldest_values=oldest,
                )
                self._log_btc_sol_state_snapshot(timestamp_ms=timestamp_ms, source=source)
                results = self.create_z_score_results()

                pipeline = r.pipeline()
                for tf, tf_data in results.items():
                    pipeline.execute_command(
                        "SET",
                        f"zscore:{self.exchange}:{self.contract_type}:{tf}",
                        msgpack.packb(tf_data),
                    )
                save_to_db = source != "backfill"
                if save_to_db:
                    self.store_z_score_results(results)
                self.fetch_and_store_zscore_history_data(redis_pipeline=pipeline)
                pipeline.execute()

                latest_complete_minute_ms = (
                    int(time.time() * 1000) // 60000
                ) * 60000 - 60000
                if timestamp_ms >= latest_complete_minute_ms:
                    self._validate_btc_sol_zscores(
                        timestamp_ms=timestamp_ms, source=source
                    )

                # Mark as processed after successful update
                mark_timestamp_processed(
                    r, "zscore", self.exchange, self.contract_type, timestamp_ms
                )
                self._last_processed_kline_timestamp_ms = max(
                    timestamp_ms,
                    self._last_processed_kline_timestamp_ms or timestamp_ms,
                )
                self._last_processed_stream_id = msg_id_str
                self._log_throttled(
                    "zscore_progress",
                    60,
                    f"[{self.exchange}] ZScore stream progress: last_processed_ts={self._last_processed_kline_timestamp_ms} "
                    f"source={source} msg_id={msg_id_str}",
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
            # Drop a possibly-dead DB connection so the redelivered message
            # reconnects instead of looping forever on a closed socket.
            connection.close()
            print(
                f"[{self.exchange}] ERROR: Stream handler failed: {e}, message will be redelivered"
            )
