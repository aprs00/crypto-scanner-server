import time
import threading
import gc
from collections import deque
import numpy as np
import msgpack
from typing import Dict, List, Optional, cast

from exchange_connections.constants import (
    KLINE_FIELD_MAP,
    get_btc_symbol,
    get_sol_symbol,
)
from exchange_connections.selectors import (
    get_exchange_symbols,
    get_historical_kline_data,
    get_symbol_kline_data,
    get_symbol_kline_data_multi_hours,
)
from core.constants import EXCHANGE_CONFIG, Exchange
from core.redis_config import get_redis_connection
from core.notifications import notification_service
from core.redis_streams import (
    get_market_stream_key,
    ensure_consumer_group,
    decode_stream_fields,
    is_timestamp_processed,
    mark_timestamp_processed,
    get_stream_last_id,
)
from correlations.services.save_correlations import (
    save_correlation_matrices_batch_to_db,
)


class CorrelationTracker:
    """
    Maintains running statistics for O(1) correlation updates.

    For N symbols, stores:
    - sum_x[N]: sum of values per symbol
    - sum_xx[N]: sum of squared values per symbol
    - sum_xy[N,N]: sum of products for each pair
    - count: number of data points in window
    """

    sum_x: np.ndarray
    sum_xx: np.ndarray
    sum_xy: np.ndarray

    def __init__(self, window_size: int, n_symbols: int):
        self.window_size = window_size
        self.n_symbols = n_symbols
        self.count = 0
        self.sum_x = np.zeros(n_symbols, dtype=np.float64)
        self.sum_xx = np.zeros(n_symbols, dtype=np.float64)
        self.sum_xy = np.zeros((n_symbols, n_symbols), dtype=np.float64)

    def initialize(self, symbol_data: Dict[int, np.ndarray]):
        """Initialize from historical data keyed by symbol index."""
        if not symbol_data:
            return

        length = len(next(iter(symbol_data.values())))
        if length == 0:
            return

        effective_length = min(length, self.window_size)

        arr = np.vstack(
            [
                symbol_data[i][-effective_length:].astype(np.float64)
                for i in range(self.n_symbols)
            ]
        )

        all_valid_mask = ~np.any(np.isnan(arr), axis=0)
        arr_aligned = arr[:, all_valid_mask]

        self.sum_x = np.sum(arr_aligned, axis=1)
        self.sum_xx = np.sum(arr_aligned * arr_aligned, axis=1)
        self.sum_xy = arr_aligned @ arr_aligned.T
        self.count = int(np.sum(all_valid_mask))

    def update(self, new_vals: np.ndarray, old_vals: Optional[np.ndarray] = None):
        """Update running sums with new values, removing old if window full."""
        if self.count >= self.window_size and old_vals is not None:
            mask = ~np.isnan(old_vals)
            if mask.any():
                vals = old_vals[mask]
                self.sum_x[mask] -= vals
                self.sum_xx[mask] -= vals * vals
                self.sum_xy[np.ix_(mask, mask)] -= np.outer(vals, vals)
                self.count -= 1

        mask = ~np.isnan(new_vals)
        if mask.any():
            vals = new_vals[mask]
            self.sum_x[mask] += vals
            self.sum_xx[mask] += vals * vals
            self.sum_xy[np.ix_(mask, mask)] += np.outer(vals, vals)

        self.count = min(self.count + 1, self.window_size)

    def get_correlations(self) -> List[float]:
        """Return upper triangle of correlation matrix as flat list."""
        if self.count <= 1 or self.n_symbols <= 1:
            return []

        c = np.float64(self.count)
        means = self.sum_x / c
        var = (self.sum_xx / c) - means * means
        var = np.where(var <= 0, np.nan, var)

        cov = (self.sum_xy / c) - np.outer(means, means)
        denom = np.sqrt(np.outer(var, var))

        with np.errstate(invalid="ignore", divide="ignore"):
            corr = cov / denom

        corr = np.clip(corr, -1.0, 1.0)
        corr = np.nan_to_num(corr, nan=0.0)
        np.fill_diagonal(corr, 1.0)

        # Extract upper triangle
        i, j = np.triu_indices(self.n_symbols, k=1)
        return [float(x) for x in corr[i, j]]


class CorrelationCalculator:
    """Main correlation calculator with Redis pubsub integration."""

    def __init__(self, exchange: Exchange, contract_type: str = "perpetual"):
        self.exchange = exchange
        self.contract_type = contract_type
        self.redis = get_redis_connection()
        self.lock = threading.RLock()
        self.symbols: List[str] = []
        self.symbol_to_idx: Dict[str, int] = {}
        self.hours_options: List[int] = []
        self.trackers: Dict[tuple, CorrelationTracker] = {}
        self.initialized = False
        self.last_update_time: float = 0
        self.update_count: int = 0

        self._validation_symbols = self._get_validation_symbols()

        # Buffer to store last 60 price points for validation
        self._pubsub_prices: Dict[str, deque] = {}
        self._pubsub_timestamps: deque = deque(maxlen=60)

    def _get_validation_symbols(self) -> List[str]:
        """Get validation symbol pair for the exchange."""
        btc = get_btc_symbol(self.exchange)
        sol = get_sol_symbol(self.exchange)
        return [btc, sol]

    def _store_pubsub_prices(self, newest: Dict, timestamp: int):
        """Store latest pubsub prices for validation symbols."""
        for sym in self._validation_symbols:
            if sym not in self._pubsub_prices:
                self._pubsub_prices[sym] = deque(maxlen=60)

            if sym in newest and "price" in newest[sym]:
                price = newest[sym]["price"]
                self._pubsub_prices[sym].append(
                    {
                        "price": price,
                        "timestamp": timestamp,
                    }
                )

        self._pubsub_timestamps.append(timestamp)

    def _rebuild_indices(self):
        """Rebuild symbol index mapping."""
        self.symbol_to_idx = {s: i for i, s in enumerate(self.symbols)}

    def _init_trackers(self):
        """Initialize all correlation trackers from historical data."""
        n = len(self.symbols)
        print(
            f"[{self.exchange}] Initializing correlations for {n} symbols ({n*(n-1)//2:,} pairs)"
        )

        max_hours = max(self.hours_options)
        print(f"[{self.exchange}] Fetching {max_hours}h of historical data...")

        start = time.time()
        all_data = get_historical_kline_data(
            hours=max_hours, symbols=self.symbols, exchange=self.exchange
        )
        print(f"[{self.exchange}] Data fetch: {time.time() - start:.2f}s")

        for hours in sorted(self.hours_options, reverse=True):
            window = hours * 60

            for data_type in KLINE_FIELD_MAP:
                tracker = CorrelationTracker(window, n)

                indexed = {}
                for sym in self.symbols:
                    idx = self.symbol_to_idx[sym]
                    if sym in all_data and data_type in all_data[sym]:
                        data = np.asarray(all_data[sym][data_type], dtype=np.float64)
                        if len(data) >= window:
                            indexed[idx] = data[-window:]
                        else:
                            padded = np.full(window, np.nan, dtype=np.float64)
                            padded[-len(data) :] = data
                            indexed[idx] = padded
                    else:
                        indexed[idx] = np.full(window, np.nan, dtype=np.float64)

                if indexed:
                    tracker.initialize(indexed)

                self.trackers[(hours, data_type)] = tracker

            print(f"[{self.exchange}] Initialized {hours}h timeframe")

        del all_data
        gc.collect()

    def _update_trackers(self, newest: Dict, oldest_by_hours: Dict):
        """Update all trackers with new/old values."""
        n = len(self.symbols)

        for hours in self.hours_options:
            oldest = oldest_by_hours.get(hours) or oldest_by_hours.get(str(hours), {})
            window = hours * 60

            for data_type in KLINE_FIELD_MAP:
                tracker = self.trackers.get((hours, data_type))
                if not tracker:
                    print(f"[DEBUG] No tracker found for ({hours}, {data_type})")
                    continue
                if tracker.n_symbols != n:
                    print(
                        f"[DEBUG] CRITICAL MISMATCH: tracker({hours},{data_type}) n_symbols={tracker.n_symbols} != len(symbols)={n}"
                    )
                    continue

                new_arr = np.full(n, np.nan, dtype=np.float64)
                old_arr = np.full(n, np.nan, dtype=np.float64)

                missing_new = 0
                missing_old = 0
                for sym, idx in self.symbol_to_idx.items():
                    if sym in newest and data_type in newest[sym]:
                        new_arr[idx] = newest[sym][data_type]
                    else:
                        missing_new += 1
                    if (
                        tracker.count >= window
                        and sym in oldest
                        and data_type in oldest[sym]
                    ):
                        old_arr[idx] = oldest[sym][data_type]
                    elif tracker.count >= window:
                        missing_old += 1

                if (
                    hours == 1
                    and data_type == "close"
                    and (missing_new > 0 or missing_old > 0)
                ):
                    new_valid = np.sum(~np.isnan(new_arr))
                    old_valid = (
                        np.sum(~np.isnan(old_arr)) if tracker.count >= window else 0
                    )
                    print(
                        f"[DEBUG] _update_trackers(1h,close): new_valid={new_valid}/{n}, old_valid={old_valid}/{n}, missing_new={missing_new}, missing_old={missing_old}"
                    )

                tracker.update(new_arr, old_arr if tracker.count >= window else None)

    def _validate_btc_sol_correlation(self):
        """Validate incremental correlation against manual numpy calculation to detect drift."""
        try:
            if len(self._validation_symbols) < 2:
                return

            btc_sym, sol_sym = self._validation_symbols[:2]
            if btc_sym not in self.symbol_to_idx or sol_sym not in self.symbol_to_idx:
                print(
                    f"[{self.exchange}][VALIDATION] Skipping - {btc_sym} or {sol_sym} not in tracked symbols"
                )
                return

            data = get_historical_kline_data(
                hours=1, symbols=[btc_sym, sol_sym], exchange=self.exchange
            )

            if btc_sym not in data or sol_sym not in data:
                print(f"[VALIDATION] Could not fetch data for {btc_sym} or {sol_sym}")
                return

            tracker = self.trackers.get((1, "price"))
            if not tracker:
                print("[VALIDATION] No 1h price tracker found")
                return

            btc_idx = self.symbol_to_idx[btc_sym]
            sol_idx = self.symbol_to_idx[sol_sym]

            incremental_matrix = tracker.get_correlations()
            if not incremental_matrix:
                print("[VALIDATION] Empty incremental matrix")
                return

            # Upper triangle flat index: pair (i,j) where i<j = i*n - i*(i+1)/2 + (j-i-1)
            n = tracker.n_symbols
            if btc_idx < sol_idx:
                flat_idx = (
                    btc_idx * n
                    - (btc_idx * (btc_idx + 1)) // 2
                    + (sol_idx - btc_idx - 1)
                )
            else:
                flat_idx = (
                    sol_idx * n
                    - (sol_idx * (sol_idx + 1)) // 2
                    + (btc_idx - sol_idx - 1)
                )

            if flat_idx >= len(incremental_matrix):
                print(
                    f"[VALIDATION] flat_idx {flat_idx} out of bounds for matrix len {len(incremental_matrix)}"
                )
                return

            incremental_corr = incremental_matrix[flat_idx]

            btc_prices = np.array(data[btc_sym]["price"], dtype=np.float64)
            sol_prices = np.array(data[sol_sym]["price"], dtype=np.float64)

            min_len = min(len(btc_prices), len(sol_prices))

            btc_prices = btc_prices[-min_len:]
            sol_prices = sol_prices[-min_len:]

            manual_corr = np.corrcoef(btc_prices, sol_prices)[0, 1]

            diff = abs(incremental_corr - manual_corr)

            # Print comparison
            print("\n" + "=" * 80)
            print(
                f"[VALIDATION] BTC-SOL 1h price - Incremental={incremental_corr:.6f}, Numpy={manual_corr:.6f}, Diff={diff:.6f}"
            )
            print(f"  DB BTC ({len(btc_prices)}): {btc_prices.tolist()}")
            print(f"  DB SOL ({len(sol_prices)}): {sol_prices.tolist()}")
            print("=" * 80)

        except Exception as e:
            print(f"[VALIDATION] Error during BTC-SOL validation: {e}")
            import traceback

            traceback.print_exc()

    def _cache_correlations(self, save_to_db: bool = True):
        """Cache correlation matrices to Redis and optionally DB."""
        pipe = self.redis.pipeline()
        matrices_for_db: Dict[str, List[float]] = {}

        for hours in self.hours_options:
            for data_type in KLINE_FIELD_MAP:
                tracker = self.trackers.get((hours, data_type))
                if not tracker:
                    print(f"NOT TRACKER FOR {hours} {data_type}")
                    continue

                matrix = tracker.get_correlations()
                key = f"correlations:{data_type}:{hours}:{self.exchange}:{self.contract_type}"
                packed_data = msgpack.packb(matrix)
                if packed_data is not None:
                    pipe.set(key, packed_data)

                if save_to_db and hours == 1:
                    matrices_for_db[data_type] = matrix

        if matrices_for_db:
            start = time.time()
            try:
                save_correlation_matrices_batch_to_db(
                    symbols=self.symbols,
                    correlation_matrices=matrices_for_db,
                    hours=1,
                    exchange=self.exchange,
                    contract_type=self.contract_type,
                )
                print(
                    f"[{self.exchange}] Saved {len(matrices_for_db)} correlation types to DB in {time.time() - start:.2f}s"
                )
            except Exception as e:
                print(f"[{self.exchange}] DB batch save failed: {e}")

        pipe.execute()
        notification_service.send_correlation_update()
        self._validate_btc_sol_correlation()

    def update_correlations(
        self,
        newest: Optional[Dict] = None,
        oldest_values: Optional[Dict] = None,
        save_to_db: bool = True,
    ):
        """Main update method - fetch data, update trackers, cache results."""
        with self.lock:
            print(
                f"[{self.exchange}][DEBUG] update_correlations called - tracked symbols: {len(self.symbols)}, save_to_db: {save_to_db}"
            )

            if newest is None:
                print(f"[{self.exchange}][DEBUG] Fetching newest values (not provided)")
                newest = get_symbol_kline_data(
                    symbols=self.symbols,
                    exchange=self.exchange,
                    contract_type=self.contract_type,
                )

            if oldest_values is None:
                print(
                    f"[{self.exchange}][DEBUG] Fetching oldest values for windows (not provided)"
                )
                oldest_values = get_symbol_kline_data_multi_hours(
                    symbols=self.symbols,
                    exchange=self.exchange,
                    contract_type=self.contract_type,
                    hours_list=self.hours_options,
                )

            missing = [s for s in self.symbols if s not in newest]
            if missing:
                # Don't fetch missing symbols - that would get data from a different
                # timestamp and corrupt correlations. Log warning instead.
                print(
                    f"[{self.exchange}][WARN] Missing {len(missing)} symbols in kline data "
                    f"(first 5: {missing[:5]}). Data should be complete after batch save."
                )

            sample_tracker = self.trackers.get((1, "close"))
            if sample_tracker:
                print(
                    f"[{self.exchange}][DEBUG] Before update - tracker(1h,close): n_symbols={sample_tracker.n_symbols}, count={sample_tracker.count}"
                )

            # Use passed oldest_values from Redis message or empty dict as fallback
            oldest_by_hours = oldest_values or {}

            self._update_trackers(newest, oldest_by_hours)

            if sample_tracker:
                print(
                    f"[{self.exchange}][DEBUG] After update - tracker(1h,close): n_symbols={sample_tracker.n_symbols}, count={sample_tracker.count}"
                )

            self._cache_correlations(save_to_db)

    def add_symbol(self, symbol: str):
        """Add new symbol to tracking."""
        with self.lock:
            print(f"[{self.exchange}][DEBUG] add_symbol called for: {symbol}")
            print(
                f"[{self.exchange}][DEBUG] Current state before add - symbols: {len(self.symbols)}, trackers: {len(self.trackers)}"
            )

            if symbol in self.symbols:
                print(
                    f"[{self.exchange}][DEBUG] Symbol {symbol} already in list, skipping"
                )
                return

            available = get_exchange_symbols(
                exchange=self.exchange, contract_type=self.contract_type
            )
            print(
                f"[{self.exchange}][DEBUG] Available symbols from exchange: {len(available)}"
            )

            if symbol not in available:
                print(f"[{self.exchange}] Symbol {symbol} not available")
                return

            min_points = min(self.hours_options) * 60
            data = get_historical_kline_data(
                hours=max(self.hours_options), symbols=[symbol], exchange=self.exchange
            )

            if symbol not in data:
                print(f"[{self.exchange}] No data for {symbol}")
                return

            points = min(len(data[symbol].get(dt, [])) for dt in KLINE_FIELD_MAP)
            if points < min_points:
                print(
                    f"[{self.exchange}] Limited data for {symbol}: {points}/{min_points}, adding anyway"
                )

            old_symbols = self.symbols.copy()
            print(f"[{self.exchange}][DEBUG] Adding {symbol} - rebuilding all trackers")
            self.symbols = get_exchange_symbols(
                exchange=self.exchange, contract_type=self.contract_type
            )
            self._rebuild_indices()

            added = set(self.symbols) - set(old_symbols)
            removed = set(old_symbols) - set(self.symbols)
            if added:
                print(f"[{self.exchange}][DEBUG] Symbols added to list: {added}")
            if removed:
                print(
                    f"[{self.exchange}][DEBUG] Symbols removed from list (unexpected): {removed}"
                )

            print(
                f"[{self.exchange}][DEBUG] Reinitializing trackers for {len(self.symbols)} symbols"
            )
            self._init_trackers()

            sample_tracker = self.trackers.get((1, "close"))
            if sample_tracker:
                print(
                    f"[{self.exchange}][DEBUG] After add_symbol - tracker(1h,close): n_symbols={sample_tracker.n_symbols}, count={sample_tracker.count}"
                )

            self.update_correlations()
            print(f"[{self.exchange}][DEBUG] add_symbol completed for {symbol}")

    def remove_symbol(self, symbol: str):
        """Remove symbol from tracking."""
        with self.lock:
            print(f"[{self.exchange}][DEBUG] remove_symbol called for: {symbol}")
            print(
                f"[{self.exchange}][DEBUG] Current state before remove - symbols: {len(self.symbols)}, trackers: {len(self.trackers)}"
            )

            if symbol not in self.symbols:
                print(f"[{self.exchange}][DEBUG] Symbol {symbol} not in list, skipping")
                return

            print(f"[{self.exchange}] Removing {symbol}")
            idx = self.symbol_to_idx[symbol]
            print(f"[{self.exchange}][DEBUG] Symbol {symbol} has index {idx}")

            old_symbols = self.symbols.copy()
            self.symbols = get_exchange_symbols(
                exchange=self.exchange, contract_type=self.contract_type
            )
            self._rebuild_indices()

            added = set(self.symbols) - set(old_symbols)
            removed = set(old_symbols) - set(self.symbols)
            print(f"[{self.exchange}][DEBUG] Symbols removed from list: {removed}")
            if added:
                print(
                    f"[{self.exchange}][DEBUG] Symbols added to list (unexpected during remove): {added}"
                )

            print(
                f"[{self.exchange}][DEBUG] New symbol count: {len(self.symbols)}, removing index {idx} from trackers"
            )

            for key, tracker in self.trackers.items():
                tracker.sum_x = np.delete(tracker.sum_x, idx)
                tracker.sum_xx = np.delete(tracker.sum_xx, idx)
                tracker.sum_xy = np.delete(tracker.sum_xy, idx, axis=0)
                tracker.sum_xy = np.delete(tracker.sum_xy, idx, axis=1)
                tracker.n_symbols = len(self.symbols)

                if tracker.sum_x.shape[0] != tracker.n_symbols:
                    print(
                        f"[{self.exchange}][DEBUG] MISMATCH in tracker {key}: sum_x shape {tracker.sum_x.shape[0]} != n_symbols {tracker.n_symbols}"
                    )
                if tracker.sum_xy.shape[0] != tracker.n_symbols:
                    print(
                        f"[{self.exchange}][DEBUG] MISMATCH in tracker {key}: sum_xy shape {tracker.sum_xy.shape} != n_symbols {tracker.n_symbols}"
                    )

            sample_tracker = self.trackers.get((1, "close"))
            if sample_tracker:
                print(
                    f"[{self.exchange}][DEBUG] After remove_symbol - tracker(1h,close): n_symbols={sample_tracker.n_symbols}, count={sample_tracker.count}"
                )
                print(
                    f"[{self.exchange}][DEBUG] Array shapes - sum_x: {sample_tracker.sum_x.shape}, sum_xy: {sample_tracker.sum_xy.shape}"
                )

            self._cache_correlations(save_to_db=False)
            print(f"[{self.exchange}][DEBUG] remove_symbol completed for {symbol}")

    def _validate_newest_values(self, newest: Dict, timestamp: int) -> bool:
        """Validate the newest_values payload for issues."""
        issues = []

        # Check symbol count
        if len(newest) != len(self.symbols):
            issues.append(
                f"Symbol count mismatch: payload has {len(newest)}, tracking {len(self.symbols)}"
            )

        # Check for missing symbols
        missing_from_payload = set(self.symbols) - set(newest.keys())
        if missing_from_payload:
            issues.append(
                f"Missing from payload: {list(missing_from_payload)[:10]}{'...' if len(missing_from_payload) > 10 else ''}"
            )

        # Check for extra symbols in payload
        extra_in_payload = set(newest.keys()) - set(self.symbols)
        if extra_in_payload:
            issues.append(
                f"Extra in payload: {list(extra_in_payload)[:10]}{'...' if len(extra_in_payload) > 10 else ''}"
            )

        # Check for invalid values
        invalid_values = []
        for sym, vals in newest.items():
            if not isinstance(vals, dict):
                invalid_values.append(f"{sym}: not a dict")
                continue
            for key in ["price", "volume", "trades"]:
                if key not in vals:
                    invalid_values.append(f"{sym}: missing {key}")
                elif vals[key] is None or (
                    isinstance(vals[key], float) and (vals[key] != vals[key])
                ):  # NaN check
                    invalid_values.append(f"{sym}: {key} is None/NaN")

        if invalid_values:
            issues.append(
                f"Invalid values: {invalid_values[:5]}{'...' if len(invalid_values) > 5 else ''}"
            )

        if issues:
            print(f"[PAYLOAD VALIDATION] Issues found in update (ts={timestamp}):")
            for issue in issues:
                print(f"  - {issue}")
            self.redis.lpush(
                "error_log", f"[PAYLOAD VALIDATION] ts={timestamp}: {'; '.join(issues)}"
            )
            return False

        return True

    def run(self):
        """Run correlation updates using Redis streams."""
        print(f"[{self.exchange}] Starting correlation calculator (streams enabled)...")
        self.hours_options = list(
            EXCHANGE_CONFIG[self.exchange]["hours_options"]["correlation"].values()
        )
        self.symbols = get_exchange_symbols(
            exchange=self.exchange, contract_type=self.contract_type
        )
        self._rebuild_indices()

        # BEFORE initialization - capture stream position to avoid losing messages
        # published during the (potentially long) init phase
        stream_key = get_market_stream_key(self.exchange, self.contract_type)
        pre_init_stream_id = get_stream_last_id(self.redis, stream_key)
        print(
            f"[{self.exchange}] Captured stream position before init: {pre_init_stream_id}"
        )

        # Initialize trackers from historical data
        print(f"[{self.exchange}] Initializing correlation trackers...")
        start = time.time()
        self._init_trackers()
        print(
            f"[{self.exchange}] Initialization completed in {time.time() - start:.2f}s"
        )

        # Mark as initialized and compute one update snapshot
        self.initialized = True
        self._cache_correlations(save_to_db=True)

        # Mark the initial timestamp as processed to prevent duplicate processing
        current_ts = (int(time.time() * 1000) // 60000) * 60000 - 60000
        mark_timestamp_processed(
            self.redis, "correlations", self.exchange, self.contract_type, current_ts
        )
        print(
            f"[{self.exchange}] Correlation snapshot complete (marked ts={current_ts})"
        )

        # Resume from captured position to process messages published during init
        self._consume_stream(resume_from_id=pre_init_stream_id)

    def _consume_stream(self, resume_from_id: str = "$"):
        stream_key = get_market_stream_key(self.exchange, self.contract_type)
        group_name = f"correlations:{self.exchange}:{self.contract_type}"
        # Use fixed consumer name to take over pending messages on restart
        consumer_name = f"correlations-{self.exchange}-worker"

        ensure_consumer_group(self.redis, stream_key, group_name)

        # Resume from the captured stream position (before init) to process any
        # messages published during the initialization phase
        self.redis.xgroup_setid(stream_key, group_name, resume_from_id)
        print(
            f"[{self.exchange}] Listening to stream {stream_key} as {consumer_name} (resuming from {resume_from_id})"
        )

        while True:
            messages = cast(
                list,
                self.redis.xreadgroup(
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
                    self.redis.xack(stream_key, group_name, msg_id)
                    return
                timestamp_ms = int(timestamp_ms)

                # Idempotency check - skip if already processed
                if is_timestamp_processed(
                    self.redis,
                    "correlations",
                    self.exchange,
                    self.contract_type,
                    timestamp_ms,
                ):
                    print(
                        f"[{self.exchange}] Skipping duplicate correlations timestamp {timestamp_ms}"
                    )
                    self.redis.xack(stream_key, group_name, msg_id)
                    return

                # Use data from stream payload
                newest = payload.get("newest_values") or {}
                oldest = payload.get("oldest_values") or {}

                save_to_db = payload.get("source") != "backfill"
                self.update_correlations(
                    newest=newest,
                    oldest_values=oldest,
                    save_to_db=save_to_db,
                )

                # Mark as processed after successful update
                mark_timestamp_processed(
                    self.redis,
                    "correlations",
                    self.exchange,
                    self.contract_type,
                    timestamp_ms,
                )
            elif event_type == "symbol_update":
                added = payload.get("added") or []
                removed = payload.get("removed") or []

                for symbol in added:
                    self.add_symbol(symbol)
                for symbol in removed:
                    self.remove_symbol(symbol)

            # ACK only on success
            self.redis.xack(stream_key, group_name, msg_id)
        except Exception as e:
            print(
                f"[{self.exchange}] ERROR: Stream handler failed: {e}, message will be redelivered"
            )
