import json
import time
import threading
import gc
import numpy as np
import redis
import msgpack
from typing import Dict, List, Optional

from exchange_connections.constants import KLINE_FIELD_MAP
from exchange_connections.selectors import (
    get_exchange_symbols,
    get_historical_kline_data,
    get_symbol_kline_data,
    get_symbol_kline_data_multi_hours,
)
from core.constants import RedisPubMessages, tf_options
from core.redis_config import get_redis_connection
from core.notifications import notification_service
from correlations.services.save_correlations import save_correlation_matrix_to_db
from correlations.db_utils import cleanup_old_correlation_data


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

        length = min(len(d) for d in symbol_data.values())
        if length == 0:
            return

        effective_length = min(length, self.window_size)

        # Build data matrix and compute sums
        for idx, data in symbol_data.items():
            segment = data[-effective_length:].astype(np.float64)
            self.sum_x[idx] = segment.sum()
            self.sum_xx[idx] = (segment * segment).sum()

        # Compute sum_xy via matrix multiplication
        arr = np.vstack(
            [
                symbol_data[i][-effective_length:].astype(np.float64)
                for i in range(self.n_symbols)
            ]
        )
        self.sum_xy = arr @ arr.T
        self.count = effective_length

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

    VALIDATION_SYMBOLS = ["BTCUSDT", "SOLUSDT"]
    CORRELATION_TOLERANCE = 0.01  # Allow 1% difference

    def __init__(self):
        self.redis = get_redis_connection()
        self.lock = threading.RLock()
        self.symbols: List[str] = []
        self.symbol_to_idx: Dict[str, int] = {}
        self.hours_options: List[int] = []
        self.trackers: Dict[tuple, CorrelationTracker] = (
            {}
        )  # (hours, data_type) -> tracker
        self.initialized = False
        self.pending_updates: List[tuple] = []
        self.cleanup_stop = threading.Event()
        self.last_update_time: float = 0
        self.update_count: int = 0

    def _rebuild_indices(self):
        """Rebuild symbol index mapping."""
        self.symbol_to_idx = {s: i for i, s in enumerate(self.symbols)}

    def _init_trackers(self):
        """Initialize all correlation trackers from historical data."""
        n = len(self.symbols)
        print(f"Initializing correlations for {n} symbols ({n*(n-1)//2:,} pairs)")

        max_hours = max(self.hours_options)
        print(f"Fetching {max_hours}h of historical data...")

        start = time.time()
        all_data = get_historical_kline_data(hours=max_hours, symbols=self.symbols)
        print(f"Data fetch: {time.time() - start:.2f}s")

        for hours in sorted(self.hours_options, reverse=True):
            window = hours * 60

            for data_type in KLINE_FIELD_MAP:
                tracker = CorrelationTracker(window, n)

                indexed = {}
                for sym in self.symbols:
                    if sym in all_data and data_type in all_data[sym]:
                        idx = self.symbol_to_idx[sym]
                        data = np.asarray(all_data[sym][data_type], dtype=np.float64)
                        # Trim to window size for smaller timeframes
                        indexed[idx] = data[-window:] if len(data) > window else data

                if indexed:
                    tracker.initialize(indexed)

                self.trackers[(hours, data_type)] = tracker

            print(f"Initialized {hours}h timeframe")

        del all_data
        gc.collect()

    def _update_trackers(self, newest: Dict, oldest_by_hours: Dict[int, Dict]):
        """Update all trackers with new/old values."""
        n = len(self.symbols)

        for hours in self.hours_options:
            oldest = oldest_by_hours.get(hours, {})
            window = hours * 60

            for data_type in KLINE_FIELD_MAP:
                tracker = self.trackers.get((hours, data_type))
                if not tracker:
                    print(f"[DEBUG] No tracker found for ({hours}, {data_type})")
                    continue
                if tracker.n_symbols != n:
                    print(f"[DEBUG] CRITICAL MISMATCH: tracker({hours},{data_type}) n_symbols={tracker.n_symbols} != len(symbols)={n}")
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

                # Log only for 1h close to avoid spam
                if hours == 1 and data_type == "close":
                    new_valid = np.sum(~np.isnan(new_arr))
                    old_valid = np.sum(~np.isnan(old_arr)) if tracker.count >= window else 0
                    if missing_new > 0 or missing_old > 0:
                        print(f"[DEBUG] _update_trackers(1h,close): new_valid={new_valid}/{n}, old_valid={old_valid}/{n}, missing_new={missing_new}, missing_old={missing_old}")

                tracker.update(new_arr, old_arr if tracker.count >= window else None)

    def _validate_btc_sol_correlation(self):
        """
        Validate incremental BTC-SOL correlation against manual numpy calculation.
        Fetches data from DB and computes correlation manually to detect drift.
        """
        try:
            # Check if validation symbols are in our tracked symbols
            btc_sym, sol_sym = self.VALIDATION_SYMBOLS
            if btc_sym not in self.symbol_to_idx or sol_sym not in self.symbol_to_idx:
                print(f"[VALIDATION] Skipping - {btc_sym} or {sol_sym} not in tracked symbols")
                return

            # Fetch 60 minutes of data for both symbols
            data = get_historical_kline_data(hours=1, symbols=[btc_sym, sol_sym])

            if btc_sym not in data or sol_sym not in data:
                print(f"[VALIDATION] Could not fetch data for {btc_sym} or {sol_sym}")
                return

            # Get the 1h close tracker for comparison
            tracker = self.trackers.get((1, "price"))
            if not tracker:
                print("[VALIDATION] No 1h price tracker found")
                return

            # Get incremental correlation value
            btc_idx = self.symbol_to_idx[btc_sym]
            sol_idx = self.symbol_to_idx[sol_sym]

            # The correlation matrix is stored as upper triangle
            # We need to find the index of the BTC-SOL pair
            incremental_matrix = tracker.get_correlations()
            if not incremental_matrix:
                print("[VALIDATION] Empty incremental matrix")
                return

            # Calculate flat index for upper triangle
            n = tracker.n_symbols
            if btc_idx < sol_idx:
                # For upper triangle, pair (i,j) where i<j has index:
                # sum(n-1-k for k in range(i)) + (j - i - 1)
                # = i*n - i*(i+1)/2 + (j - i - 1)
                flat_idx = btc_idx * n - (btc_idx * (btc_idx + 1)) // 2 + (sol_idx - btc_idx - 1)
            else:
                flat_idx = sol_idx * n - (sol_idx * (sol_idx + 1)) // 2 + (btc_idx - sol_idx - 1)

            if flat_idx >= len(incremental_matrix):
                print(f"[VALIDATION] flat_idx {flat_idx} out of bounds for matrix len {len(incremental_matrix)}")
                return

            incremental_corr = incremental_matrix[flat_idx]

            # Calculate manual correlation with numpy
            btc_prices = np.array(data[btc_sym]["price"], dtype=np.float64)
            sol_prices = np.array(data[sol_sym]["price"], dtype=np.float64)

            # Make sure both arrays have the same length
            min_len = min(len(btc_prices), len(sol_prices))
            if min_len < 10:
                print(f"[VALIDATION] Not enough data points: {min_len}")
                return

            btc_prices = btc_prices[-min_len:]
            sol_prices = sol_prices[-min_len:]

            # Calculate Pearson correlation manually
            manual_corr = np.corrcoef(btc_prices, sol_prices)[0, 1]

            # Compare and log
            diff = abs(incremental_corr - manual_corr)

            log_msg = (
                f"[VALIDATION] BTC-SOL 1h price correlation: "
                f"Incremental={incremental_corr:.6f}, Manual={manual_corr:.6f}, "
                f"Diff={diff:.6f}, DataPoints={min_len}, "
                f"TrackerCount={tracker.count}"
            )

            if diff > self.CORRELATION_TOLERANCE:
                print("\n" + "=" * 80)
                print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
                print("[CORRELATION MISMATCH DETECTED] Incremental and manual calculations DISAGREE!")
                print(log_msg)
                print(f"BTC prices (last 5): {btc_prices[-5:].tolist()}")
                print(f"SOL prices (last 5): {sol_prices[-5:].tolist()}")
                print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
                print("=" * 80 + "\n")

                # Store error in Redis for monitoring
                error_msg = f"[CORRELATION MISMATCH] {log_msg}"
                self.redis.lpush("error_log", error_msg)
            else:
                print(log_msg)

        except Exception as e:
            print(f"[VALIDATION] Error during BTC-SOL validation: {e}")
            import traceback
            traceback.print_exc()

    def _cache_correlations(self, save_to_db: bool = True):
        """Cache correlation matrices to Redis and optionally DB."""
        pipe = self.redis.pipeline()

        for hours in self.hours_options:
            for data_type in KLINE_FIELD_MAP:
                tracker = self.trackers.get((hours, data_type))
                if not tracker:
                    continue

                matrix = tracker.get_correlations()
                key = f"correlations:{data_type}:{hours}:binance:perpetual"
                packed_data = msgpack.packb(matrix)
                if packed_data is not None:
                    pipe.set(key, packed_data)

                # Save 1h correlations to DB
                if save_to_db and hours == 1:
                    start = time.time()
                    try:
                        save_correlation_matrix_to_db(
                            symbols=self.symbols,
                            correlation_matrix=matrix,
                            data_type=data_type,
                            hours=hours,
                            exchange="binance",
                            contract_type="perpetual",
                        )
                        print(
                            f"Saved {data_type} correlations to DB in {time.time() - start:.2f}s"
                        )
                    except Exception as e:
                        print(f"DB save failed ({data_type}): {e}")

        pipe.execute()
        notification_service.send_correlation_update()

        # Validate BTC-SOL correlation on every update
        self._validate_btc_sol_correlation()

    def update_correlations(
        self,
        newest: Optional[Dict] = None,
        kline_timestamp_ms: Optional[int] = None,
        save_to_db: bool = True,
    ):
        """Main update method - fetch data, update trackers, cache results."""
        with self.lock:
            print(f"[DEBUG] update_correlations called - tracked symbols: {len(self.symbols)}, save_to_db: {save_to_db}")

            # Get newest values if not provided
            if newest is None:
                print("[DEBUG] Fetching newest values (not provided)")
                newest = get_symbol_kline_data(
                    symbols=self.symbols,
                    exchange="binance",
                    contract_type="perpetual",
                )

            # Validate we have data for all symbols
            missing = [s for s in self.symbols if s not in newest]
            if missing:
                print(f"[DEBUG] Missing symbols in newest data: {len(missing)} - {missing[:5]}...")
                try:
                    extra = get_symbol_kline_data(
                        symbols=missing,
                        exchange="binance",
                        contract_type="perpetual",
                    )
                    newest.update(extra)
                    print(f"[DEBUG] Fetched {len(extra)} missing symbols")
                except Exception as e:
                    print(f"Failed to fetch missing symbols: {e}")
                    return

            # Check tracker state before update
            sample_tracker = self.trackers.get((1, "close"))
            if sample_tracker:
                print(f"[DEBUG] Before update - tracker(1h,close): n_symbols={sample_tracker.n_symbols}, count={sample_tracker.count}")

            oldest_by_hours = get_symbol_kline_data_multi_hours(
                symbols=self.symbols,
                exchange="binance",
                contract_type="perpetual",
                hours_list=self.hours_options,
                kline_timestamp_ms=kline_timestamp_ms,
            )

            self._update_trackers(newest, oldest_by_hours)

            # Check tracker state after update
            if sample_tracker:
                print(f"[DEBUG] After update - tracker(1h,close): n_symbols={sample_tracker.n_symbols}, count={sample_tracker.count}")

            self._cache_correlations(save_to_db)

    def add_symbol(self, symbol: str):
        """Add new symbol to tracking."""
        with self.lock:
            print(f"[DEBUG] add_symbol called for: {symbol}")
            print(f"[DEBUG] Current state before add - symbols: {len(self.symbols)}, trackers: {len(self.trackers)}")

            if symbol in self.symbols:
                print(f"[DEBUG] Symbol {symbol} already in list, skipping")
                return

            available = get_exchange_symbols()
            print(f"[DEBUG] Available symbols from exchange: {len(available)}")

            if symbol not in available:
                print(f"Symbol {symbol} not available")
                return

            min_points = min(self.hours_options) * 60
            data = get_historical_kline_data(
                hours=max(self.hours_options), symbols=[symbol]
            )

            if symbol not in data:
                print(f"No data for {symbol}")
                return

            points = min(len(data[symbol].get(dt, [])) for dt in KLINE_FIELD_MAP)
            if points < min_points:
                print(
                    f"Limited data for {symbol}: {points}/{min_points}, adding anyway"
                )

            old_symbols = self.symbols.copy()
            print(f"[DEBUG] Adding {symbol} - rebuilding all trackers")
            self.symbols = get_exchange_symbols()
            self._rebuild_indices()

            # Log symbol list changes
            added = set(self.symbols) - set(old_symbols)
            removed = set(old_symbols) - set(self.symbols)
            if added:
                print(f"[DEBUG] Symbols added to list: {added}")
            if removed:
                print(f"[DEBUG] Symbols removed from list (unexpected): {removed}")

            print(f"[DEBUG] Reinitializing trackers for {len(self.symbols)} symbols")
            self._init_trackers()

            # Verify tracker state
            sample_tracker = self.trackers.get((1, "close"))
            if sample_tracker:
                print(f"[DEBUG] After add_symbol - tracker(1h,close): n_symbols={sample_tracker.n_symbols}, count={sample_tracker.count}")

            self.update_correlations()
            print(f"[DEBUG] add_symbol completed for {symbol}")

    def remove_symbol(self, symbol: str):
        """Remove symbol from tracking."""
        with self.lock:
            print(f"[DEBUG] remove_symbol called for: {symbol}")
            print(f"[DEBUG] Current state before remove - symbols: {len(self.symbols)}, trackers: {len(self.trackers)}")

            if symbol not in self.symbols:
                print(f"[DEBUG] Symbol {symbol} not in list, skipping")
                return

            print(f"Removing {symbol}")
            idx = self.symbol_to_idx[symbol]
            print(f"[DEBUG] Symbol {symbol} has index {idx}")

            old_symbols = self.symbols.copy()
            self.symbols = get_exchange_symbols()
            self._rebuild_indices()

            # Log symbol list changes
            added = set(self.symbols) - set(old_symbols)
            removed = set(old_symbols) - set(self.symbols)
            print(f"[DEBUG] Symbols removed from list: {removed}")
            if added:
                print(f"[DEBUG] Symbols added to list (unexpected during remove): {added}")

            print(f"[DEBUG] New symbol count: {len(self.symbols)}, removing index {idx} from trackers")

            # Remove from all trackers
            for key, tracker in self.trackers.items():
                tracker.sum_x = np.delete(tracker.sum_x, idx)
                tracker.sum_xx = np.delete(tracker.sum_xx, idx)
                tracker.sum_xy = np.delete(tracker.sum_xy, idx, axis=0)
                tracker.sum_xy = np.delete(tracker.sum_xy, idx, axis=1)
                tracker.n_symbols = len(self.symbols)

                # Verify array shapes match
                if tracker.sum_x.shape[0] != tracker.n_symbols:
                    print(f"[DEBUG] MISMATCH in tracker {key}: sum_x shape {tracker.sum_x.shape[0]} != n_symbols {tracker.n_symbols}")
                if tracker.sum_xy.shape[0] != tracker.n_symbols:
                    print(f"[DEBUG] MISMATCH in tracker {key}: sum_xy shape {tracker.sum_xy.shape} != n_symbols {tracker.n_symbols}")

            # Verify final state
            sample_tracker = self.trackers.get((1, "close"))
            if sample_tracker:
                print(f"[DEBUG] After remove_symbol - tracker(1h,close): n_symbols={sample_tracker.n_symbols}, count={sample_tracker.count}")
                print(f"[DEBUG] Array shapes - sum_x: {sample_tracker.sum_x.shape}, sum_xy: {sample_tracker.sum_xy.shape}")

            self._cache_correlations(save_to_db=False)
            print(f"[DEBUG] remove_symbol completed for {symbol}")

    def _log_state_summary(self):
        """Log periodic state summary for debugging."""
        print(f"[DEBUG] === STATE SUMMARY (update #{self.update_count}) ===")
        print(f"[DEBUG] Symbols tracked: {len(self.symbols)}")
        print(f"[DEBUG] Symbol-to-index mapping size: {len(self.symbol_to_idx)}")
        print(f"[DEBUG] Trackers: {len(self.trackers)}")

        for hours in self.hours_options:
            tracker = self.trackers.get((hours, "close"))
            if tracker:
                print(f"[DEBUG] Tracker({hours}h,close): n_symbols={tracker.n_symbols}, count={tracker.count}, sum_x_shape={tracker.sum_x.shape}, sum_xy_shape={tracker.sum_xy.shape}")

                # Check for consistency
                if tracker.n_symbols != len(self.symbols):
                    print(f"[DEBUG] CONSISTENCY ERROR: tracker n_symbols ({tracker.n_symbols}) != len(symbols) ({len(self.symbols)})")
                if tracker.sum_x.shape[0] != tracker.n_symbols:
                    print(f"[DEBUG] CONSISTENCY ERROR: sum_x shape ({tracker.sum_x.shape[0]}) != n_symbols ({tracker.n_symbols})")
                if tracker.sum_xy.shape[0] != tracker.n_symbols:
                    print(f"[DEBUG] CONSISTENCY ERROR: sum_xy shape ({tracker.sum_xy.shape[0]}) != n_symbols ({tracker.n_symbols})")

        print(f"[DEBUG] === END STATE SUMMARY ===")

    def _cleanup_loop(self):
        """Background cleanup of old correlation data."""
        while not self.cleanup_stop.wait(timeout=900):
            try:
                cleanup_old_correlation_data(retention_hours=4)
            except Exception as e:
                print(f"Cleanup failed: {e}")

    def _pubsub_loop(self):
        """Listen for Redis pubsub messages."""
        channels = [
            RedisPubMessages.KLINE_SAVED_TO_DB.value,
            RedisPubMessages.SYMBOL_ADDED.value,
            RedisPubMessages.SYMBOL_DELISTED.value,
        ]

        while True:
            try:
                pubsub = self.redis.pubsub()
                pubsub.subscribe(*channels)
                pubsub.get_message()

                for msg in pubsub.listen():
                    if msg["type"] != "message":
                        continue

                    channel = msg["channel"]
                    data = msg.get("data", b"")

                    if isinstance(data, bytes):
                        data = data.decode("utf-8")

                    if channel == RedisPubMessages.KLINE_SAVED_TO_DB.value:
                        self._handle_kline_update(data)
                    elif channel == RedisPubMessages.SYMBOL_ADDED.value:
                        self.add_symbol(data.split(":")[0])
                    elif channel == RedisPubMessages.SYMBOL_DELISTED.value:
                        self.remove_symbol(data.split(":")[0])

            except (redis.ConnectionError, redis.TimeoutError) as e:
                print(f"Redis error: {e}, reconnecting...")
                time.sleep(5)
            except Exception as e:
                print(f"Pubsub error: {e}")
                time.sleep(5)

    def _validate_newest_values(self, newest: Dict, timestamp: int) -> bool:
        """Validate the newest_values payload for issues."""
        issues = []

        # Check symbol count
        if len(newest) != len(self.symbols):
            issues.append(f"Symbol count mismatch: payload has {len(newest)}, tracking {len(self.symbols)}")

        # Check for missing symbols
        missing_from_payload = set(self.symbols) - set(newest.keys())
        if missing_from_payload:
            issues.append(f"Missing from payload: {list(missing_from_payload)[:10]}{'...' if len(missing_from_payload) > 10 else ''}")

        # Check for extra symbols in payload
        extra_in_payload = set(newest.keys()) - set(self.symbols)
        if extra_in_payload:
            issues.append(f"Extra in payload: {list(extra_in_payload)[:10]}{'...' if len(extra_in_payload) > 10 else ''}")

        # Check for invalid values
        invalid_values = []
        for sym, vals in newest.items():
            if not isinstance(vals, dict):
                invalid_values.append(f"{sym}: not a dict")
                continue
            for key in ["price", "volume", "trades"]:
                if key not in vals:
                    invalid_values.append(f"{sym}: missing {key}")
                elif vals[key] is None or (isinstance(vals[key], float) and (vals[key] != vals[key])):  # NaN check
                    invalid_values.append(f"{sym}: {key} is None/NaN")

        if invalid_values:
            issues.append(f"Invalid values: {invalid_values[:5]}{'...' if len(invalid_values) > 5 else ''}")

        if issues:
            print(f"[PAYLOAD VALIDATION] Issues found in update (ts={timestamp}):")
            for issue in issues:
                print(f"  - {issue}")
            self.redis.lpush("error_log", f"[PAYLOAD VALIDATION] ts={timestamp}: {'; '.join(issues)}")
            return False

        return True

    def _handle_kline_update(self, data: str):
        """Process kline update message."""
        try:
            payload = json.loads(data)
        except json.JSONDecodeError:
            print(f"[DEBUG] Failed to decode kline update JSON: {data[:100]}")
            return

        newest = payload.get("newest_values")
        timestamp = payload.get("timestamp")

        if not isinstance(newest, dict):
            print(f"[DEBUG] newest_values is not a dict: {type(newest)}")
            return

        current_time = time.time()
        time_since_last = current_time - self.last_update_time if self.last_update_time > 0 else 0
        self.update_count += 1

        print(f"[DEBUG] Kline update #{self.update_count} - timestamp: {timestamp}, symbols in payload: {len(newest)}, tracked symbols: {len(self.symbols)}, time_since_last: {time_since_last:.1f}s")

        if time_since_last > 90 and self.last_update_time > 0:
            print(f"[DEBUG] WARNING: Gap of {time_since_last:.1f}s since last update (expected ~60s)")

        # Validate payload
        if self.initialized:
            self._validate_newest_values(newest, timestamp)

        if not self.initialized:
            self.pending_updates.append((newest, timestamp))
            print(f"Queued update (pending: {len(self.pending_updates)})")
            return

        self.last_update_time = current_time
        start = time.time()
        self.update_correlations(newest, timestamp)
        print(f"Update completed in {time.time() - start:.2f}s")

        # Periodic state dump every 10 updates
        if self.update_count % 10 == 0:
            self._log_state_summary()

    def run(self):
        """Main entry point."""
        self.hours_options = list(tf_options["correlation"].values())
        self.symbols = get_exchange_symbols()
        self._rebuild_indices()

        pubsub_thread = threading.Thread(target=self._pubsub_loop, daemon=True)
        pubsub_thread.start()

        cleanup_thread = threading.Thread(target=self._cleanup_loop, daemon=True)
        cleanup_thread.start()

        time.sleep(2)

        print("Initializing correlation trackers...")
        start = time.time()
        self._init_trackers()
        print(f"Initialization completed in {time.time() - start:.2f}s")

        self.initialized = True

        if self.pending_updates:
            print(f"Processing {len(self.pending_updates)} pending updates...")
            for newest, ts in self.pending_updates:
                self.update_correlations(newest, ts, save_to_db=False)
            self.pending_updates.clear()

        print("Ready for real-time updates")

        try:
            pubsub_thread.join()
        except KeyboardInterrupt:
            print("Shutting down...")
            self.cleanup_stop.set()
