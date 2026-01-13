import time
import threading
import gc
from collections import deque
import numpy as np
import redis
import msgpack
from typing import Any, Dict, List, Optional, cast

from exchange_connections.constants import KLINE_FIELD_MAP, get_btc_symbol
from exchange_connections.selectors import (
    get_exchange_symbols,
    get_historical_kline_data,
    get_symbol_kline_data,
)
from core.constants import RedisStreamKeys, EXCHANGE_CONFIG, Exchange
from core.redis_config import get_redis_connection
from core.redis_streams import StreamConsumer
from core.notifications import notification_service
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

        self.sum_x = np.nansum(arr, axis=1)
        self.sum_xx = np.nansum(arr * arr, axis=1)

        arr_zeroed = np.nan_to_num(arr, nan=0.0)
        self.sum_xy = arr_zeroed @ arr_zeroed.T

        valid_counts = np.sum(~np.isnan(arr), axis=1)
        self.count = int(np.max(valid_counts))

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

        # Buffer to store last 60 pubsub price points for validation
        self._pubsub_prices: Dict[str, deque] = {}
        self._pubsub_timestamps: deque = deque(maxlen=60)

    def _get_validation_symbols(self) -> List[str]:
        """Get validation symbol pair for the exchange."""
        btc = get_btc_symbol(self.exchange)
        sol = "SOLUSDT" if self.exchange == Exchange.BINANCE else "SOL"
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
                    if sym in all_data and data_type in all_data[sym]:
                        idx = self.symbol_to_idx[sym]
                        data = np.asarray(all_data[sym][data_type], dtype=np.float64)
                        if len(data) >= window:
                            indexed[idx] = data[-window:]
                        else:
                            padded = np.full(window, np.nan, dtype=np.float64)
                            padded[-len(data) :] = data
                            indexed[idx] = padded

                if indexed:
                    tracker.initialize(indexed)

                self.trackers[(hours, data_type)] = tracker

            print(f"[{self.exchange}] Initialized {hours}h timeframe")

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

                if hours == 1 and data_type == "close":
                    new_valid = np.sum(~np.isnan(new_arr))
                    old_valid = (
                        np.sum(~np.isnan(old_arr)) if tracker.count >= window else 0
                    )
                    if missing_new > 0 or missing_old > 0:
                        print(
                            # f"[DEBUG] _update_tackers(1h,close): new_valid={new_valid}/{n}, old_valid={old_valid}/{n}, missing_new={missing_new}, missing_old={missing_old}"
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
            if min_len < 10:
                print(f"[VALIDATION] Not enough data points: {min_len}")
                return

            btc_prices = btc_prices[-min_len:]
            sol_prices = sol_prices[-min_len:]

            manual_corr = np.corrcoef(btc_prices, sol_prices)[0, 1]

            diff = abs(incremental_corr - manual_corr)

            pubsub_btc_prices = [
                p["price"] for p in self._pubsub_prices.get(btc_sym, [])
            ]
            pubsub_sol_prices = [
                p["price"] for p in self._pubsub_prices.get(sol_sym, [])
            ]

            # Print comparison
            print("\n" + "=" * 80)
            print(
                f"[VALIDATION] BTC-SOL 1h price - Incremental={incremental_corr:.6f}, Numpy={manual_corr:.6f}, Diff={diff:.6f}"
            )
            print(f"  PS BTC ({len(pubsub_btc_prices)}): {pubsub_btc_prices}")
            print(f"  DB BTC ({len(btc_prices)}): {btc_prices.tolist()}")
            print(f"  PS SOL ({len(pubsub_sol_prices)}): {pubsub_sol_prices}")
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
        oldest_values: Optional[Dict[int, Dict]] = None,
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

            missing = [s for s in self.symbols if s not in newest]
            if missing:
                print(
                    f"[{self.exchange}][DEBUG] Missing symbols in newest data: {len(missing)} - {missing[:5]}..."
                )
                try:
                    extra = get_symbol_kline_data(
                        symbols=missing,
                        exchange=self.exchange,
                        contract_type=self.contract_type,
                    )
                    newest.update(extra)
                    print(
                        f"[{self.exchange}][DEBUG] Fetched {len(extra)} missing symbols"
                    )
                except Exception as e:
                    print(f"[{self.exchange}] Failed to fetch missing symbols: {e}")
                    return

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

    def _handle_kline_message(self, msg_id: str, msg_data: Dict[str, Any]) -> bool:
        """
        Handle a single kline message from Redis Stream.

        Returns:
            True if processed successfully (will be ACKed)
            False if processing failed (will retry)
        """
        try:
            # Filter by exchange
            if msg_data.get("exchange") != str(self.exchange):
                return True  # ACK, not for us

            # Extract data
            newest_values = msg_data.get("newest_values", {})
            oldest_values = msg_data.get("oldest_values", {})
            timestamp_raw = msg_data.get("timestamp")

            if timestamp_raw is None:
                print(f"[{self.exchange}] Missing timestamp in kline message")
                return True  # ACK to skip invalid message

            try:
                timestamp = int(cast(int | str, timestamp_raw))
            except (TypeError, ValueError):
                print(f"[{self.exchange}] Invalid timestamp: {timestamp_raw!r}")
                return True  # ACK to skip invalid message

            # Process update
            self._handle_kline_update_from_stream(newest_values, timestamp, oldest_values)
            return True  # Success

        except Exception as e:
            print(f"[{self.exchange}] Error processing kline message: {e}")
            return False  # Retry

    def _handle_symbol_message(self, msg_id: str, msg_data: Dict[str, Any]) -> bool:
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

            symbol_raw = msg_data.get("symbol")
            event = msg_data.get("event")

            if not isinstance(symbol_raw, str) or not symbol_raw:
                print(f"[{self.exchange}] Invalid symbol: {symbol_raw!r}")
                return True  # ACK to skip invalid message

            if event == "ADDED":
                self.add_symbol(symbol_raw)
            elif event == "DELISTED":
                self.remove_symbol(symbol_raw)

            return True  # Success

        except Exception as e:
            print(f"[{self.exchange}] Error processing symbol message: {e}")
            return True  # ACK anyway for symbol events to avoid blocking

    def _handle_kline_update_from_stream(
        self, newest: Dict, timestamp: int, oldest_values: Dict
    ):
        """Handle kline update from Redis Stream (already decoded)."""
        self.last_update_time = time.time()
        self.update_count += 1

        time_since_last = (
            self.last_update_time - self.last_update_time
            if self.last_update_time > 0
            else 0
        )

        print(
            f"[{self.exchange}][DEBUG] Kline update #{self.update_count} - timestamp: {timestamp}, "
            f"symbols in payload: {len(newest)}, tracked symbols: {len(self.symbols)}"
        )

        if self.initialized:
            self._validate_newest_values(newest, timestamp)

        self._store_pubsub_prices(newest, timestamp)
        self.update_correlations(newest, oldest_values)


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
        """Main entry point."""
        print(
            f"[{self.exchange}] Starting correlation calculator with Redis Streams..."
        )
        self.hours_options = list(
            EXCHANGE_CONFIG[self.exchange]["hours_options"]["correlation"].values()
        )
        self.symbols = get_exchange_symbols(
            exchange=self.exchange, contract_type=self.contract_type
        )
        self._rebuild_indices()

        # Initialize trackers from historical data first
        print(f"[{self.exchange}] Initializing correlation trackers...")
        start = time.time()
        self._init_trackers()
        print(
            f"[{self.exchange}] Initialization completed in {time.time() - start:.2f}s"
        )

        # Mark as initialized - handlers can now process messages
        self.initialized = True

        # Set up stream consumers
        klines_stream = RedisStreamKeys.klines(self.exchange)
        symbols_stream = RedisStreamKeys.symbols(self.exchange)
        group_name = RedisStreamKeys.consumer_group("correlations", self.exchange)

        klines_consumer = StreamConsumer(self.redis, klines_stream, group_name)
        symbols_consumer = StreamConsumer(self.redis, symbols_stream, group_name)

        # Create consumer groups - start from latest to only process new messages
        klines_consumer.create_consumer_group(start_id="$")
        symbols_consumer.create_consumer_group(start_id="$")

        print(f"[{self.exchange}] Starting stream consumers...")

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

        print(f"[{self.exchange}] Ready for real-time updates from streams")

        try:
            klines_thread.join()
            symbols_thread.join()
        except KeyboardInterrupt:
            print(f"[{self.exchange}] Shutting down...")
            klines_consumer.stop()
            symbols_consumer.stop()
