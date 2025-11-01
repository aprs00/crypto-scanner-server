import json
import redis
import msgpack
import time
import threading
import gc
import numpy as np
from typing import Dict, List, Optional, Set
from concurrent.futures import ThreadPoolExecutor

from exchange_connections.constants import KLINE_FIELD_MAP
from exchange_connections.selectors import (
    get_exchange_symbols,
    get_historical_kline_data,
    get_symbol_kline_data,
)
from core.constants import RedisPubMessages, tf_options
from core.redis_config import get_redis_connection
from core.notifications import notification_service
from correlations.services.save_correlations import save_correlation_matrix_to_db
from correlations.db_utils import cleanup_old_correlation_data


if np.finfo(np.longdouble).eps < np.finfo(np.float64).eps:
    ACCUM_DTYPE = np.longdouble
else:
    ACCUM_DTYPE = np.float64


class MatrixCorrelationTracker:
    """
    Optimized correlation tracker using dense NumPy arrays.
    For N symbols, stores:
    - N values for sum_x, sum_xx (per symbol)
    - N×N dense matrix for sum_xy (upper triangle used)
    - Single count value
    """

    def __init__(self, window_size: int, n_symbols: int):
        self.window_size = window_size
        self.n_symbols = n_symbols
        self.count = 0

        # Per-symbol statistics
        self.sum_x = np.zeros(n_symbols, dtype=ACCUM_DTYPE)
        self.sum_xx = np.zeros(n_symbols, dtype=ACCUM_DTYPE)

        # Dense sum_xy matrix
        self.sum_xy = np.zeros((n_symbols, n_symbols), dtype=ACCUM_DTYPE)

        # Precompute upper-triangle indices once
        self.upper_i, self.upper_j = np.triu_indices(n_symbols, k=1)

    def initialize_from_data(self, symbol_data: Dict[int, np.ndarray]):
        """Initialize statistics from historical data."""
        if not symbol_data:
            return

        min_length = min(len(data) for data in symbol_data.values())

        # Only initialize if we have sufficient data points
        # This prevents initialization with insufficient data that would corrupt correlations
        if min_length == 0:
            return

        for idx, data in symbol_data.items():
            data_segment = np.asarray(data[:min_length], dtype=ACCUM_DTYPE)
            self.sum_x[idx] = np.sum(data_segment, dtype=ACCUM_DTYPE)
            self.sum_xx[idx] = np.sum(data_segment * data_segment, dtype=ACCUM_DTYPE)

        arr = np.vstack(
            [
                np.asarray(symbol_data[i][:min_length], dtype=ACCUM_DTYPE)
                for i in range(self.n_symbols)
            ]
        )
        self.sum_xy = arr @ arr.T

        self.count = min_length

    def update(self, new_values: np.ndarray, old_values: Optional[np.ndarray] = None):
        """Update statistics with new values (vectorized)."""
        # Remove old values if window is full
        if self.count >= self.window_size and old_values is not None:
            mask = ~np.isnan(old_values)
            if np.any(mask):
                vals = old_values[mask]

                self.sum_x[mask] -= vals
                self.sum_xx[mask] -= vals * vals

                self.sum_xy[np.ix_(mask, mask)] -= np.outer(vals, vals)

            self.count -= 1

        # Add new values
        mask = ~np.isnan(new_values)
        if np.any(mask):
            vals = new_values[mask]

            # Per-symbol
            self.sum_x[mask] += vals
            self.sum_xx[mask] += vals * vals

            # Pairwise update
            self.sum_xy[np.ix_(mask, mask)] += np.outer(vals, vals)

        self.count = min(self.count + 1, self.window_size)

    def get_correlation(self, i: int, j: int) -> float:
        """Compute correlation between symbols i and j."""
        if self.count <= 1:
            return 0.0
        if i == j:
            return 1.0

        dtype = ACCUM_DTYPE
        c = dtype(self.count)

        sum_x = dtype(self.sum_x[i])
        sum_y = dtype(self.sum_x[j])
        sum_xx = dtype(self.sum_xx[i])
        sum_yy = dtype(self.sum_xx[j])
        sum_xy = dtype(self.sum_xy[i, j])

        mean_x = sum_x / c
        mean_y = sum_y / c

        var_x = (sum_xx / c) - mean_x * mean_x
        var_y = (sum_yy / c) - mean_y * mean_y

        if var_x <= 0 or var_y <= 0:
            return 0.0

        cov_xy = (sum_xy / c) - mean_x * mean_y
        denominator = np.sqrt(var_x * var_y)
        if denominator == 0:
            return 0.0

        correlation = cov_xy / denominator
        correlation = np.clip(correlation, -1.0, 1.0)
        return float(correlation)

    def get_correlation_matrix_upper(self, symbol_indices: List[int]) -> List[float]:
        """Get upper triangle of correlation matrix as flat list."""
        results = []
        n = len(symbol_indices)

        for i in range(n):
            for j in range(i + 1, n):
                corr = self.get_correlation(symbol_indices[i], symbol_indices[j])
                results.append(round(corr, 2))

        return results

    def get_correlation_matrix_upper_vectorized(
        self, symbol_indices: List[int]
    ) -> List[float]:
        """
        Vectorized creation of the upper-triangle correlation list.
        Returns list of rounded correlations (2 decimal places) in row-major upper triangle order.
        """
        if self.count == 0:
            return []

        n = len(symbol_indices)
        if n <= 1:
            return []

        indices = np.array(symbol_indices, dtype=np.int32)
        dtype = ACCUM_DTYPE
        c = dtype(self.count)

        Sx = self.sum_x[indices].astype(dtype, copy=False)  # shape (n,)
        Sxx = self.sum_xx[indices].astype(dtype, copy=False)  # shape (n,)
        Sxy = self.sum_xy[np.ix_(indices, indices)].astype(
            dtype, copy=False
        )  # shape (n,n)

        means = Sx / c
        var = (Sxx / c) - means * means
        var = np.where(var <= 0, np.nan, var)

        cov = (Sxy / c) - np.outer(means, means)
        denom = np.sqrt(np.outer(var, var))  # shape (n,n)

        with np.errstate(invalid="ignore", divide="ignore"):
            corr = cov / denom

        corr = np.clip(corr, -1.0, 1.0)
        corr = np.nan_to_num(corr, nan=0.0, posinf=0.0, neginf=0.0)
        np.fill_diagonal(corr, 1.0)

        iu, ju = np.triu_indices(n, k=1)
        upper = corr[iu, ju]

        return [round(float(x), 2) for x in upper]


class MatrixCorrelationCalculator:
    def __init__(self):
        self.r = get_redis_connection()
        self.print_lock = threading.Lock()
        self.correlation_lock = threading.RLock()
        self.symbols = []
        self.symbol_to_idx = {}
        self.idx_to_symbol = {}
        self.hours_options = []
        self.min_hour = 1
        self.trackers = {}  # {(hours, data_type): MatrixCorrelationTracker}
        self.initialization_complete = False
        self.pending_message_count = 0
        self.pending_messages_lock = threading.Lock()
        self.pending_newest_value_batches: List[Dict[str, Dict[str, float]]] = []
        self.pending_symbols: Set[str] = set()
        self.pending_symbols_lock = threading.Lock()
        self.retry_scheduler = ThreadPoolExecutor(
            max_workers=2, thread_name_prefix="symbol-retry"
        )
        self.cleanup_interval_seconds = 15 * 60
        self._cleanup_stop_event = threading.Event()
        self._cleanup_thread: Optional[threading.Thread] = None

    def _sync_symbol_indices(self):
        """Rebuild symbol index mappings to match the current symbol ordering."""
        self.symbol_to_idx = {sym: i for i, sym in enumerate(self.symbols)}
        self.idx_to_symbol = {i: sym for sym, i in self.symbol_to_idx.items()}

    def initialize_correlation_trackers(self):
        """Initialize correlation trackers with optimized data fetching."""
        with self.correlation_lock:
            n_symbols = len(self.symbols)
            print(
                f"Initializing correlations for {n_symbols} symbols ({n_symbols*(n_symbols-1)//2:,} pairs)"
            )

            sorted_hours = sorted(self.hours_options, reverse=True)
            max_hours = sorted_hours[0]

            print(f"Fetching data for largest timeframe: {max_hours}h...")
            start_fetch = time.time()

            max_symbols_data = get_historical_kline_data(
                hours=max_hours, symbols=self.symbols
            )

            fetch_time = time.time() - start_fetch
            print(f"  Data fetch completed in {fetch_time:.2f}s")

            for hours in sorted_hours:
                print(f"Processing timeframe: {hours}h...")

                window_size = hours * 60

                if hours == max_hours:
                    symbols_data = max_symbols_data
                else:
                    symbols_data = {}
                    for symbol in self.symbols:
                        if symbol in max_symbols_data:
                            symbols_data[symbol] = {}
                            for data_type, data in max_symbols_data[symbol].items():
                                symbols_data[symbol][data_type] = (
                                    data[-window_size:]
                                    if len(data) >= window_size
                                    else data
                                )

                for data_type in KLINE_FIELD_MAP.keys():
                    tracker = MatrixCorrelationTracker(window_size, n_symbols)

                    indexed_data = {}
                    for symbol in self.symbols:
                        if symbol in symbols_data and data_type in symbols_data[symbol]:
                            idx = self.symbol_to_idx[symbol]
                            data = np.asarray(
                                symbols_data[symbol][data_type], dtype=ACCUM_DTYPE
                            )
                            indexed_data[idx] = data

                    if indexed_data:
                        tracker.initialize_from_data(indexed_data)

                    self.trackers[(hours, data_type)] = tracker

                print(f"  Timeframe {hours}h completed\n")

            del max_symbols_data
            gc.collect()
            print("All timeframes initialization completed\n")

    def _ensure_tracker_alignment(
        self, hours: int, data_type: str
    ) -> Optional[MatrixCorrelationTracker]:
        """
        Ensure a tracker exists for the requested timeframe/data type and that its
        internal arrays match the current symbol set. If mismatched, rebuild the
        trackers so updates run against correctly sized matrices.
        """
        tracker = self.trackers.get((hours, data_type))
        expected_symbols = len(self.symbols)

        if expected_symbols == 0:
            return tracker

        tracker_size = tracker.sum_x.shape[0] if tracker is not None else 0
        if tracker is not None and tracker_size == expected_symbols:
            return tracker

        reason = (
            "missing"
            if tracker is None
            else f"dimension mismatch (expected {expected_symbols}, got {tracker_size})"
        )

        with self.print_lock:
            print(
                f"Tracker misalignment detected for {hours}h {data_type}: {reason}. "
                "Rebuilding trackers."
            )

        try:
            refreshed_symbols = get_exchange_symbols()
        except Exception as exc:
            refreshed_symbols = None
            with self.print_lock:
                print(
                    f"Failed to refresh exchange symbols while rebalancing trackers: {exc}"
                )

        if refreshed_symbols:
            if refreshed_symbols != self.symbols:
                self.symbols = refreshed_symbols
        self._sync_symbol_indices()
        expected_symbols = len(self.symbols)

        self.initialize_correlation_trackers()

        tracker = self.trackers.get((hours, data_type))
        if tracker is None:
            with self.print_lock:
                print(
                    f"Tracker {hours}h {data_type} unavailable after rebuild; skipping update."
                )
            return None

        tracker_size = tracker.sum_x.shape[0]
        if tracker_size != expected_symbols:
            with self.print_lock:
                print(
                    f"Tracker {hours}h {data_type} still mismatched after rebuild "
                    f"(expected {expected_symbols}, got {tracker_size}); skipping update."
                )
            return None

        return tracker

    def update_correlations(self, hours: int, newest_values: dict, oldest_values: dict):
        """Update correlation trackers with new data."""
        window_size = hours * 60

        for data_type in KLINE_FIELD_MAP.keys():
            tracker = self._ensure_tracker_alignment(hours, data_type)
            if tracker is None:
                continue

            n_symbols = len(self.symbols)
            if tracker.sum_x.shape[0] != n_symbols:
                with self.print_lock:
                    print(
                        f"Skipping update for {hours}h {data_type}: tracker size mismatch "
                        f"(tracker={tracker.sum_x.shape[0]}, symbols={n_symbols})"
                    )
                continue

            new_vals = np.full(n_symbols, np.nan, dtype=ACCUM_DTYPE)
            old_vals = np.full(n_symbols, np.nan, dtype=ACCUM_DTYPE)

            for symbol in self.symbols:
                if symbol not in self.symbol_to_idx:
                    continue

                idx = self.symbol_to_idx[symbol]

                if symbol in newest_values and data_type in newest_values[symbol]:
                    new_vals[idx] = ACCUM_DTYPE(newest_values[symbol][data_type])

                if tracker.count >= window_size:
                    if symbol in oldest_values and data_type in oldest_values[symbol]:
                        old_vals[idx] = ACCUM_DTYPE(oldest_values[symbol][data_type])

            if tracker.count >= window_size:
                tracker.update(new_vals, old_vals)
            else:
                tracker.update(new_vals)

    def create_correlation_matrix(self, hours: int, data_type: str) -> List[float]:
        """Create correlation matrix for specific hours/data_type."""
        tracker = self.trackers.get((hours, data_type))
        if tracker is None:
            return []

        symbol_indices = [self.symbol_to_idx[s] for s in self.symbols]
        return tracker.get_correlation_matrix_upper_vectorized(symbol_indices)

    def _prepare_newest_values(
        self, newest_values: Optional[Dict[str, Dict[str, float]]]
    ) -> Optional[Dict[str, Dict[str, float]]]:
        """
        Ensure we have the latest values for every tracked symbol before updating trackers.
        Returns a complete mapping or None if the data cannot be made complete.
        """
        if newest_values is None:
            return None

        complete_values: Dict[str, Dict[str, float]] = {
            symbol: dict(values) for symbol, values in newest_values.items()
        }

        missing_symbols = [s for s in self.symbols if s not in complete_values]

        if missing_symbols:
            try:
                supplemental = get_symbol_kline_data(
                    symbols=missing_symbols,
                    exchange="binance",
                    contract_type="perpetual",
                )
            except Exception as exc:
                with self.print_lock:
                    print(
                        "Failed to backfill newest values for symbols "
                        f"{missing_symbols[:5]}{'...' if len(missing_symbols) > 5 else ''}: {exc}"
                    )
                return None

            for symbol, values in supplemental.items():
                symbol_entry = complete_values.setdefault(symbol, {})
                for data_type, value in values.items():
                    if data_type not in symbol_entry or symbol_entry[data_type] is None:
                        symbol_entry[data_type] = value

        incomplete_symbols = [
            symbol
            for symbol in self.symbols
            if symbol not in complete_values
            or any(
                data_type not in complete_values[symbol]
                or complete_values[symbol][data_type] is None
                for data_type in KLINE_FIELD_MAP.keys()
            )
        ]

        if incomplete_symbols:
            with self.print_lock:
                print(
                    "Skipping correlation update; incomplete newest_values for "
                    f"{incomplete_symbols[:5]}{'...' if len(incomplete_symbols) > 5 else ''}"
                )
            return None

        return complete_values

    def update_and_cache_incremental_correlations(
        self,
        *,
        save_to_db: bool = True,
        newest_values: Optional[Dict[str, Dict[str, float]]] = None,
    ):
        """Update correlations and cache to Redis."""
        with self.correlation_lock:
            if newest_values is None:
                newest_values = get_symbol_kline_data(
                    symbols=self.symbols, exchange="binance", contract_type="perpetual"
                )

            newest_values = self._prepare_newest_values(newest_values)
            if newest_values is None:
                return

            set_pipeline = self.r.pipeline()

            for hours in self.hours_options:
                start_oldest = time.time()
                oldest_values = get_symbol_kline_data(
                    symbols=self.symbols,
                    exchange="binance",
                    contract_type="perpetual",
                    hours=hours,
                )
                oldest_time = time.time() - start_oldest
                print(f"Getting oldest_values for {hours}h took {oldest_time:.3f}s")

                self.update_correlations(
                    hours=hours,
                    newest_values=newest_values,
                    oldest_values=oldest_values,
                )

                for data_type in KLINE_FIELD_MAP.keys():
                    correlation_matrix = self.create_correlation_matrix(
                        hours, data_type
                    )

                    set_pipeline.execute_command(
                        "SET",
                        f"correlations:{data_type}:{hours}:binance:perpetual",
                        msgpack.packb(correlation_matrix),
                    )

                    if save_to_db and hours == 1:
                        try:
                            start_save = time.time()
                            saved_count = save_correlation_matrix_to_db(
                                symbols=self.symbols,
                                correlation_matrix=correlation_matrix,
                                data_type=data_type,
                                hours=hours,
                                exchange="binance",
                                contract_type="perpetual",
                            )
                            save_time = time.time() - start_save
                            if saved_count > 0:
                                print(
                                    f"Saved {saved_count} correlations to DB ({data_type}, {hours}h) in {save_time:.3f}s"
                                )
                        except Exception as e:
                            print(
                                f"Failed to save correlations to DB ({data_type}, {hours}h): {e}"
                            )

            set_pipeline.execute()

            notification_service.send_correlation_update()


    def _start_cleanup_scheduler(self):
        """Start background thread to periodically purge old correlation data."""
        if self._cleanup_thread and self._cleanup_thread.is_alive():
            return

        def _cleanup_loop():
            while not self._cleanup_stop_event.is_set():
                try:
                    cleanup_old_correlation_data(retention_hours=4)
                except Exception as exc:
                    print(f"Failed to cleanup correlation data: {exc}")

                if self._cleanup_stop_event.wait(timeout=self.cleanup_interval_seconds):
                    break

        self._cleanup_thread = threading.Thread(
            target=_cleanup_loop,
            name="correlation-cleanup",
            daemon=True,
        )
        self._cleanup_thread.start()

    def schedule_symbol_retry(self, symbol_name: str):
        """Schedule a retry to add symbol after delay to allow data accumulation."""
        with self.pending_symbols_lock:
            if symbol_name in self.pending_symbols:
                print(f"Symbol {symbol_name} already scheduled for retry")
                return

            self.pending_symbols.add(symbol_name)
            print(f"Scheduling retry for {symbol_name} in {self.min_hour} hour(s)")

        def retry_add_symbol():
            """Background task to retry adding symbol after delay."""
            retry_delay_seconds = self.min_hour * 3600
            time.sleep(retry_delay_seconds)

            with self.pending_symbols_lock:
                if symbol_name in self.pending_symbols:
                    self.pending_symbols.remove(symbol_name)

            print(
                f"Retrying to add symbol {symbol_name} after {self.min_hour} hour delay"
            )
            self.add_new_symbol(symbol_name, is_retry=True)

        self.retry_scheduler.submit(retry_add_symbol)

    def add_new_symbol(self, symbol_name: str, is_retry: bool = False):
        """Add a new symbol to correlation tracking."""
        with self.correlation_lock:
            if symbol_name in self.symbols:
                print(f"Symbol {symbol_name} already being tracked in correlations")
                return

            if not self.hours_options:
                print(
                    f"Correlation hours options not initialized; skipping add for {symbol_name}"
                )
                return

            available_symbols = get_exchange_symbols()
            if symbol_name not in available_symbols:
                print(
                    f"Symbol {symbol_name} not found in exchange symbol list; cannot add yet"
                )
                return

            max_hours = max(self.hours_options)
            min_hours = min(self.hours_options)
            required_points = min_hours * 60

            historical_data = get_historical_kline_data(
                hours=max_hours, symbols=[symbol_name]
            )
            symbol_history = historical_data.get(symbol_name)

            available_points = (
                min(
                    len(symbol_history.get(data_type, []))
                    for data_type in KLINE_FIELD_MAP.keys()
                )
                if symbol_history
                else 0
            )
            has_required_history = (
                bool(symbol_history) and available_points >= required_points
            )

            if not has_required_history:
                print(
                    f"Insufficient historical data for {symbol_name}: have {available_points}/{required_points} points; scheduling retry"
                )
                self.schedule_symbol_retry(symbol_name)
                return

            with self.pending_symbols_lock:
                self.pending_symbols.discard(symbol_name)

            print(f"Adding symbol {symbol_name} to correlation tracking")

            self.symbols = available_symbols
            self._sync_symbol_indices()

            if self.initialization_complete:
                self.initialize_correlation_trackers()
                self.update_and_cache_incremental_correlations()
                print(
                    f"Successfully added {symbol_name}; now tracking {len(self.symbols)} symbols"
                )
            else:
                # Trackers will be built during initial initialization flow
                print(
                    f"Initialization in progress; {symbol_name} will be included once setup completes"
                )

    def remove_symbol(self, symbol_name: str):
        """Remove a symbol from correlation tracking."""
        with self.correlation_lock:
            if symbol_name not in self.symbols:
                print(f"Symbol {symbol_name} not being tracked in correlations")
                return

            print(f"Removing symbol {symbol_name} from correlation tracking")

            idx_to_remove = self.symbol_to_idx[symbol_name]

            self.symbols = get_exchange_symbols()

            self._sync_symbol_indices()

            for tracker in self.trackers.values():
                tracker.sum_x = np.delete(tracker.sum_x, idx_to_remove)
                tracker.sum_xx = np.delete(tracker.sum_xx, idx_to_remove)

                tracker.sum_xy = np.delete(tracker.sum_xy, idx_to_remove, axis=0)
                tracker.sum_xy = np.delete(tracker.sum_xy, idx_to_remove, axis=1)

                tracker.n_symbols = len(self.symbols)
                if tracker.n_symbols > 0:
                    tracker.upper_i, tracker.upper_j = np.triu_indices(
                        tracker.n_symbols, k=1
                    )

            print(
                f"Successfully removed {symbol_name} from {len(self.trackers)} trackers"
            )

    def process_pending_messages(self):
        """Process any messages that were queued during initialization."""
        with self.pending_messages_lock:
            if not self.pending_newest_value_batches:
                print("No pending messages to process")
                return

            message_count = len(self.pending_newest_value_batches)
            batches = list(self.pending_newest_value_batches)
            self.pending_newest_value_batches.clear()
            self.pending_message_count = 0

            print(f"Processing {message_count} pending messages...")

        start_time = time.time()
        for newest_values in batches:
            self.update_and_cache_incremental_correlations(
                save_to_db=False, newest_values=newest_values
            )
        elapsed = time.time() - start_time
        print(f"Processed {message_count} pending messages in {elapsed:.2f}s")

    def start_pubsub_listener(self):
        """Listen for Redis pubsub messages with reconnection logic."""
        retries = 0

        while True:
            try:
                print("Subscribing to Redis channels...")
                pubsub = self.r.pubsub()
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
                            print(f'CORRELATIONS {message["channel"]}')

                            data_raw = message.get("data")
                            if data_raw is None:
                                print("Received empty payload; skipping")
                                continue

                            if isinstance(data_raw, bytes):
                                data_text = data_raw.decode("utf-8")
                            elif isinstance(data_raw, str):
                                data_text = data_raw
                            else:
                                print(
                                    f"Unexpected payload type {type(data_raw)}; skipping"
                                )
                                continue

                            try:
                                payload = json.loads(data_text)
                            except (TypeError, json.JSONDecodeError):
                                print(
                                    "Failed to decode newest values payload; skipping"
                                )
                                continue

                            newest_values = payload.get("newest_values")
                            if not isinstance(newest_values, dict):
                                print("No newest_values found in payload; skipping")
                                continue

                            if not self.initialization_complete:
                                with self.pending_messages_lock:
                                    self.pending_newest_value_batches.append(
                                        newest_values
                                    )
                                    self.pending_message_count = len(
                                        self.pending_newest_value_batches
                                    )
                                    print(
                                        f"Queued message during initialization. Queue size: {self.pending_message_count}"
                                    )
                                continue

                            start_time = time.time()
                            self.update_and_cache_incremental_correlations(
                                newest_values=newest_values
                            )
                            elapsed = time.time() - start_time
                            with self.print_lock:
                                print(
                                    f"update_and_cache_incremental_correlations finished in {elapsed:.2f}s"
                                )

                        elif channel == RedisPubMessages.SYMBOL_ADDED.value:
                            data = message.get("data", b"").decode("utf-8")
                            print(f"CORRELATIONS: Received symbol added event: {data}")
                            symbol_name = data.split(":")[0]
                            self.add_new_symbol(symbol_name)

                        elif channel == RedisPubMessages.SYMBOL_DELISTED.value:
                            data = message.get("data", b"").decode("utf-8")
                            print(
                                f"CORRELATIONS: Received symbol delisted event: {data}"
                            )
                            symbol_name = data.split(":")[0]
                            self.remove_symbol(symbol_name)

                retries = 0

            except (redis.ConnectionError, redis.TimeoutError) as e:
                retries += 1
                wait = min(2**retries, 60)
                print(
                    f"[Redis Listener] Disconnected from Redis: {e}. Retrying in {wait}s..."
                )
                time.sleep(wait)

            except Exception as e:
                print(f"[Redis Listener] Unexpected error: {e}")
                time.sleep(5)

    def run(self):
        """Main entry point to start the correlation calculator."""
        self.hours_options = list(tf_options["correlation"].values())
        self.min_hour = self.hours_options[0]
        self.symbols = get_exchange_symbols()

        # Build symbol index mappings
        self._sync_symbol_indices()

        print("Starting pubsub listener thread...")
        pubsub_thread = threading.Thread(target=self.start_pubsub_listener)
        pubsub_thread.start()

        self._start_cleanup_scheduler()

        time.sleep(2)

        print("Starting correlation trackers initialization...")
        start_time = time.time()
        self.initialize_correlation_trackers()
        elapsed = time.time() - start_time
        print(f"Correlation trackers initialization completed in {elapsed:.2f}s")

        self.initialization_complete = True
        print("Initialization complete - now processing any pending messages")

        self.process_pending_messages()
        print("Ready for real-time message processing")

        try:
            pubsub_thread.join()
        except KeyboardInterrupt:
            print("Shutting down...")
        finally:
            print("Stopping cleanup scheduler...")
            self._cleanup_stop_event.set()
            if self._cleanup_thread and self._cleanup_thread.is_alive():
                self._cleanup_thread.join(timeout=5)
            print("Cleanup scheduler stopped")

            print("Shutting down retry scheduler...")
            self.retry_scheduler.shutdown(wait=False)
            print("Retry scheduler shut down")
