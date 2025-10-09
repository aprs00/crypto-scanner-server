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
        self.sum_x = np.zeros(n_symbols, dtype=np.float64)
        self.sum_xx = np.zeros(n_symbols, dtype=np.float64)

        # Dense sum_xy matrix
        self.sum_xy = np.zeros((n_symbols, n_symbols), dtype=np.float64)

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
            data = data[:min_length]
            self.sum_x[idx] = np.sum(data)
            self.sum_xx[idx] = np.sum(data * data)

        arr = np.vstack([symbol_data[i][:min_length] for i in range(self.n_symbols)])
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
        if self.count == 0:
            return 0.0
        if i == j:
            return 1.0

        sum_xy = self.sum_xy[i, j]
        var_x = self.count * self.sum_xx[i] - self.sum_x[i] ** 2
        var_y = self.count * self.sum_xx[j] - self.sum_x[j] ** 2

        if var_x <= 0 or var_y <= 0:
            return 0.0

        numerator = self.count * sum_xy - self.sum_x[i] * self.sum_x[j]
        denominator = np.sqrt(var_x * var_y)

        if denominator == 0:
            return 0.0
        return float(numerator / denominator)

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
        Sx = self.sum_x[indices]  # shape (n,)
        Sxx = self.sum_xx[indices]  # shape (n,)
        Sxy = self.sum_xy[np.ix_(indices, indices)]  # shape (n,n)
        c = self.count

        num = c * Sxy - np.outer(Sx, Sx)

        var = c * Sxx - Sx * Sx  # shape (n,)
        var = np.where(var <= 0, np.nan, var)

        denom = np.sqrt(np.outer(var, var))  # shape (n,n)

        corr = num / denom
        np.fill_diagonal(corr, 1.0)

        corr = np.nan_to_num(corr, nan=0.0, posinf=0.0, neginf=0.0)

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
        self.pending_symbols: Set[str] = set()
        self.pending_symbols_lock = threading.Lock()
        self.retry_scheduler = ThreadPoolExecutor(
            max_workers=2, thread_name_prefix="symbol-retry"
        )

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
                                symbols_data[symbol][data_type], dtype=np.float64
                            )
                            indexed_data[idx] = data

                    if indexed_data:
                        tracker.initialize_from_data(indexed_data)

                    self.trackers[(hours, data_type)] = tracker

                print(f"  Timeframe {hours}h completed\n")

            del max_symbols_data
            gc.collect()
            print("All timeframes initialization completed\n")

    def update_correlations(self, hours: int, newest_values: dict, oldest_values: dict):
        """Update correlation trackers with new data."""
        window_size = hours * 60

        for data_type in KLINE_FIELD_MAP.keys():
            tracker = self.trackers.get((hours, data_type))
            if tracker is None:
                print(f"Missing tracker for {hours}h {data_type}")
                continue

            n_symbols = len(self.symbols)
            new_vals = np.full(n_symbols, np.nan, dtype=np.float64)
            old_vals = np.full(n_symbols, np.nan, dtype=np.float64)

            for symbol in self.symbols:
                if symbol not in self.symbol_to_idx:
                    continue

                idx = self.symbol_to_idx[symbol]

                if symbol in newest_values and data_type in newest_values[symbol]:
                    new_vals[idx] = float(newest_values[symbol][data_type])

                if tracker.count >= window_size:
                    if symbol in oldest_values and data_type in oldest_values[symbol]:
                        old_vals[idx] = float(oldest_values[symbol][data_type])

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

    def update_and_cache_incremental_correlations(self):
        """Update correlations and cache to Redis."""
        with self.correlation_lock:
            newest_values = get_symbol_kline_data(
                symbols=self.symbols, exchange="binance", contract_type="perpetual"
            )

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

            set_pipeline.execute()

            notification_service.send_correlation_update()

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
        return
        """Add a new symbol to correlation tracking."""
        with self.correlation_lock:
            start_time = time.time()
            if symbol_name in self.symbols:
                print(f"Symbol {symbol_name} already being tracked in correlations")
                return

            with self.pending_symbols_lock:
                if symbol_name in self.pending_symbols:
                    print(
                        f"Symbol {symbol_name} is already pending retry, skipping duplicate add"
                    )
                    return

            if not is_retry:
                print(
                    f"Adding {symbol_name} to correlation tracking - scheduling retry in {self.min_hour} hour(s)"
                )
                self.schedule_symbol_retry(symbol_name)
                return

            print(
                f"Retry: Adding {symbol_name} to correlation tracking with accumulated data"
            )

            old_symbols = self.symbols.copy()

            self.symbols = get_exchange_symbols()

            self.symbol_to_idx = {sym: i for i, sym in enumerate(self.symbols)}
            self.idx_to_symbol = {i: sym for sym, i in self.symbol_to_idx.items()}

            new_idx = self.symbol_to_idx[symbol_name]

            fetch_start = time.time()

            new_symbol_data = get_historical_kline_data(
                hours=self.min_hour, symbols=[symbol_name]
            )

            all_existing_data = get_historical_kline_data(
                hours=self.min_hour, symbols=old_symbols
            )

            fetch_time = time.time() - fetch_start
            print(f"Data fetch completed in {fetch_time:.2f}s")

            for (hours, data_type), tracker in self.trackers.items():
                old_n = tracker.n_symbols
                new_n = len(self.symbols)

                new_sum_x = np.zeros(new_n, dtype=np.float64)
                new_sum_xx = np.zeros(new_n, dtype=np.float64)
                new_sum_x[:old_n] = tracker.sum_x
                new_sum_xx[:old_n] = tracker.sum_xx
                tracker.sum_x = new_sum_x
                tracker.sum_xx = new_sum_xx

                new_sum_xy = np.zeros((new_n, new_n), dtype=np.float64)
                new_sum_xy[:old_n, :old_n] = tracker.sum_xy
                tracker.sum_xy = new_sum_xy

                tracker.n_symbols = new_n
                tracker.upper_i, tracker.upper_j = np.triu_indices(new_n, k=1)

                if (
                    symbol_name in new_symbol_data
                    and data_type in new_symbol_data[symbol_name]
                ):
                    data = np.asarray(
                        new_symbol_data[symbol_name][data_type], dtype=np.float64
                    )

                    use_count = min(tracker.count, len(data))

                    if use_count > 0:
                        recent_data = data[-use_count:]

                        tracker.sum_x[new_idx] = np.sum(recent_data)
                        tracker.sum_xx[new_idx] = np.sum(recent_data * recent_data)
                        tracker.sum_xy[new_idx, new_idx] = np.sum(
                            recent_data * recent_data
                        )

                        for existing_idx in range(old_n):
                            existing_symbol = old_symbols[existing_idx]

                            if (
                                existing_symbol in all_existing_data
                                and data_type in all_existing_data[existing_symbol]
                            ):
                                other_data = np.asarray(
                                    all_existing_data[existing_symbol][data_type],
                                    dtype=np.float64,
                                )
                                if len(other_data) >= use_count:
                                    aligned_other = other_data[-use_count:]
                                    cross = np.sum(aligned_other * recent_data)
                                    tracker.sum_xy[existing_idx, new_idx] = cross
                                    tracker.sum_xy[new_idx, existing_idx] = cross

            print(f"Successfully added {symbol_name} to {len(self.trackers)} trackers")
            elapsed = time.time() - start_time
            print(f"Add symbol operation took {elapsed:.2f}s")

    def remove_symbol(self, symbol_name: str):
        return
        """Remove a symbol from correlation tracking."""
        with self.correlation_lock:
            if symbol_name not in self.symbols:
                print(f"Symbol {symbol_name} not being tracked in correlations")
                return

            print(f"Removing symbol {symbol_name} from correlation tracking")

            idx_to_remove = self.symbol_to_idx[symbol_name]

            self.symbols = get_exchange_symbols()

            self.symbol_to_idx = {sym: i for i, sym in enumerate(self.symbols)}
            self.idx_to_symbol = {i: sym for sym, i in self.symbol_to_idx.items()}

            # Remove the symbol from all trackers
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
            if self.pending_message_count == 0:
                print("No pending messages to process")
                return

            message_count = self.pending_message_count
            print(f"Processing {message_count} pending messages...")

            start_time = time.time()
            for _ in range(message_count):
                self.update_and_cache_incremental_correlations()
            elapsed = time.time() - start_time

            self.pending_message_count = 0
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

                            if not self.initialization_complete:
                                with self.pending_messages_lock:
                                    self.pending_message_count += 1
                                    print(
                                        f"Queued message during initialization. Queue size: {self.pending_message_count}"
                                    )
                                continue

                            start_time = time.time()
                            self.update_and_cache_incremental_correlations()
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
        for i, symbol in enumerate(self.symbols):
            self.symbol_to_idx[symbol] = i
            self.idx_to_symbol[i] = symbol

        print("Starting pubsub listener thread...")
        pubsub_thread = threading.Thread(target=self.start_pubsub_listener)
        pubsub_thread.start()

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
            print("Shutting down retry scheduler...")
            self.retry_scheduler.shutdown(wait=False)
            print("Retry scheduler shut down")
