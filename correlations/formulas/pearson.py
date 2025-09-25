import redis
import msgpack
import time
import threading
import gc
import numpy as np
from scipy.sparse import dok_matrix
from typing import Dict, List, Tuple, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

from exchange_connections.constants import KLINE_FIELD_MAP
from exchange_connections.selectors import (
    get_exchange_symbols,
    get_historical_kline_data,
    get_symbol_kline_data,
)
from core.constants import RedisPubMessages, tf_options
from core.redis_config import get_redis_connection


class MatrixCorrelationTracker:
    """
    Tracks correlation statistics using matrices instead of individual objects.
    For N symbols, stores:
    - N values for sum_x, sum_xx (per symbol)
    - N×N sparse matrix for sum_xy (only upper triangle stored)
    - Single count value (shared across all pairs in same timeframe)
    """

    def __init__(self, window_size: int, n_symbols: int):
        self.window_size = window_size
        self.n_symbols = n_symbols
        self.count = 0

        # Per-symbol statistics
        self.sum_x = np.zeros(n_symbols, dtype=np.float64)
        self.sum_xx = np.zeros(n_symbols, dtype=np.float64)

        # Pairwise statistics (sparse matrix for memory efficiency)
        self.sum_xy = dok_matrix((n_symbols, n_symbols), dtype=np.float64)

    def initialize_from_data(self, symbol_data: Dict[int, np.ndarray]):
        """Initialize statistics from historical data."""
        # Find minimum length across all symbols
        min_length = min(len(data) for data in symbol_data.values())
        if min_length == 0:
            return

        # Truncate all data to same length
        for idx, data in symbol_data.items():
            data = data[:min_length]
            self.sum_x[idx] = np.sum(data)
            self.sum_xx[idx] = np.sum(data * data)

        # Compute pairwise sum_xy (only upper triangle)
        for i in symbol_data:
            data_i = symbol_data[i][:min_length]
            for j in symbol_data:
                if j > i:  # Only compute upper triangle
                    data_j = symbol_data[j][:min_length]
                    self.sum_xy[i, j] = np.sum(data_i * data_j)

        self.count = min_length

    def update(self, new_values: np.ndarray, old_values: Optional[np.ndarray] = None):
        """Update statistics with new values (and remove old if at window size)."""
        # Remove old values if window is full
        if self.count >= self.window_size and old_values is not None:
            valid_mask = ~np.isnan(old_values)
            self.sum_x[valid_mask] -= old_values[valid_mask]
            self.sum_xx[valid_mask] -= old_values[valid_mask] ** 2

            # Update sum_xy for pairs
            for i in np.where(valid_mask)[0]:
                for j in np.where(valid_mask)[0]:
                    if j > i:
                        self.sum_xy[i, j] -= old_values[i] * old_values[j]

            self.count -= 1

        # Add new values
        valid_mask = ~np.isnan(new_values)
        self.sum_x[valid_mask] += new_values[valid_mask]
        self.sum_xx[valid_mask] += new_values[valid_mask] ** 2

        # Update sum_xy for pairs
        for i in np.where(valid_mask)[0]:
            for j in np.where(valid_mask)[0]:
                if j > i:
                    self.sum_xy[i, j] += new_values[i] * new_values[j]

        self.count = min(self.count + 1, self.window_size)

    def get_correlation(self, i: int, j: int) -> float:
        """Compute correlation between symbols i and j."""
        if self.count == 0:
            return 0.0

        # Get sum_xy (handle both triangle cases)
        if i < j:
            sum_xy = self.sum_xy[i, j]
        elif i > j:
            sum_xy = self.sum_xy[j, i]
        else:  # i == j
            return 1.0

        # Compute variance components
        var_x = self.count * self.sum_xx[i] - self.sum_x[i] ** 2
        var_y = self.count * self.sum_xx[j] - self.sum_x[j] ** 2

        if var_x <= 0 or var_y <= 0:
            return 0.0

        # Compute correlation
        numerator = self.count * sum_xy - self.sum_x[i] * self.sum_y[j]
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
                idx_i = symbol_indices[i]
                idx_j = symbol_indices[j]
                corr = self.get_correlation(idx_i, idx_j)
                results.append(round(corr, 2))

        return results


class MatrixCorrelationCalculator:
    def __init__(self):
        self.r = get_redis_connection()
        self.print_lock = threading.Lock()
        self.symbols = []
        self.symbol_to_idx = {}  # Map symbol name to index
        self.idx_to_symbol = {}  # Map index to symbol name
        self.hours_options = []
        self.trackers = {}  # {(hours, data_type): MatrixCorrelationTracker}
        self.initialization_complete = False
        self.pending_message_count = 0
        self.pending_messages_lock = threading.Lock()

    def initialize_correlation_trackers(self):
        """Initialize correlation trackers with optimized data fetching."""
        n_symbols = len(self.symbols)
        print(
            f"Initializing correlations for {n_symbols} symbols ({n_symbols*(n_symbols-1)//2:,} pairs)"
        )

        for hours in reversed(self.hours_options):
            print(f"Processing timeframe: {hours}h...")

            start_fetch = time.time()
            symbols_data = get_historical_kline_data(hours=hours, symbols=self.symbols)
            fetch_time = time.time() - start_fetch
            print(f"  Data fetch completed in {fetch_time:.2f}s")

            window_size = hours * 60

            # Process each data type
            for data_type in KLINE_FIELD_MAP.keys():
                # Create tracker for this hours/data_type combination
                tracker = MatrixCorrelationTracker(window_size, n_symbols)

                # Prepare symbol data with indices
                indexed_data = {}
                for symbol in self.symbols:
                    if symbol in symbols_data and data_type in symbols_data[symbol]:
                        idx = self.symbol_to_idx[symbol]
                        data = np.asarray(
                            symbols_data[symbol][data_type], dtype=np.float64
                        )
                        indexed_data[idx] = data

                # Initialize tracker with historical data
                if indexed_data:
                    tracker.initialize_from_data(indexed_data)

                self.trackers[(hours, data_type)] = tracker

            del symbols_data
            gc.collect()
            print(f"  Timeframe {hours}h completed\n")

    def update_correlations(self, hours: int, newest_values: dict, oldest_values: dict):
        """Update correlation trackers with new data."""
        window_size = hours * 60

        for data_type in KLINE_FIELD_MAP.keys():
            tracker = self.trackers.get((hours, data_type))
            if tracker is None:
                print(f"Missing tracker for {hours}h {data_type}")
                continue

            # Prepare value arrays indexed by symbol position
            n_symbols = len(self.symbols)
            new_vals = np.full(n_symbols, np.nan, dtype=np.float64)
            old_vals = np.full(n_symbols, np.nan, dtype=np.float64)

            for symbol in self.symbols:
                idx = self.symbol_to_idx[symbol]

                # New values
                if symbol in newest_values and data_type in newest_values[symbol]:
                    new_vals[idx] = float(newest_values[symbol][data_type])

                # Old values (only if window is full)
                if tracker.count >= window_size:
                    if symbol in oldest_values and data_type in oldest_values[symbol]:
                        old_vals[idx] = float(oldest_values[symbol][data_type])

            # Update tracker
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
        return tracker.get_correlation_matrix_upper(symbol_indices)

    def update_and_cache_incremental_correlations(self):
        """Update correlations and cache to Redis."""
        set_pipeline = self.r.pipeline()

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

            self.update_correlations(
                hours=hours, newest_values=newest_values, oldest_values=oldest_values
            )

            for data_type in KLINE_FIELD_MAP.keys():
                correlation_matrix = self.create_correlation_matrix(hours, data_type)

                set_pipeline.execute_command(
                    "SET",
                    f"correlations:{data_type}:{hours}:binance:perpetual",
                    msgpack.packb(correlation_matrix),
                )

        set_pipeline.execute()

    def add_new_symbol(self, symbol_name: str):
        """Add a new symbol to correlation tracking."""
        if symbol_name in self.symbols:
            print(f"Symbol {symbol_name} already being tracked in correlations")
            return

        print(f"Adding {symbol_name} to correlation tracking")

        # Add symbol to tracking
        new_idx = len(self.symbols)
        self.symbols.append(symbol_name)
        self.symbol_to_idx[symbol_name] = new_idx
        self.idx_to_symbol[new_idx] = symbol_name

        # Extend all trackers
        for (hours, data_type), tracker in self.trackers.items():
            # Extend arrays
            new_sum_x = np.zeros(len(self.symbols), dtype=np.float64)
            new_sum_xx = np.zeros(len(self.symbols), dtype=np.float64)

            # Copy existing values
            new_sum_x[:new_idx] = tracker.sum_x
            new_sum_xx[:new_idx] = tracker.sum_xx

            # Update tracker arrays
            tracker.sum_x = new_sum_x
            tracker.sum_xx = new_sum_xx
            tracker.n_symbols = len(self.symbols)

            # Sparse matrix automatically handles new dimensions

            # Initialize new symbol's statistics from historical data
            symbol_data = get_historical_kline_data(hours=hours, symbols=[symbol_name])
            if symbol_name in symbol_data and data_type in symbol_data[symbol_name]:
                data = np.asarray(symbol_data[symbol_name][data_type], dtype=np.float64)
                data = data[: tracker.count]  # Match existing data length

                if len(data) == tracker.count:
                    tracker.sum_x[new_idx] = np.sum(data)
                    tracker.sum_xx[new_idx] = np.sum(data * data)

                    # Compute sum_xy with all existing symbols
                    for existing_idx in range(new_idx):
                        existing_symbol = self.idx_to_symbol[existing_idx]
                        existing_data = get_historical_kline_data(
                            hours=hours, symbols=[existing_symbol]
                        )
                        if (
                            existing_symbol in existing_data
                            and data_type in existing_data[existing_symbol]
                        ):
                            other_data = np.asarray(
                                existing_data[existing_symbol][data_type][
                                    : tracker.count
                                ],
                                dtype=np.float64,
                            )
                            tracker.sum_xy[existing_idx, new_idx] = np.sum(
                                other_data * data
                            )

    def remove_symbol(self, symbol_name: str):
        """Remove a symbol from correlation tracking."""
        if symbol_name not in self.symbols:
            print(f"Symbol {symbol_name} not being tracked in correlations")
            return

        print(f"Removing symbol {symbol_name} from correlation tracking")

        idx_to_remove = self.symbol_to_idx[symbol_name]

        # Remove from tracking lists
        self.symbols.remove(symbol_name)

        # Rebuild index mappings
        self.symbol_to_idx = {}
        self.idx_to_symbol = {}
        for i, sym in enumerate(self.symbols):
            self.symbol_to_idx[sym] = i
            self.idx_to_symbol[i] = sym

        # Update all trackers
        for tracker in self.trackers.values():
            # Create new arrays without the removed symbol
            mask = np.ones(tracker.n_symbols, dtype=bool)
            mask[idx_to_remove] = False

            tracker.sum_x = tracker.sum_x[mask]
            tracker.sum_xx = tracker.sum_xx[mask]
            tracker.n_symbols = len(self.symbols)

            # Recreate sparse matrix without removed symbol
            new_sum_xy = dok_matrix(
                (tracker.n_symbols, tracker.n_symbols), dtype=np.float64
            )

            old_to_new = {}
            new_idx = 0
            for old_idx in range(len(mask)):
                if mask[old_idx]:
                    old_to_new[old_idx] = new_idx
                    new_idx += 1

            for (i, j), val in tracker.sum_xy.items():
                if i != idx_to_remove and j != idx_to_remove:
                    new_i = old_to_new[i]
                    new_j = old_to_new[j]
                    new_sum_xy[new_i, new_j] = val

            tracker.sum_xy = new_sum_xy

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


# For backwards compatibility
IncrementalCorrelationCalculator = MatrixCorrelationCalculator
