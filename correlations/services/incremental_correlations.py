import redis
import msgpack
import time
import threading
import gc
import numpy as np
from typing import Dict, List, Optional

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
        min_length = min(len(data) for data in symbol_data.values())
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


class MatrixCorrelationCalculator:
    def __init__(self):
        self.r = get_redis_connection()
        self.print_lock = threading.Lock()
        self.correlation_lock = threading.RLock()
        self.symbols = []
        self.symbol_to_idx = {}
        self.idx_to_symbol = {}
        self.hours_options = []
        self.trackers = {}  # {(hours, data_type): MatrixCorrelationTracker}
        self.initialization_complete = False
        self.pending_message_count = 0
        self.pending_messages_lock = threading.Lock()

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
        # NOTE: This method should only be called from within correlation_lock context
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
        return tracker.get_correlation_matrix_upper(symbol_indices)

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

    def add_new_symbol(self, symbol_name: str):
        """Add a new symbol to correlation tracking."""
        with self.correlation_lock:
            if symbol_name in self.symbols:
                print(f"Symbol {symbol_name} already being tracked in correlations")
                return

            print(f"Adding {symbol_name} to correlation tracking")

            new_idx = len(self.symbols)
            self.symbols = get_exchange_symbols()
            self.symbol_to_idx[symbol_name] = new_idx
            self.idx_to_symbol[new_idx] = symbol_name

            max_hours = max(self.hours_options)
            symbol_data = get_historical_kline_data(
                hours=max_hours, symbols=[symbol_name]
            )

            for (hours, data_type), tracker in self.trackers.items():
                new_sum_x = np.zeros(len(self.symbols), dtype=np.float64)
                new_sum_xx = np.zeros(len(self.symbols), dtype=np.float64)

                new_sum_x[:new_idx] = tracker.sum_x
                new_sum_xx[:new_idx] = tracker.sum_xx

                tracker.sum_x = new_sum_x
                tracker.sum_xx = new_sum_xx

                new_sum_xy = np.zeros(
                    (len(self.symbols), len(self.symbols)), dtype=np.float64
                )
                new_sum_xy[:new_idx, :new_idx] = tracker.sum_xy
                tracker.sum_xy = new_sum_xy

                tracker.n_symbols = len(self.symbols)
                tracker.upper_i, tracker.upper_j = np.triu_indices(
                    tracker.n_symbols, k=1
                )

                if symbol_name in symbol_data and data_type in symbol_data[symbol_name]:
                    data = np.asarray(
                        symbol_data[symbol_name][data_type], dtype=np.float64
                    )

                    window_size = hours * 60
                    if len(data) > window_size:
                        data = data[-window_size:]

                    if tracker.count > 0 and len(data) >= tracker.count:
                        data = data[-tracker.count :]

                        if len(data) == tracker.count:
                            tracker.sum_x[new_idx] = np.sum(data)
                            tracker.sum_xx[new_idx] = np.sum(data * data)

                            tracker.sum_xy[new_idx, new_idx] = np.sum(data * data)

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
                                            -tracker.count :
                                        ],
                                        dtype=np.float64,
                                    )

                                    if len(other_data) == len(data):
                                        cross_product = np.sum(other_data * data)
                                        tracker.sum_xy[existing_idx, new_idx] = (
                                            cross_product
                                        )
                                        tracker.sum_xy[new_idx, existing_idx] = (
                                            cross_product
                                        )

            print(f"Successfully added {symbol_name} to {len(self.trackers)} trackers")

    def remove_symbol(self, symbol_name: str):
        """Remove a symbol from correlation tracking."""
        with self.correlation_lock:
            if symbol_name not in self.symbols:
                print(f"Symbol {symbol_name} not being tracked in correlations")
                return

            print(f"Removing symbol {symbol_name} from correlation tracking")

            idx_to_remove = self.symbol_to_idx[symbol_name]

            self.symbols.remove(symbol_name)

            self.symbol_to_idx = {sym: i for i, sym in enumerate(self.symbols)}
            self.idx_to_symbol = {i: sym for sym, i in self.symbol_to_idx.items()}

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

        # self.process_pending_messages() // TODO: Uncomment
        print("Ready for real-time message processing")

        try:
            pubsub_thread.join()
        except KeyboardInterrupt:
            print("Shutting down...")
