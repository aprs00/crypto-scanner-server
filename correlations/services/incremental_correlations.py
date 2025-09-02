import redis
import msgpack
import time
import threading
from itertools import combinations, product
from concurrent.futures import ThreadPoolExecutor, as_completed

from exchange_connections.constants import KLINE_FIELD_MAP
from exchange_connections.selectors import (
    get_exchange_symbols,
    get_historical_kline_data,
    get_symbol_kline_data,
)
from correlations.formulas.pearson import IncrementalPearsonCorrelation, SumCache
from filters.constants import tf_options
from core.constants import RedisPubMessages
from core.redis_config import get_redis_connection


class IncrementalCorrelationCalculator:
    def __init__(self):
        self.r = get_redis_connection()
        self.print_lock = threading.Lock()
        self.symbols = []
        self.symbol_pairs = []
        self.hours_options = []
        self.correlations = {}
        self.initialization_complete = False
        self.pending_message_count = 0
        self.pending_messages_lock = threading.Lock()
        self.sum_cache = SumCache()

    def chunked_iterable(self, iterable, size):
        """Yield successive chunks from iterable of given size."""
        for i in range(0, len(iterable), size):
            yield iterable[i : i + size]

    def initialize_correlation_objects(self):
        futures = []
        all_pairs = list(combinations(self.symbols, 2))
        pair_batches = list(self.chunked_iterable(all_pairs, 32000))

        completed_futures = 0
        total_futures = 0

        with ThreadPoolExecutor() as executor:
            for hours, pair_batch in product(
                reversed(self.hours_options), pair_batches
            ):
                symbols = list({s for pair in pair_batch for s in pair})

                futures.append(
                    executor.submit(
                        self.process_correlation_batch,
                        hours=hours,
                        batch_symbols=symbols,
                        symbol_pairs=pair_batch,
                    )
                )

            total_futures = len(futures)

            for future in as_completed(futures):
                tf, result = future.result()

                for tuple_key, data_type_dict in result.items():
                    for data_type, hours_dict in data_type_dict.items():
                        self.correlations.setdefault(tuple_key, {}).setdefault(
                            data_type, {}
                        ).update(hours_dict)

                completed_futures += 1

                with self.print_lock:
                    print(
                        f"TF {tf}, Batch done: {completed_futures}/{total_futures} ({(completed_futures/total_futures)*100:.1f}%), {len(result)} correlations"
                    )

    def process_correlation_batch(self, hours, batch_symbols, symbol_pairs):
        symbols_data = get_historical_kline_data(hours=hours, symbols=batch_symbols)
        window_size = hours * 60
        correlation_batch = {}

        for data_type in KLINE_FIELD_MAP.keys():
            for symbol_pair in symbol_pairs:
                symbol_a, symbol_b = symbol_pair

                correlation_batch.setdefault(symbol_pair, {}).setdefault(data_type, {})[
                    hours
                ] = IncrementalPearsonCorrelation(
                    window_size=window_size,
                    x_initial=symbols_data[symbol_a][data_type],
                    y_initial=symbols_data[symbol_b][data_type],
                    sum_cache=self.sum_cache,
                    x_symbol=symbol_a,
                    y_symbol=symbol_b,
                    data_type=data_type,
                    hours=hours,
                )

        return hours, correlation_batch

    def update_correlations(self, hours, newest_values, oldest_values):
        for data_type in KLINE_FIELD_MAP.keys():
            for pair_key in self.symbol_pairs:
                a, b = pair_key
                val_a = newest_values.get(a, {}).get(data_type)
                val_b = newest_values.get(b, {}).get(data_type)

                if val_a is None or val_b is None:
                    continue

                correlation_obj = (
                    self.correlations.get(pair_key, {}).get(data_type, {}).get(hours)
                )

                if correlation_obj is None:
                    print(
                        f"Missing correlation object for {pair_key} {data_type} {hours}h"
                    )
                    continue

                x_old = y_old = None

                if correlation_obj.count >= correlation_obj.window_size:
                    x_old = oldest_values.get(a, {}).get(data_type, 0.0)
                    y_old = oldest_values.get(b, {}).get(data_type, 0.0)

                correlation_obj.add_data_point(float(val_a), float(val_b), x_old, y_old)

    def create_correlation_matrix(self, hours, data_type, is_upper_triangle=True):
        results = []

        for i in range(len(self.symbols)):
            for j in range(i + 1, len(self.symbols)) if is_upper_triangle else range(i):
                corr_obj = (
                    self.correlations.get((self.symbols[i], self.symbols[j]), {})
                    .get(data_type, {})
                    .get(hours)
                )
                val = round(corr_obj.get_correlation(), 2) if corr_obj else 0.0
                results.append(val)

        return results

    def update_and_cache_incremental_correlations(self):
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
                correlation_matrix = self.create_correlation_matrix(
                    hours=hours,
                    data_type=data_type,
                    is_upper_triangle=True,
                )

                set_pipeline.execute_command(
                    "SET",
                    f"correlations:{data_type}:{hours}:binance:perpetual",
                    msgpack.packb(correlation_matrix),
                )

        set_pipeline.execute()

    def process_pending_messages(self):
        """Process any messages that were queued during initialization."""
        with self.pending_messages_lock:
            if self.pending_message_count == 0:
                print("No pending messages to process")
                return

            message_count = self.pending_message_count

            print(f"Processing {message_count} pending messages...")

            start_time = time.time()
            for i in range(message_count):
                self.update_and_cache_incremental_correlations()
            elapsed = time.time() - start_time

            self.pending_message_count = 0

            print(f"Processed {message_count} pending messages in {elapsed:.2f}s")

    def start_pubsub_listener(self):
        retries = 0

        while True:
            try:
                print("Subscribing to Redis channels...")
                pubsub = self.r.pubsub()
                pubsub.subscribe(RedisPubMessages.KLINE_SAVED_TO_DB.value)
                pubsub.get_message()

                for message in pubsub.listen():
                    if (
                        message["type"] == "message"
                        and message["channel"]
                        == RedisPubMessages.KLINE_SAVED_TO_DB.value
                    ):
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
        """
        Calculate and cache large correlations for all combinations of correlation types,
        timeframes, and data types, and listen for Redis pubsub messages with reconnection logic.
        """

        self.hours_options = list(tf_options["correlation"].values())
        self.symbols = get_exchange_symbols()
        self.symbol_pairs = list(combinations(self.symbols, 2))

        self.r.execute_command(
            "SET",
            f"correlations:symbols:binance:perpetual",
            msgpack.packb(self.symbols),
        )

        print("Starting pubsub listener thread...")
        pubsub_thread = threading.Thread(target=self.start_pubsub_listener)
        pubsub_thread.start()

        time.sleep(2)

        print("Starting correlation objects initialization...")
        start_time = time.time()
        self.initialize_correlation_objects()
        elapsed = time.time() - start_time
        print(f"Correlation objects initialization completed in {elapsed:.2f}s")

        self.initialization_complete = True
        print("Initialization complete - now processing any pending messages")

        print("Clearing sum cache to free memory...")
        self.sum_cache.clear()
        print("Sum cache cleared")

        self.process_pending_messages()

        print("Ready for real-time message processing")

        try:
            pubsub_thread.join()
        except KeyboardInterrupt:
            print("Shutting down...")
