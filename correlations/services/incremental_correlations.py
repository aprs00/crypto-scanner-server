import redis
import msgpack
import time
import threading
from itertools import combinations, product
from concurrent.futures import ThreadPoolExecutor, as_completed

from exchange_connections.constants import KLINE_FIELD_MAP
from exchange_connections.selectors import (
    get_exchange_symbols,
    get_symbol_kline_data,
    get_historical_kline_data,
)
from correlations.formulas.pearson import IncrementalPearsonCorrelation
from filters.constants import tf_options
from core.constants import RedisPubMessages


class IncrementalCorrelationCalculator:
    def __init__(self, redis_host="redis"):
        self.r = redis.Redis(host=redis_host)
        self.print_lock = threading.Lock()
        self.symbols = []
        self.hours_options = []
        self.correlations = {}

    @staticmethod
    def key_for(symbol_a, symbol_b):
        """Generate consistent key for symbol pairs (sorted order)"""
        return tuple(sorted([symbol_a, symbol_b]))

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
                        f"TF {tf} / Batch done: {completed_futures}/{total_futures} ({(completed_futures/total_futures)*100:.1f}%), {len(result)} correlations"
                    )

    def process_correlation_batch(self, hours, batch_symbols, symbol_pairs):
        symbols_data = get_historical_kline_data(hours=hours, symbols=batch_symbols)
        window_size = hours * 60
        correlation_batch = {}

        for data_type in KLINE_FIELD_MAP.keys():
            for symbol_a, symbol_b in symbol_pairs:
                key_pair = IncrementalCorrelationCalculator.key_for(
                    symbol_a=symbol_a,
                    symbol_b=symbol_b,
                )

                correlation_batch.setdefault(key_pair, {}).setdefault(data_type, {})[
                    hours
                ] = IncrementalPearsonCorrelation(
                    window_size=window_size,
                    x_initial=symbols_data[symbol_a][data_type],
                    y_initial=symbols_data[symbol_b][data_type],
                )

        return hours, correlation_batch

    def update_correlations(self, hours):
        newest_values = get_symbol_kline_data(
            symbols=self.symbols, exchange="binance", contract_type="perpetual"
        )
        oldest_values = get_symbol_kline_data(
            symbols=self.symbols,
            hours=hours,
            exchange="binance",
            contract_type="perpetual",
        )
        symbol_pairs = [self.key_for(a, b) for a, b in combinations(self.symbols, 2)]

        for data_type in KLINE_FIELD_MAP.keys():
            for pair_key in symbol_pairs:
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

    def create_correlation_matrix(self, hours, data_type, is_upper_triangle):
        pairwise_correlations = {
            self.key_for(sym_a, sym_b): self.correlations[self.key_for(sym_a, sym_b)][
                data_type
            ][hours]
            for sym_a, sym_b in combinations(self.symbols, 2)
        }

        def get_corr(i, j):
            key = (self.symbols[min(i, j)], self.symbols[max(i, j)])
            return round(pairwise_correlations.get(key, {}).get_correlation() or 0, 2)

        return [
            get_corr(i, j)
            for i in range(len(self.symbols))
            for j in (
                range(i + 1, len(self.symbols)) if is_upper_triangle else range(i)
            )
        ]

    def update_and_cache_incremental_correlations(self):
        set_pipeline = self.r.pipeline()

        for hours in self.hours_options:
            self.update_correlations(hours=hours)

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

    def start_pubsub_listener(self):
        retries = 0

        while True:
            try:
                print("Subscribing to Redis channels...")
                pubsub = self.r.pubsub()
                pubsub.subscribe(RedisPubMessages.KLINE_SAVED_TO_DB.value)
                pubsub.get_message()

                for message in pubsub.listen():
                    print(f'CORRELATIONS {message["channel"]}')

                    if message["channel"] == RedisPubMessages.KLINE_SAVED_TO_DB.value:
                        self.update_and_cache_incremental_correlations()

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

        self.r.execute_command(
            "SET",
            f"correlations:symbols:binance:perpetual",
            msgpack.packb(self.symbols),
        )

        self.initialize_correlation_objects()

        self.start_pubsub_listener()
