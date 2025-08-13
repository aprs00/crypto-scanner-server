import redis
import msgpack
import time
import threading
from itertools import combinations
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

r = redis.Redis(host="redis")
print_lock = threading.Lock()


def key_for(symbol_a, symbol_b):
    return (symbol_a, symbol_b) if symbol_a < symbol_b else (symbol_b, symbol_a)


def chunked_iterable(iterable, size):
    """Yield successive chunks from iterable of given size."""
    for i in range(0, len(iterable), size):
        yield iterable[i : i + size]


def initialize_correlation_objects(symbols, hours_options):
    correlations = {}
    futures = []
    all_pairs = list(combinations(symbols, 2))
    pair_batches = list(chunked_iterable(all_pairs, 5000))

    completed_futures = 0
    total_futures = 0

    with ThreadPoolExecutor() as executor:
        for hours in hours_options:
            for pair_batch in pair_batches:
                batch_symbols = set()

                for a, b in pair_batch:
                    batch_symbols.add(a)
                    batch_symbols.add(b)

                batch_symbols = list(batch_symbols)

                futures.append(
                    executor.submit(
                        process_correlation_batch,
                        hours=hours,
                        batch_symbols=batch_symbols,
                        symbol_pairs=pair_batch,
                    )
                )

        total_futures = len(futures)

        for future in as_completed(futures):
            tf, result = future.result()

            for tuple_key, data_type_dict in result.items():
                if tuple_key not in correlations:
                    correlations[tuple_key] = {}

                for dt, tf_dict in data_type_dict.items():
                    if dt not in correlations[tuple_key]:
                        correlations[tuple_key][dt] = {}
                    correlations[tuple_key][dt].update(tf_dict)

            completed_futures += 1

            with print_lock:
                print(
                    f"TF {tf} / Batch done: {completed_futures}/{total_futures} ({(completed_futures/total_futures)*100:.1f}%), {len(result)} correlations"
                )

    return correlations


def process_correlation_batch(hours, batch_symbols, symbol_pairs):
    symbols_data = get_historical_kline_data(hours=hours, symbols=batch_symbols)
    window_size = hours * 60
    correlation_batch = {}

    for data_type in KLINE_FIELD_MAP.keys():
        for symbol_a, symbol_b in symbol_pairs:
            key_pair = key_for(
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


def update_correlations(
    incremental_correlations,
    hours,
    symbols,
):
    newest_values = get_symbol_kline_data(symbols=symbols)
    oldest_values = get_symbol_kline_data(symbols=symbols, hours=hours)
    symbol_pairs = [key_for(a, b) for a, b in combinations(symbols, 2)]

    for data_type in KLINE_FIELD_MAP.keys():
        for pair_key in symbol_pairs:
            a, b = pair_key
            value_a = newest_values.get(a, {}).get(data_type)
            value_b = newest_values.get(b, {}).get(data_type)

            if value_a is None or value_b is None:
                continue

            correlation_obj = (
                incremental_correlations.get(pair_key, {}).get(data_type, {}).get(hours)
            )

            if correlation_obj is None:
                print(f"Missing correlation object for {pair_key} {data_type} {hours}h")
                continue

            x_old = y_old = None

            if correlation_obj.count >= correlation_obj.window_size:
                x_old = oldest_values.get(a, {}).get(data_type, 0.0)
                y_old = oldest_values.get(b, {}).get(data_type, 0.0)

            correlation_obj.add_data_point(float(value_a), float(value_b), x_old, y_old)


def create_correlation_matrix(
    hours, data_type, correlations, symbols, is_upper_triangle
):
    pairwise_correlations = {
        key_for(sym_a, sym_b): correlations[key_for(sym_a, sym_b)][data_type][hours]
        for sym_a, sym_b in combinations(symbols, 2)
    }

    def get_corr(i, j):
        key = (symbols[min(i, j)], symbols[max(i, j)])
        return round(pairwise_correlations.get(key, {}).get_correlation() or 0, 2)

    return [
        get_corr(i, j)
        for i in range(len(symbols))
        for j in (range(i + 1, len(symbols)) if is_upper_triangle else range(i))
    ]


def update_and_cache_incremental_correlations(correlations, hours_options, symbols):
    set_pipeline = r.pipeline()

    for hours in hours_options:
        update_correlations(
            incremental_correlations=correlations,
            hours=hours,
            symbols=symbols,
        )

        for data_type in KLINE_FIELD_MAP.keys():
            correlation_matrix = create_correlation_matrix(
                hours=hours,
                data_type=data_type,
                correlations=correlations,
                symbols=symbols,
                is_upper_triangle=True,
            )

            set_pipeline.execute_command(
                "SET",
                f"correlations:{data_type}:{hours}",
                msgpack.packb(correlation_matrix),
            )

    set_pipeline.execute()


def start_pubsub_listener(correlations, hours_options, symbols):
    retries = 0

    while True:
        try:
            print("Subscribing to Redis channels...")
            pubsub = r.pubsub()
            pubsub.subscribe(RedisPubMessages.KLINE_SAVED_TO_DB.value)
            pubsub.get_message()

            for message in pubsub.listen():
                print(f'CORRELATIONS {message["channel"]}')

                if message["channel"] == RedisPubMessages.KLINE_SAVED_TO_DB.value:
                    update_and_cache_incremental_correlations(
                        correlations=correlations,
                        hours_options=hours_options,
                        symbols=symbols,
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


def initialize_incremental_correlations():
    """
    Calculate and cache large correlations for all combinations of correlation types,
    timeframes, and data types, and listen for Redis pubsub messages with reconnection logic.
    """

    hours_options = tf_options["correlation"].values()
    symbols = get_exchange_symbols()

    correlations = initialize_correlation_objects(
        symbols=symbols,
        hours_options=hours_options,
    )

    start_pubsub_listener(
        correlations=correlations,
        hours_options=hours_options,
        symbols=symbols,
    )
