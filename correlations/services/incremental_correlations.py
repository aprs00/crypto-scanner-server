import redis
import msgpack
import time
import threading
from itertools import combinations
from concurrent.futures import ThreadPoolExecutor, as_completed

from exchange_connections.selectors import get_exchange_symbols, get_latest_kline_values
from correlations.models.pearson import IncrementalPearsonCorrelation
from correlations.selectors.correlations import (
    get_tickers_data,
)
from filters.constants import tf_options
from exchange_connections.constants import redis_time_series_data_types
from core.constants import RedisPubMessages

r = redis.Redis(host="redis")
print_lock = threading.Lock()


def generate_tuple(tf, data_type, symbol_a, symbol_b):
    return (
        tf,
        data_type,
        symbol_a,
        symbol_b,
    )


def chunked_iterable(iterable, size):
    """Yield successive chunks from iterable of given size."""
    for i in range(0, len(iterable), size):
        yield iterable[i : i + size]


def initialize_correlation_objects(symbols, timeframes):
    correlations = {}
    futures = []
    all_pairs = list(combinations(symbols, 2))
    pair_batches = list(chunked_iterable(all_pairs, 5000))

    completed_futures = 0
    total_futures = 0

    with ThreadPoolExecutor() as executor:
        for tf in timeframes:
            for data_type in redis_time_series_data_types:
                for pair_batch in pair_batches:
                    batch_symbols = set()
                    for a, b in pair_batch:
                        batch_symbols.add(a)
                        batch_symbols.add(b)
                    batch_symbols = list(batch_symbols)

                    futures.append(
                        executor.submit(
                            process_correlation_batch,
                            tf=tf,
                            data_type=data_type,
                            symbols=batch_symbols,
                            symbol_pairs=pair_batch,
                        )
                    )

        total_futures = len(futures)

        for future in as_completed(futures):
            tf, data_type, result = future.result()
            correlations.update(result)

            completed_futures += 1

            with print_lock:
                print(
                    f"TF {tf} / Data Type {data_type} batch done: {completed_futures}/{total_futures} ({(completed_futures/total_futures)*100:.1f}%), {len(result)} correlations"
                )

    return correlations


def process_correlation_batch(tf, data_type, symbols, symbol_pairs):
    symbol_data = get_tickers_data(
        duration_hours=tf, data_type=data_type, symbols=symbols
    )
    window_size = tf * 60

    local_correlations = {}

    for symbol_a, symbol_b in symbol_pairs:
        dict_tuple = generate_tuple(
            tf=tf,
            data_type=data_type,
            symbol_a=symbol_a,
            symbol_b=symbol_b,
        )

        local_correlations[dict_tuple] = IncrementalPearsonCorrelation(
            window_size=window_size,
            x_initial=symbol_data[symbol_a],
            y_initial=symbol_data[symbol_b],
        )

    return tf, data_type, local_correlations


def get_symbol_data(symbols):
    """
    Get the latest data for all symbols and all data types from the database.
    Returns:
        {symbol: {data_type: value, ...}, ...}
    """
    result = {symbol: {} for symbol in symbols}
    latest_klines = get_latest_kline_values()

    for kline in latest_klines:
        result[kline.symbol]["price"] = float(kline.close)
        # result[kline.symbol]["volume"] = float(kline.base_volume)
        # result[kline.symbol]["trades"] = float(kline.number_of_trades)

    return result


def update_correlations(
    incremental_correlations,
    tf,
    symbol_data,
    symbols,
):
    """Update incremental correlations with the latest data points."""
    for data_type in redis_time_series_data_types:
        for symbol_a, symbol_b in combinations(symbols, 2):
            value_a = float(symbol_data.get(symbol_a, {}).get(data_type, 0))
            value_b = float(symbol_data.get(symbol_b, {}).get(data_type, 0))

            dict_tuple = generate_tuple(
                tf=tf,
                data_type=data_type,
                symbol_a=symbol_a,
                symbol_b=symbol_b,
            )

            if dict_tuple in incremental_correlations:
                incremental_correlations[dict_tuple].add_data_point(value_a, value_b)
            else:
                print(f"[Warning] Correlation object not found for {dict_tuple}")


def create_correlation_list(
    tf,
    data_type,
    incremental_correlations,
    symbols,
    is_matrix_upper_triangle,
):
    correlations = {
        (symbol_a, symbol_b): incremental_correlations[
            generate_tuple(
                tf=tf,
                data_type=data_type,
                symbol_a=symbol_a,
                symbol_b=symbol_b,
            )
        ]
        for symbol_a, symbol_b in combinations(symbols, 2)
    }

    return [
        round(
            correlations[(symbols[min(i, j)], symbols[max(i, j)])].get_correlation()
            or 0,
            2,
        )
        for i in range(len(symbols))
        for j in (range(i + 1, len(symbols)) if is_matrix_upper_triangle else range(i))
    ]


def update_and_cache_incremental_correlations(correlations, timeframes, symbols):
    set_pipeline = r.pipeline()

    symbol_data = get_symbol_data(symbols=symbols)

    for tf in timeframes:
        update_correlations(
            incremental_correlations=correlations,
            tf=tf,
            symbol_data=symbol_data,
            symbols=symbols,
        )

        for data_type in redis_time_series_data_types:
            correlation_matrix = create_correlation_list(
                tf=tf,
                data_type=data_type,
                incremental_correlations=correlations,
                symbols=symbols,
                is_matrix_upper_triangle=True,
            )

            set_pipeline.execute_command(
                "SET",
                f"correlations:{data_type}:{tf}",
                msgpack.packb(correlation_matrix),
            )

    set_pipeline.execute()


def start_pubsub_listener(correlations, timeframes, symbols):
    retries = 0

    while True:
        try:
            print("Subscribing to Redis channels...")
            pubsub = r.pubsub()
            pubsub.subscribe(RedisPubMessages.KLINE_SAVED_TO_DB.value)

            for message in pubsub.listen():
                print(f'CORRELATIONS {message["channel"]}')

                if message["channel"] == RedisPubMessages.KLINE_SAVED_TO_DB.value:
                    update_and_cache_incremental_correlations(
                        correlations=correlations,
                        timeframes=timeframes,
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

    timeframes = tf_options.values()
    symbols = get_exchange_symbols()

    correlations = initialize_correlation_objects(
        symbols=symbols,
        timeframes=timeframes,
    )

    start_pubsub_listener(
        correlations=correlations,
        timeframes=timeframes,
        symbols=symbols,
    )
