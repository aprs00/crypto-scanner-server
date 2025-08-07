import redis
import msgpack
import time
import threading
from itertools import combinations
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
from typing import Dict, List, Tuple

from exchange_connections.selectors import get_exchange_symbols, get_latest_kline_values
from correlations.formulas.pearson import IncrementalPearsonCorrelation
from correlations.selectors.correlations import (
    get_tickers_data,
    get_oldest_values_efficient,
)
from filters.constants import tf_options
from exchange_connections.constants import correlations_data_types
from core.constants import RedisPubMessages

r = redis.Redis(host="redis")
print_lock = threading.Lock()

timeframe_oldest_values_cache: Dict[Tuple[int, str], Dict[str, Tuple[float, int]]] = (
    defaultdict(dict)
)


def generate_tuple(symbol_a, symbol_b):
    return (
        symbol_a,
        symbol_b,
    )


def chunked_iterable(iterable, size):
    """Yield successive chunks from iterable of given size."""
    for i in range(0, len(iterable), size):
        yield iterable[i : i + size]


def get_oldest_values_for_timeframe(
    tf: int, data_type: str, symbols: List[str]
) -> Dict[str, Tuple[float, int]]:
    """
    Fetch oldest values for all symbols in a timeframe at once.
    Returns dict with symbol -> (oldest_value, oldest_timestamp)
    """
    cache_key = (tf, data_type)

    if cache_key in timeframe_oldest_values_cache and all(
        symbol in timeframe_oldest_values_cache[cache_key] for symbol in symbols
    ):
        return {
            symbol: timeframe_oldest_values_cache[cache_key][symbol]
            for symbol in symbols
        }

    try:
        oldest_data = get_oldest_values_efficient(
            duration_hours=tf, data_type=data_type, symbols=symbols
        )

        for symbol in symbols:
            if symbol in oldest_data:
                timeframe_oldest_values_cache[cache_key][symbol] = oldest_data[symbol]

        return {
            symbol: timeframe_oldest_values_cache[cache_key].get(symbol, (0.0, 0))
            for symbol in symbols
        }

    except Exception as e:
        print(f"Error fetching oldest values for tf {tf}, data_type {data_type}: {e}")
        return {symbol: (0.0, 0) for symbol in symbols}


def initialize_correlation_objects(symbols, timeframes):
    correlations = {}
    futures = []
    all_pairs = list(combinations(symbols, 2))
    pair_batches = list(chunked_iterable(all_pairs, 5000))

    completed_futures = 0
    total_futures = 0

    with ThreadPoolExecutor() as executor:
        for tf in timeframes:
            for data_type in correlations_data_types:
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
            symbol_a=symbol_a,
            symbol_b=symbol_b,
        )

        if dict_tuple not in local_correlations:
            local_correlations[dict_tuple] = {}

        if data_type not in local_correlations[dict_tuple]:
            local_correlations[dict_tuple][data_type] = {}

        local_correlations[dict_tuple][data_type][tf] = IncrementalPearsonCorrelation(
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

    data_type_db_column_mapper = {
        "price": "close",
        "volume": "base_volume",
        "trades": "number_of_trades",
    }

    for kline in latest_klines:
        for data_type, db_column in data_type_db_column_mapper.items():
            result[kline.symbol][data_type] = float(getattr(kline, db_column))

    return result


def update_correlations(
    incremental_correlations,
    tf,
    symbol_data,
    symbols,
):
    """Update incremental correlations with the latest data points."""

    oldest_values_cache = {}

    for data_type in correlations_data_types:
        oldest_values_cache[data_type] = get_oldest_values_for_timeframe(
            tf, data_type, symbols
        )

        for symbol_a, symbol_b in combinations(symbols, 2):
            value_a = symbol_data.get(symbol_a, {}).get(data_type)
            value_b = symbol_data.get(symbol_b, {}).get(data_type)

            if value_a is None or value_b is None:
                continue

            value_a = float(value_a)
            value_b = float(value_b)

            dict_tuple = generate_tuple(
                symbol_a=symbol_a,
                symbol_b=symbol_b,
            )

            if (
                dict_tuple in incremental_correlations
                and data_type in incremental_correlations[dict_tuple]
                and tf in incremental_correlations[dict_tuple][data_type]
            ):
                correlation_obj = incremental_correlations[dict_tuple][data_type][tf]
                x_old = None
                y_old = None

                if correlation_obj.count >= correlation_obj.window_size:
                    oldest_values = oldest_values_cache[data_type]
                    if symbol_a in oldest_values and symbol_b in oldest_values:
                        x_old = oldest_values[symbol_a][0]
                        y_old = oldest_values[symbol_b][0]

                correlation_obj.add_data_point(value_a, value_b, x_old, y_old)
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
                symbol_a=symbol_a,
                symbol_b=symbol_b,
            )
        ][data_type][tf]
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


def clear_timeframe_cache():
    """Clear the oldest values cache - call this periodically to prevent memory buildup"""
    global timeframe_oldest_values_cache
    timeframe_oldest_values_cache.clear()


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

        for data_type in correlations_data_types:
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

    if int(time.time()) % 3600 == 0:  # Every hour
        clear_timeframe_cache()


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

    timeframes = tf_options["correlation"].values()
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
