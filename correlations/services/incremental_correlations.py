import redis
import msgpack
from itertools import combinations
import concurrent.futures

from exchange_connections.models import BinanceSpotKline5m
from correlations.models.pearson import IncrementalPearsonCorrelation
from correlations.models.spearman import IncrementalSpearmanCorrelation
from correlations.selectors.correlations import (
    get_tickers_data,
    extract_time_series_data,
)
from utils.time import convert_timeframe_to_seconds
from crypto_scanner.constants import (
    test_socket_symbols,
    large_correlations_timeframes,
    redis_ts_data_types,
    large_correlation_types,
    stats_select_options_all,
    tickers,
)

r = redis.Redis(host="redis")

last_end_time_tickers = {symbol: None for symbol in tickers}


def initialize_correlation_objects(symbols, data_origin, timeframes):
    """Initialize incremental correlation objects for all combinations of timeframes, data types, and symbol pairs."""
    correlations = {}

    incremental_correlation_models = {
        "pearson": IncrementalPearsonCorrelation,
        "spearman": IncrementalSpearmanCorrelation,
    }

    for tf in timeframes:
        for data_type in redis_ts_data_types:
            match data_origin:
                case "DB":
                    symbol_data = get_tickers_data(
                        duration_hours=tf, data_type=data_type, symbols=tickers
                    )
                    window_size = tf * 12
                case "REDIS":
                    symbol_data = extract_time_series_data(tf, data_type, symbols)
                    window_size = tf

            for symbol_a, symbol_b in combinations(symbols, 2):
                for correlation_type in large_correlation_types:
                    dict_tuple = (
                        correlation_type,
                        tf,
                        data_origin,
                        data_type,
                        symbol_a,
                        symbol_b,
                    )

                    symbol_a_data = symbol_data[symbol_a]
                    symbol_b_data = symbol_data[symbol_b]

                    correlations[dict_tuple] = incremental_correlation_models[
                        correlation_type
                    ](
                        window_size=window_size,
                        x_initial=symbol_a_data,
                        y_initial=symbol_b_data,
                    )

    return correlations


def get_symbol_data(data_type, symbols, data_origin):
    """
    Get the latest data for symbols of the specified data type from Redis or Database.

    Args:
        data_type: The type of data to retrieve ("price", "volume", "trades")
        symbols: List of symbols to get data for
        data_origin: Source of data ("REDIS" or "DB")

    Returns:
        Dict mapping symbols to their data results
    """

    match data_origin:
        case "REDIS":
            pipeline = r.pipeline()

            for symbol in symbols:
                pipeline.execute_command(f"TS.GET 1s:{data_type}:{symbol}")
            results = pipeline.execute()

            return {
                symbol: result[1] for symbol, result in zip(symbols, results) if result
            }

        case "DB":
            result = {}

            for symbol in symbols:
                latest_kline = (
                    BinanceSpotKline5m.objects.filter(ticker=symbol)
                    .order_by("-end_time")
                    .first()
                )

                if last_end_time_tickers[symbol] != latest_kline.end_time:
                    last_end_time_tickers[symbol] = latest_kline.end_time

                    match data_type:
                        case "price":
                            value = float(latest_kline.close)
                        case "volume":
                            value = float(latest_kline.base_volume)
                        case "trades":
                            value = float(latest_kline.number_of_trades)

                    result[symbol] = value

            return result


def update_correlations(
    incremental_correlations,
    correlation_type,
    data_origin,
    timeframe,
    data_type,
    symbol_data,
    symbols,
):
    """Update incremental correlations with the latest data points."""
    for symbol_a, symbol_b in combinations(symbols, 2):
        value_data_a = symbol_data.get(symbol_a)
        value_data_b = symbol_data.get(symbol_b)

        if value_data_a and value_data_b:
            incremental_correlations[
                (
                    correlation_type,
                    timeframe,
                    data_origin,
                    data_type,
                    symbol_a,
                    symbol_b,
                )
            ].add_data_point(float(value_data_a), float(value_data_b))


def create_correlation_matrix(
    correlation_type,
    data_origin,
    timeframe,
    data_type,
    incremental_correlations,
    symbols,
):
    """
    Creates a correlation matrix from incremental correlation objects and converts it to a matrix representation.

    Args:
        timeframe: The timeframe for the correlations
        data_type: The type of data being correlated
        incremental_correlations: Dictionary containing IncrementalPearsonCorrelation objects
        symbols: List of symbols defining the matrix dimensions

    Returns:
        List of [i, j, value] entries representing the specified triangle of the correlation matrix
    """
    correlations = {
        (symbol_a, symbol_b): incremental_correlations[
            (correlation_type, timeframe, data_origin, data_type, symbol_a, symbol_b)
        ]
        for symbol_a, symbol_b in combinations(symbols, 2)
    }
    is_matrix_upper_triangle = correlation_type == "pearson"

    return [
        [
            i,
            j,
            round(
                correlations[(symbols[min(i, j)], symbols[max(i, j)])].get_correlation()
                or 0,
                2,
            ),
        ]
        for i in range(len(symbols))
        for j in (range(i + 1, len(symbols)) if is_matrix_upper_triangle else range(i))
    ]


def initialize_incremental_correlations():
    """
    Calculate and cache large correlations for all combinations of correlation types,
    timeframes, and data types.
    """

    redis_timeframes = list(
        map(convert_timeframe_to_seconds, large_correlations_timeframes)
    )
    db_timeframes = stats_select_options_all.values()

    with concurrent.futures.ThreadPoolExecutor() as executor:
        redis_correlations = executor.submit(
            initialize_correlation_objects,
            test_socket_symbols,
            "REDIS",
            redis_timeframes,
        ).result()

        db_correlations = executor.submit(
            initialize_correlation_objects, tickers, "DB", db_timeframes
        ).result()

    pubsub = r.pubsub()
    pubsub.subscribe("test_socket_symbols_stored", "klines_fetched")

    for message in pubsub.listen():
        if message["type"] == "message":
            print("MESSAGE", message["channel"])
            if message["channel"] == b"test_socket_symbols_stored":
                handle_redis_pubsub_message(
                    data_origin="REDIS",
                    correlations=redis_correlations,
                    timeframes=redis_timeframes,
                    symbols=test_socket_symbols,
                )

            elif message["channel"] == b"klines_fetched":
                handle_redis_pubsub_message(
                    data_origin="DB",
                    correlations=db_correlations,
                    timeframes=db_timeframes,
                    symbols=tickers,
                )

        print("DONE")


def handle_redis_pubsub_message(data_origin, correlations, timeframes, symbols):
    set_pipeline = r.pipeline()

    for data_type in redis_ts_data_types:
        latest_data = get_symbol_data(
            data_type=data_type, symbols=symbols, data_origin=data_origin
        )

        if not latest_data:
            continue

        for timeframe in timeframes:
            for correlation_type in large_correlation_types:
                update_correlations(
                    incremental_correlations=correlations,
                    correlation_type=correlation_type,
                    data_origin=data_origin,
                    timeframe=timeframe,
                    data_type=data_type,
                    symbol_data=latest_data,
                    symbols=symbols,
                )

                correlation_matrix = create_correlation_matrix(
                    correlation_type=correlation_type,
                    data_origin=data_origin,
                    timeframe=timeframe,
                    data_type=data_type,
                    incremental_correlations=correlations,
                    symbols=symbols,
                )

                set_pipeline.execute_command(
                    "SET",
                    f"{correlation_type}:{data_type}:{timeframe}:{data_origin}",
                    msgpack.packb(correlation_matrix),
                )

    set_pipeline.execute()
