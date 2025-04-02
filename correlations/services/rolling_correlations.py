import redis
import msgpack
from itertools import combinations

from correlations.models.pearson import RollingPearsonCorrelation
from correlations.models.spearman import RollingSpearmanCorrelation
from crypto_scanner.utils import convert_timeframe_to_seconds
from crypto_scanner.constants import test_socket_symbols
from crypto_scanner.constants import (
    test_socket_symbols,
    large_correlations_timeframes,
    redis_ts_data_types,
    large_correlation_types,
)

r = redis.Redis(host="redis")


def initialize_correlation_objects():
    """Initialize rolling correlation objects for all combinations of timeframes, data types, and symbol pairs."""
    correlations = {}

    for tf in large_correlations_timeframes:
        for data_type in redis_ts_data_types:
            for symbol_a, symbol_b in combinations(test_socket_symbols, 2):
                window_size = convert_timeframe_to_seconds(tf)
                correlations[("pearson", tf, data_type, symbol_a, symbol_b)] = (
                    RollingPearsonCorrelation(window_size)
                )
                if "spearman" in large_correlation_types:
                    correlations[("spearman", tf, data_type, symbol_a, symbol_b)] = (
                        RollingSpearmanCorrelation(window_size)
                    )

    return correlations


def get_symbol_data(data_type, symbols):
    """Get the latest data for all symbols of the specified data type from Redis."""
    pipeline = r.pipeline()

    for symbol in symbols:
        pipeline.execute_command(f"TS.GET 1s:{data_type}:{symbol}")
    results = pipeline.execute()

    return {symbol: result for symbol, result in zip(symbols, results) if result}


def update_correlations(
    rolling_correlations, correlation_type, timeframe, data_type, symbol_data
):
    """Update rolling correlations with the latest data points."""
    for symbol_a, symbol_b in combinations(test_socket_symbols, 2):
        value_data_a = symbol_data.get(symbol_a)
        value_data_b = symbol_data.get(symbol_b)

        if value_data_a and value_data_b:
            value_a = float(value_data_a[1])
            value_b = float(value_data_b[1])

            rolling_correlations[
                (correlation_type, timeframe, data_type, symbol_a, symbol_b)
            ].add_data_point(value_a, value_b)


def create_correlation_matrix(
    correlation_type,
    timeframe,
    data_type,
    rolling_correlations,
    symbols,
):
    """
    Creates a correlation matrix from rolling correlation objects and converts it to a matrix representation.

    Args:
        timeframe: The timeframe for the correlations
        data_type: The type of data being correlated
        rolling_correlations: Dictionary containing RollingPearsonCorrelation objects
        symbols: List of symbols defining the matrix dimensions (default: test_socket_symbols)

    Returns:
        List of [i, j, value] entries representing the specified triangle of the correlation matrix
    """
    correlations = {
        (symbol_a, symbol_b): rolling_correlations[
            (correlation_type, timeframe, data_type, symbol_a, symbol_b)
        ]
        for symbol_a, symbol_b in combinations(test_socket_symbols, 2)
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


def initialize_rolling_correlations():
    """
    Calculate and cache large correlations for all combinations of correlation types,
    timeframes, and data types.
    """
    pubsub = r.pubsub()
    pubsub.subscribe("test_socket_symbols_stored")

    rolling_correlations = initialize_correlation_objects()

    for message in pubsub.listen():
        if message["type"] == "message":
            print("MESSAGE")
            set_pipeline = r.pipeline()

            for correlation_type in large_correlation_types:
                for timeframe in large_correlations_timeframes:
                    for data_type in redis_ts_data_types:
                        symbol_data = get_symbol_data(data_type, test_socket_symbols)

                        update_correlations(
                            rolling_correlations,
                            correlation_type,
                            timeframe,
                            data_type,
                            symbol_data,
                        )

                        correlation_matrix = create_correlation_matrix(
                            correlation_type,
                            timeframe,
                            data_type,
                            rolling_correlations,
                            test_socket_symbols,
                        )

                        set_pipeline.execute_command(
                            "SET",
                            f"{correlation_type}_correlation_large_{data_type}_{timeframe}",
                            msgpack.packb(correlation_matrix),
                        )

            set_pipeline.execute()
            print("DONE")
