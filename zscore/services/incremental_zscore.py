import redis
import numpy as np
import msgpack
from collections import deque

from crypto_scanner.constants import (
    test_socket_symbols,
    large_correlations_timeframes,
    redis_ts_data_types,
)
from crypto_scanner.utils import convert_timeframe_to_seconds

r = redis.Redis(host="redis")


class IncrementalZScore:
    """
    Calculates Z-score over a incremental window of data points.
    Z-score = (current_value - mean) / standard_deviation
    """

    def __init__(self, window_size):
        self.window_size = window_size
        self.values = deque(maxlen=window_size)

        self.sum = 0
        self.sum_squared = 0
        self.count = 0

    def add_data_point(self, value):
        if self.count == self.window_size:
            old_value = self.values[0]

            self.sum -= old_value
            self.sum_squared -= old_value * old_value
            self.count -= 1

        self.values.append(value)

        self.sum += value
        self.sum_squared += value * value
        self.count = min(self.count + 1, self.window_size)

    def get_z_score(self, current_value=None):
        """
        Calculate the Z-score for the current value.
        If current_value is not provided, uses the most recent value in the window.
        """
        if current_value is None and self.values:
            current_value = self.values[-1]
        elif current_value is None:
            return 0

        mean = self.sum / self.count

        variance = (self.sum_squared / self.count) - (mean * mean)

        if variance <= 0:
            return 0

        std_dev = np.sqrt(variance)

        return (current_value - mean) / std_dev


def initialize_z_score_objects(symbols, timeframes, data_types):
    """Initialize incremental Z-score objects for all combinations of timeframes, data types, and symbols."""
    return {
        (tf, data_type, symbol): IncrementalZScore(convert_timeframe_to_seconds(tf))
        for tf in timeframes
        for data_type in data_types
        for symbol in symbols
    }


def get_symbol_data(data_type, symbols):
    """Get the latest data for all symbols of the specified data type from Redis."""
    pipeline = r.pipeline()

    for symbol in symbols:
        pipeline.execute_command(f"TS.GET 1s:{data_type}:{symbol}")
    results = pipeline.execute()

    return {symbol: result for symbol, result in zip(symbols, results) if result}


def update_z_scores(incremental_zscores, timeframe, data_type, symbol_data):
    """Update incremental Z-scores with the latest data points."""
    for symbol, value_data in symbol_data.items():
        if value_data:
            value = float(value_data[1])
            incremental_zscores[(timeframe, data_type, symbol)].add_data_point(value)


def create_z_score_matrix(timeframe, data_type, incremental_zscores, symbols):
    """
    Creates a Z-score matrix for all symbols.

    Returns:
        Dictionary with symbols as keys and their Z-scores as values
    """
    return {
        symbol: round(
            incremental_zscores[(timeframe, data_type, symbol)].get_z_score(), 2
        )
        for symbol in symbols
    }


def initialize_incremental_zscore():
    """
    Calculate and cache Z-scores for all combinations of timeframes, data types, and symbols.
    """
    incremental_zscores = initialize_z_score_objects(
        test_socket_symbols, large_correlations_timeframes, redis_ts_data_types
    )

    pubsub = r.pubsub()
    pubsub.subscribe("test_socket_symbols_stored")

    for message in pubsub.listen():
        if message["type"] == "message":
            set_pipeline = r.pipeline()

            for timeframe in large_correlations_timeframes:
                z_scores_by_tf = {}

                for data_type in redis_ts_data_types:
                    symbol_data = get_symbol_data(data_type, test_socket_symbols)

                    update_z_scores(
                        incremental_zscores, timeframe, data_type, symbol_data
                    )

                    z_score_matrix = create_z_score_matrix(
                        timeframe, data_type, incremental_zscores, test_socket_symbols
                    )

                    for symbol, z_score in z_score_matrix.items():
                        z_scores_by_tf.setdefault(symbol, {})[data_type] = z_score

                r.execute_command(
                    "SET",
                    f"z_score_matrix_large_{timeframe}",
                    msgpack.packb(z_scores_by_tf),
                )

            set_pipeline.execute()

            # TODO add storing to database for z score history
