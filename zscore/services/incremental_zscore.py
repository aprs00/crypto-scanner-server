import redis
import numpy as np
import msgpack
from collections import deque
from datetime import timedelta
from django.utils import timezone

from filters.constants import tf_options
from zscore.constants import zscore_data_types
from exchange_connections.selectors import get_exchange_symbols, get_latest_kline_values
from exchange_connections.models import Kline1m
from core.constants import RedisPubMessages

r = redis.Redis(host="redis")


class IncrementalZScore:
    """
    Calculates Z-score over a incremental window of data points.
    """

    def __init__(self, window_size, initial_data):
        self.window_size = window_size

        if initial_data:
            data = (
                initial_data[-window_size:]
                if len(initial_data) > window_size
                else initial_data
            )
            self.values = deque(data, maxlen=window_size)
            self.count = len(self.values)
            self.sum = sum(self.values)
            self.sum_squared = sum(x * x for x in self.values)
        else:
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


def initialize_z_score_objects(symbols, timeframes, zscore_data_types):
    """
    Initialize Z-score objects with historical data from the database.
    Each timeframe gets its own window of historical data.
    """
    z_score_dict = {}

    for tf in timeframes:
        start_time = timezone.now() - timedelta(hours=tf)

        historical_klines = (
            Kline1m.objects.filter(symbol__in=symbols, start_time__gte=start_time)
            .values("symbol", "start_time", "close", "base_volume", "number_of_trades")
            .order_by("symbol", "start_time")
        )

        data_by_symbol = {
            symbol: {data_type: [] for data_type in zscore_data_types}
            for symbol in symbols
        }

        for kline in historical_klines:
            data_by_symbol[kline["symbol"]]["price"].append(float(kline["close"]))
            data_by_symbol[kline["symbol"]]["volume"].append(
                float(kline["base_volume"])
            )
            data_by_symbol[kline["symbol"]]["trades"].append(
                float(kline["number_of_trades"])
            )

        for data_type in zscore_data_types:
            for symbol in symbols:
                z_score_dict[(tf, data_type, symbol)] = IncrementalZScore(
                    tf, data_by_symbol[symbol][data_type]
                )

    return z_score_dict


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
        result[kline.symbol]["volume"] = float(kline.base_volume)
        result[kline.symbol]["trades"] = float(kline.number_of_trades)

    return result


def update_z_scores(incremental_zscores, tf, symbol_data):
    """Update incremental Z-scores with the latest data points for all data types."""
    for symbol, data_dict in symbol_data.items():
        for data_type, value in data_dict.items():
            dict_tuple = (tf, data_type, symbol)

            if tuple in incremental_zscores:
                incremental_zscores[dict_tuple].add_data_point(float(value))
            else:
                print(f"[Warning] Correlation object not found for {dict_tuple}")


def create_z_score_matrix(tf, data_type, incremental_zscores, symbols):
    """
    Creates a Z-score matrix for all symbols.

    Returns:
        Dictionary with symbols as keys and their Z-scores as values
    """
    return {
        symbol: round(incremental_zscores[(tf, data_type, symbol)].get_z_score(), 2)
        for symbol in symbols
    }


def initialize_incremental_zscore():
    """
    Calculate and cache Z-scores for all combinations of timeframes, data types, and symbols.
    """
    symbols = get_exchange_symbols()
    timeframes = tf_options.values()
    incremental_zscores = initialize_z_score_objects(
        symbols, timeframes, zscore_data_types
    )

    pubsub = r.pubsub()
    pubsub.subscribe(RedisPubMessages.KLINE_SAVED_TO_DB.value)

    for message in pubsub.listen():
        if message["channel"] == RedisPubMessages.KLINE_SAVED_TO_DB.value:
            set_pipeline = r.pipeline()
            symbol_data = get_symbol_data(symbols)

            for tf in timeframes:
                z_scores_by_tf = {}

                update_z_scores(incremental_zscores, tf, symbol_data)

                for data_type in zscore_data_types:
                    z_score_matrix = create_z_score_matrix(
                        tf, data_type, incremental_zscores, symbols
                    )

                    for symbol, z_score in z_score_matrix.items():
                        z_scores_by_tf.setdefault(symbol, {})[data_type] = z_score

                r.execute_command(
                    "SET",
                    f"zscore:{tf}",
                    msgpack.packb(z_scores_by_tf),
                )

            set_pipeline.execute()
