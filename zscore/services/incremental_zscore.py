import redis
import numpy as np
import msgpack

from filters.constants import tf_options
from zscore.constants import zscore_data_types
from zscore.selectors.zscore import get_oldest_kline_for_timeframe
from exchange_connections.selectors import get_exchange_symbols
from correlations.selectors.correlations import (
    get_symbol_kline_data,
    get_historical_kline_data,
)
from core.constants import RedisPubMessages

r = redis.Redis(host="redis")


class IncrementalZScore:
    """
    Calculates Z-score over an incremental window of data points.
    """

    def __init__(self, window_size):
        self.window_size = window_size
        self.sum = 0
        self.sum_squared = 0
        self.count = 0
        self.current_value = None

    def initialize_from_data(self, data):
        if data:
            self.count = len(data)
            self.sum = sum(data)
            self.sum_squared = sum(x * x for x in data)
            if data:
                self.current_value = data[-1]

    def remove_data_point(self, value):
        if self.count > 0:
            self.sum -= value
            self.sum_squared -= value * value
            self.count -= 1

    def add_data_point(self, value):
        self.sum += value
        self.sum_squared += value * value
        self.count += 1
        self.current_value = value

    def update_data_point(self, old_value, new_value):
        self.remove_data_point(old_value)
        self.add_data_point(new_value)

    def get_z_score(self):
        """
        Calculate the Z-score for the current value.
        """
        if self.count == 0 or self.current_value is None:
            return 0

        mean = self.sum / self.count

        variance = (self.sum_squared / self.count) - (mean * mean)

        if variance <= 0:
            return 0

        std_dev = np.sqrt(variance)

        return (self.current_value - mean) / std_dev


def initialize_zscores(symbols, hours_options, zscore_data_types):
    """
    Initialize Z-score objects using historical kline data from the database.
    Returns nested dictionary: {symbol: {data_type: {timeframe: ZScore}}}
    """
    dict = {}

    for symbol in symbols:
        for data_type in zscore_data_types:
            for hours in hours_options:
                dict.setdefault(symbol, {}).setdefault(data_type, {})[hours] = (
                    IncrementalZScore(hours * 60)
                )

    for hours in hours_options:
        data_by_symbol = get_historical_kline_data(hours=hours, symbols=symbols)

        for symbol in symbols:
            for data_type in zscore_data_types:
                series = data_by_symbol.get(symbol, {}).get(data_type, [])
                if series is not None and len(series) > 0:
                    dict[symbol][data_type][hours].initialize_from_data(series.tolist())

    return dict


def update_zscores(incremental_zscores, symbols, timeframes):
    """
    Update incremental Z-scores by removing oldest values and adding new ones.
    """
    symbol_data = get_symbol_kline_data(symbols=symbols)

    for symbol in symbols:
        if symbol not in symbol_data:
            continue

        new_data = symbol_data[symbol]

        for tf in timeframes:
            old_data = get_oldest_kline_for_timeframe(symbol, tf)

            for data_type in zscore_data_types:
                if (
                    symbol in incremental_zscores
                    and data_type in incremental_zscores[symbol]
                ):
                    zscore_obj = incremental_zscores[symbol][data_type].get(tf)

                    if zscore_obj:
                        new_value = new_data[data_type]

                        if old_data:
                            old_value = old_data[data_type]
                            zscore_obj.update_data_point(old_value, new_value)
                        else:
                            zscore_obj.add_data_point(new_value)


def create_z_score_results(incremental_zscores, timeframes):
    return {
        tf: {
            symbol: {
                data_type: round(tf_dict[tf].get_z_score(), 2)
                for data_type, tf_dict in data_types.items()
            }
            for symbol, data_types in incremental_zscores.items()
        }
        for tf in timeframes
    }


def initialize_incremental_zscore():
    """
    Calculate and cache Z-scores for all combinations of hours, data types, and symbols.
    """
    symbols = get_exchange_symbols()
    hours_options = list(tf_options["zscore"].values())

    incremental_zscores = initialize_zscores(symbols, hours_options, zscore_data_types)

    pubsub = r.pubsub()
    pubsub.subscribe(RedisPubMessages.KLINE_SAVED_TO_DB.value)
    pubsub.get_message()

    for message in pubsub.listen():
        if (
            message["type"] == "message"
            and message["channel"] == RedisPubMessages.KLINE_SAVED_TO_DB.value
        ):
            print(f'ZSCORE {message["channel"]}')
            update_zscores(incremental_zscores, symbols, hours_options)

            results = create_z_score_results(incremental_zscores, hours_options)

            pipeline = r.pipeline()

            for tf, tf_data in results.items():
                pipeline.execute_command(
                    "SET",
                    f"zscore:{tf}",
                    msgpack.packb(tf_data),
                )

            pipeline.execute()
