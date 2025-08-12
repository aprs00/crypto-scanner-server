import redis
import numpy as np
import msgpack
from datetime import timedelta
from django.utils import timezone

from filters.constants import tf_options
from zscore.constants import zscore_data_types
from zscore.selectors.zscore import get_oldest_kline_for_timeframe
from exchange_connections.selectors import get_exchange_symbols, get_latest_kline_values
from exchange_connections.models import Kline1m
from exchange_connections.constants import KLINE_FIELD_MAP
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


def initialize_zscore_dict(symbols, timeframes, zscore_data_types):
    """
    Initialize Z-score objects using historical kline data from the database.
    Returns nested dictionary: {symbol: {data_type: {timeframe: ZScore}}}
    """
    dict = {}

    for symbol in symbols:
        dict[symbol] = {}
        for data_type in zscore_data_types:
            dict[symbol][data_type] = {}
            for tf in timeframes:
                dict[symbol][data_type][tf] = IncrementalZScore(tf * 60)

    for tf in timeframes:
        start_time = timezone.now() - timedelta(hours=tf)

        klines = (
            Kline1m.objects.filter(symbol__name__in=symbols, start_time__gte=start_time)
            .values(
                "symbol__name", "close", "base_volume", "number_of_trades", "start_time"
            )
            .order_by("symbol__name", "start_time")
        )

        data_by_symbol = {
            symbol: {dt: [] for dt in zscore_data_types} for symbol in symbols
        }

        for kline in klines:
            symbol = kline["symbol__name"]
            for data_type, field in KLINE_FIELD_MAP.items():
                if data_type in zscore_data_types:
                    data_by_symbol[symbol][data_type].append(float(kline[field]))

        for symbol in symbols:
            for data_type in zscore_data_types:
                if data_by_symbol[symbol][data_type]:
                    dict[symbol][data_type][tf].initialize_from_data(
                        data_by_symbol[symbol][data_type]
                    )

    return dict


def get_symbols_data():
    """
    Get the latest data for all symbols and all data types from the database.
    Returns:
        {symbol: {data_type: value, ...}, ...}
    """
    return {
        kline.symbol.name: {
            "price": float(kline.close),
            "volume": float(kline.base_volume),
            "trades": float(kline.number_of_trades),
        }
        for kline in get_latest_kline_values()
    }


def update_z_scores_incremental(incremental_zscores, symbols, timeframes):
    """
    Update incremental Z-scores by removing oldest values and adding new ones.
    """
    symbol_data = get_symbols_data()

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
    """
    Create Z-score results for all timeframes.
    Returns a dictionary per timeframe: {tf: {symbol: {data_type: z_score}}}
    """
    results = {}

    for tf in timeframes:
        tf_results = {}

        for symbol, data_types in incremental_zscores.items():
            tf_results[symbol] = {}

            for data_type, tf_dict in data_types.items():
                if tf in tf_dict:
                    z_score = round(tf_dict[tf].get_z_score(), 2)
                    tf_results[symbol][data_type] = z_score

        results[tf] = tf_results

    return results


def initialize_incremental_zscore():
    """
    Calculate and cache Z-scores for all combinations of timeframes, data types, and symbols.
    """
    symbols = get_exchange_symbols()
    timeframes = list(tf_options["zscore"].values())

    incremental_zscores = initialize_zscore_dict(symbols, timeframes, zscore_data_types)

    pubsub = r.pubsub()
    pubsub.subscribe(RedisPubMessages.KLINE_SAVED_TO_DB.value)

    for message in pubsub.listen():
        if (
            message["type"] == "message"
            and message["channel"] == RedisPubMessages.KLINE_SAVED_TO_DB.value
        ):
            update_z_scores_incremental(incremental_zscores, symbols, timeframes)

            results = create_z_score_results(incremental_zscores, timeframes)

            pipeline = r.pipeline()

            for tf, tf_data in results.items():
                pipeline.execute_command(
                    "SET",
                    f"zscore:{tf}",
                    msgpack.packb(tf_data),
                )

            pipeline.execute()
