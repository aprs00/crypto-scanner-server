import redis
import numpy as np
import time
from django.utils import timezone
from datetime import timedelta
from django.db.models import FloatField
from django.db.models.functions import Cast

from exchange_connections.models import Kline1m

r = redis.Redis(host="redis")


def get_tickers_data(duration_hours, data_type, symbols):
    """
    Get historical ticker data from the database.

    Args:
        duration_hours: The duration in hours to retrieve data for
        data_type: The type of data to retrieve (price, volume, trades)

    Returns:
        Dict mapping symbols to their historical data
    """
    field_mapping = {
        "price": "close",
        "volume": "base_volume",
        "trades": "number_of_trades",
    }

    field_name = field_mapping.get(data_type)
    annotated_field = f"{field_name}_as_float"

    end_time = timezone.now()
    start_time = end_time - timedelta(hours=duration_hours)

    all_data = (
        Kline1m.objects.filter(
            symbol__in=symbols,
            start_time__gte=start_time.astimezone(timezone.utc),
            start_time__lte=end_time.astimezone(timezone.utc),
            exchange="binance",
        )
        .annotate(**{annotated_field: Cast(field_name, FloatField())})
        .values("symbol", "start_time", annotated_field)
        .order_by("symbol", "start_time")
    )

    query_symbols_data = {symbol: [] for symbol in symbols}
    for item in all_data:
        symbol = item["symbol"]
        if symbol in query_symbols_data:
            query_symbols_data[symbol].append(item[annotated_field])

    query_symbols_data = {k: np.array(v) for k, v in query_symbols_data.items()}

    return query_symbols_data


def extract_time_series_data(tf, data_type, symbols):
    """
    Get historical data for symbols of the specified data type from Redis.

    Args:
        timeframe: The timeframe to retrieve data for
        data_type: The type of data to retrieve
        symbols: List of symbols to get data for.

    Returns:
        Dict mapping symbols to their historical data
    """
    current_time_ms = int(time.time() * 1000)
    from_time = current_time_ms - tf * 60 * 1000
    to_time = int(timezone.now().timestamp() * 1000)

    result = {}
    for symbol in symbols:
        redis_data = r.execute_command(
            "TS.RANGE", f"250ms:{data_type}:{symbol}", from_time, to_time
        )
        result[symbol] = np.array([float(x[1]) for x in redis_data])

    return result
