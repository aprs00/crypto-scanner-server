import redis
import numpy as np
import time

from django.utils import timezone
from datetime import timedelta
from django.db.models import FloatField
from django.db.models.functions import Cast

from correlations.utils import get_min_length
from crypto_scanner.models import BinanceSpotKline5m

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
        BinanceSpotKline5m.objects.filter(
            ticker__in=symbols,
            start_time__gte=start_time.astimezone(timezone.utc),
            start_time__lte=end_time.astimezone(timezone.utc),
        )
        .annotate(**{annotated_field: Cast(field_name, FloatField())})
        .values("ticker", "start_time", annotated_field)
        .order_by("ticker", "start_time")
    )

    query_tickers_data = {ticker: [] for ticker in symbols}
    for item in all_data:
        ticker = item["ticker"]
        if ticker in query_tickers_data:
            query_tickers_data[ticker].append(item[annotated_field])

    query_tickers_data = get_min_length(query_tickers_data, symbols)
    query_tickers_data = {k: np.array(v) for k, v in query_tickers_data.items()}

    return query_tickers_data


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
            "TS.RANGE", f"1s:{data_type}:{symbol}", from_time, to_time
        )
        result[symbol] = np.array([float(x[1]) for x in redis_data])

    return result
