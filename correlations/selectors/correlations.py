import redis
import numpy as np
import time
from django.utils import timezone
from datetime import timedelta
from django.db.models import FloatField, Min
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


def get_oldest_values_efficient(
    duration_hours: int, data_type: str, symbols: list
) -> dict:
    """
    Get the oldest value for each symbol that should be removed from the sliding window.
    This should be the value that's exactly `duration_hours` old.
    """
    field_mapping = {
        "price": "close",
        "volume": "base_volume",
        "trades": "number_of_trades",
    }

    field_name = field_mapping.get(data_type)
    if not field_name:
        return {symbol: (0.0, 0) for symbol in symbols}

    annotated_field = f"{field_name}_as_float"

    target_time = timezone.now() - timedelta(hours=duration_hours)

    buffer_time = timedelta(minutes=30)
    start_window = target_time - buffer_time
    end_window = target_time + buffer_time

    oldest_data = (
        Kline1m.objects.filter(
            symbol__in=symbols,
            start_time__gte=start_window.astimezone(timezone.utc),
            start_time__lte=end_window.astimezone(timezone.utc),
            exchange="binance",
        )
        .annotate(**{annotated_field: Cast(field_name, FloatField())})
        .values("symbol", "start_time", annotated_field)
        .order_by("symbol", "start_time")
    )

    result = {}
    for symbol in symbols:
        symbol_data = [item for item in oldest_data if item["symbol"] == symbol]
        if symbol_data:
            closest_record = min(
                symbol_data,
                key=lambda x: abs(
                    (
                        x["start_time"] - target_time.astimezone(timezone.utc)
                    ).total_seconds()
                ),
            )
            result[symbol] = (
                float(closest_record[annotated_field]),
                int(closest_record["start_time"].timestamp()),
            )
        else:
            result[symbol] = (0.0, 0)

    return result
