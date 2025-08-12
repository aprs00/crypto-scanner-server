import redis
import numpy as np
import time
from django.utils import timezone
from datetime import timedelta
from django.db.models import FloatField
from django.db.models.functions import Cast

from exchange_connections.models import Kline1m
from exchange_connections.constants import KLINE_FIELD_MAP
from exchange_connections.selectors import get_latest_kline_values


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
    field_name = KLINE_FIELD_MAP.get(data_type)
    annotated_field = f"{field_name}_as_float"

    end_time = timezone.now()
    start_time = end_time - timedelta(hours=duration_hours)

    all_data = (
        Kline1m.objects.filter(
            symbol__name__in=symbols,
            start_time__gte=start_time.astimezone(timezone.utc),
            start_time__lte=end_time.astimezone(timezone.utc),
            exchange__name="binance",
        )
        .annotate(**{annotated_field: Cast(field_name, FloatField())})
        .values("symbol__name", "start_time", annotated_field)
        .order_by("symbol__name", "start_time")
    )

    query_symbols_data = {symbol: [] for symbol in symbols}
    for item in all_data:
        symbol = item["symbol__name"]
        if symbol in query_symbols_data:
            query_symbols_data[symbol].append(item[annotated_field])

    query_symbols_data = {k: np.array(v) for k, v in query_symbols_data.items()}

    return query_symbols_data


def get_symbols_x_hours_ago(symbols: list, hours: int):
    target_time = timezone.now() - timedelta(hours=hours)

    klines = (
        Kline1m.objects.filter(
            symbol__name__in=symbols,
            exchange__name="binance",
            start_time__lte=target_time.astimezone(timezone.utc),
        )
        .select_related("symbol", "exchange")
        .annotate(
            price=Cast("close", FloatField()),
            volume=Cast("base_volume", FloatField()),
            trades=Cast("number_of_trades", FloatField()),
        )
        .order_by("symbol__name", "-start_time")
        .distinct("symbol__name")
        .values("symbol__name", "price", "volume", "trades")
    )

    return {
        kline["symbol__name"]: {
            "price": kline["price"],
            "volume": kline["volume"],
            "trades": kline["trades"],
        }
        for kline in klines
    }


def get_symbol_data(symbols):
    """
    Get the latest data for all symbols and all data types from the database.
    Returns:
        {symbol: {data_type: value, ...}, ...}
    """
    result = {symbol: {} for symbol in symbols}
    latest_klines = get_latest_kline_values()

    for kline in latest_klines:
        symbol_name = (
            kline.symbol.name if hasattr(kline.symbol, "name") else str(kline.symbol)
        )

        if symbol_name in result:
            for data_type, db_column in KLINE_FIELD_MAP.items():
                result[symbol_name][data_type] = float(getattr(kline, db_column))

    return result


"""
this code is used for storing streaming pearson correlations.
every minute new data gets saved to database, that data is then queried and added to pearson formula
in the same time oldest value is removed from the calculation
"""
