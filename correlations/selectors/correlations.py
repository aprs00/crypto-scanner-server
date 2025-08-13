import redis
import numpy as np
from django.utils import timezone
from datetime import timedelta
from typing import Optional

from exchange_connections.models import Kline1m
from exchange_connections.constants import KLINE_FIELD_MAP, kline_annotations


r = redis.Redis(host="redis")


def get_historical_kline_data(hours, symbols):
    """Get historical ticker data from the database for all KLINE fields."""

    end_time = timezone.now()
    start_time = end_time - timedelta(hours=hours)

    klines = (
        Kline1m.objects.filter(
            symbol__name__in=symbols,
            start_time__gte=start_time.astimezone(timezone.utc),
            start_time__lte=end_time.astimezone(timezone.utc),
            exchange__name="binance",
        )
        .annotate(**kline_annotations)
        .values("symbol__name", "start_time", *kline_annotations.keys())
        .order_by("symbol__name", "start_time")
    )

    klines_data = {}

    for item in klines:
        symbol = item["symbol__name"]

        if symbol not in klines_data:
            klines_data[symbol] = {field: [] for field in KLINE_FIELD_MAP.keys()}

        for data_type, field_name in KLINE_FIELD_MAP.items():
            klines_data[symbol][data_type].append(item[f"{field_name}_as_float"])

    for symbol in klines_data:
        for data_type in klines_data[symbol]:
            klines_data[symbol][data_type] = np.array(klines_data[symbol][data_type])

    return klines_data


def get_symbol_kline_data(symbols: list, hours: Optional[int] = None):
    """
    If hours is provided, gets the kline data from X hours ago
    Else, gets the most recent available kline data.
    """
    queryset = Kline1m.objects.filter(
        symbol__name__in=symbols,
        exchange__name="binance",
    )

    if hours is not None:
        target_time = timezone.now() - timedelta(hours=hours)
        queryset = queryset.filter(
            start_time__lte=target_time.astimezone(timezone.utc),
        )

    klines = (
        queryset.select_related("symbol", "exchange")
        .annotate(**kline_annotations)
        .order_by("symbol__name", "-start_time")
        .distinct("symbol__name")
        .values("symbol__name", *kline_annotations.keys())
    )

    return {
        kline["symbol__name"]: {
            data_type: kline[f"{field_name}_as_float"]
            for data_type, field_name in KLINE_FIELD_MAP.items()
        }
        for kline in klines
    }
