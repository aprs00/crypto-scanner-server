import numpy as np
from django.utils import timezone
from datetime import timedelta
from typing import Optional

from exchange_connections.models import Kline1m, Symbol
from exchange_connections.constants import KLINE_FIELD_MAP, kline_annotations


def get_exchange_symbols(exchange="binance", contract_type="perpetual"):
    return list(
        Symbol.objects.filter(
            exchange__name=exchange,
            contract_type__name=contract_type,
        )
        .order_by("name")
        .distinct("name")
        .values_list("name", flat=True)
    )


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


def get_symbol_kline_data(
    symbols: list, exchange: str, contract_type: str, hours: Optional[int] = None
):
    """
    If hours is provided, gets the kline data from X hours ago.
    Else, gets the most recent available kline data for the given exchange and contract type.
    """
    print("11")

    base_qs = Kline1m.objects.filter(
        symbol__name__in=symbols,
        exchange__name=exchange,
        symbol__contract_type__name=contract_type,
    )

    print("22")

    if hours is not None:
        target_time = timezone.now() - timedelta(hours=hours)
        base_qs = base_qs.filter(
            start_time__lte=target_time.astimezone(timezone.utc),
        )

    print("33")

    latest_ids = list(
        base_qs.order_by("symbol__name", "-start_time")
        .distinct("symbol__name")
        .values_list("id", flat=True)
    )

    print("44")

    klines = (
        Kline1m.objects.filter(id__in=latest_ids)
        .annotate(**kline_annotations)
        .values("symbol__name", *kline_annotations.keys())
    )

    print("55")

    return {
        kline["symbol__name"]: {
            data_type: kline[f"{field_name}_as_float"]
            for data_type, field_name in KLINE_FIELD_MAP.items()
        }
        for kline in klines
    }
