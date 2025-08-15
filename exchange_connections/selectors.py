import numpy as np
from django.utils import timezone
from datetime import timedelta
from typing import Optional
from django.db import connection

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
    print("OPTIMIZED QUERY")
    symbol_placeholders = ",".join(["%s"] * len(symbols))

    if hours is not None:
        target_time = timezone.now() - timezone.timedelta(hours=hours)
        time_condition = "AND k.start_time <= %s"
        params = [target_time] + [exchange, contract_type] + symbols
    else:
        time_condition = ""
        params = [exchange, contract_type] + symbols

    query = f"""
        SELECT 
            s.name AS symbol_name,
            k.close,
            k.base_volume,
            k.number_of_trades
        FROM cs_symbols s
        JOIN cs_exchanges e ON s.exchange_id = e.id
        JOIN cs_contract_types ct ON s.contract_type_id = ct.id
        CROSS JOIN LATERAL (
            SELECT close, base_volume, number_of_trades
            FROM cs_klines_1m k
            WHERE 
                k.symbol_id = s.id
                AND k.exchange_id = e.id
                {time_condition}
            ORDER BY k.start_time DESC
            LIMIT 1
        ) k
        WHERE 
            e.name = %s
            AND ct.name = %s
            AND s.name IN ({symbol_placeholders})
    """

    with connection.cursor() as cursor:
        cursor.execute(query, params)
        rows = cursor.fetchall()

        return {
            row[0]: {
                "price": float(row[1]),
                "volume": float(row[2]),
                "trades": float(row[3]),
            }
            for row in rows
        }
