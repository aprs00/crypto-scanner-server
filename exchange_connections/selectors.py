from django.utils import timezone
from datetime import timedelta
from typing import Optional, List
from django.db import connection
from collections import defaultdict

from exchange_connections.models import Kline1m, Exchange, Symbol
from exchange_connections.constants import KLINE_FIELD_MAP
from core.redis_config import get_redis_connection

r = get_redis_connection()


def get_exchange_symbols(exchange="binance", contract_type="perpetual"):
    symbols_b = r.execute_command("SMEMBERS", f"symbols:{exchange}:{contract_type}")
    return sorted([symbol.decode("utf-8") for symbol in symbols_b])


def get_historical_kline_data(hours, symbols):
    """Get historical ticker data from the database for all KLINE fields."""

    end_time = timezone.now()
    start_time = end_time - timedelta(hours=hours)

    exchange = Exchange.objects.get(name="binance")
    symbol_ids = Symbol.objects.filter(name__in=symbols, exchange=exchange).values_list(
        "id", flat=True
    )

    klines = (
        Kline1m.objects.filter(
            exchange=exchange,
            symbol_id__in=symbol_ids,
            start_time__gte=start_time.astimezone(timezone.utc),
            start_time__lte=end_time.astimezone(timezone.utc),
        )
        .select_related("symbol")
        .values(
            "symbol__name", "start_time", "close", "base_volume", "number_of_trades"
        )
        .order_by("symbol__name", "start_time")
    )

    klines_data = defaultdict(lambda: {field: [] for field in KLINE_FIELD_MAP.keys()})

    for item in klines:
        symbol_name = item["symbol__name"]
        symbol_data = klines_data[symbol_name]
        symbol_data["price"].append(float(item["close"]))
        symbol_data["volume"].append(float(item["base_volume"]))
        symbol_data["trades"].append(float(item["number_of_trades"]))

    return dict(klines_data)


def get_symbol_kline_data(
    symbols: list, exchange: str, contract_type: str, hours: Optional[int] = None
):
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
