from django.utils import timezone
from datetime import timedelta, timezone as dt_timezone
from typing import Optional, List, Sequence, cast
from django.db import connection
from collections import defaultdict

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

    symbol_placeholders = ",".join(["%s"] * len(symbols))

    query = f"""
        SELECT
            s.name AS symbol_name,
            k.close,
            k.base_volume,
            k.number_of_trades
        FROM cs_klines_1m k
        JOIN cs_symbols s ON k.symbol_id = s.id
        JOIN cs_exchanges e ON k.exchange_id = e.id
        WHERE
            e.name = 'binance'
            AND s.name IN ({symbol_placeholders})
            AND k.start_time >= %s
            AND k.start_time <= %s
        ORDER BY s.name, k.start_time
    """

    klines_data = defaultdict(lambda: {field: [] for field in KLINE_FIELD_MAP.keys()})

    with connection.cursor() as cursor:
        params = symbols + [
            start_time.astimezone(dt_timezone.utc),
            end_time.astimezone(dt_timezone.utc),
        ]
        cursor.execute(query, params)

        for row in cursor.fetchall():
            symbol_name = row[0]
            symbol_data = klines_data[symbol_name]
            symbol_data["price"].append(float(row[1]))
            symbol_data["volume"].append(float(row[2]))
            symbol_data["trades"].append(float(row[3]))

    return dict(klines_data)


def get_symbol_kline_data(
    symbols: list, exchange: str, contract_type: str, hours: Optional[int] = None
):
    symbol_placeholders = ",".join(["%s"] * len(symbols))

    if hours is not None:
        target_time = timezone.now() - timedelta(hours=hours)
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


def get_top_market_cap_symbols(
    limit: int = 100, exchange: str = "binance", contract_type: str = "perpetual"
) -> List[str]:
    """Return a simple list of symbols sorted by market cap descending."""
    if limit <= 0:
        return []

    zset_key = f"market_cap:{exchange}:{contract_type}"
    raw_entries = r.zrevrange(zset_key, 0, limit - 1)
    entries: Sequence[bytes] = cast(Sequence[bytes], raw_entries)

    symbols: List[str] = []
    for symbol_bytes in entries:
        try:
            symbol = symbol_bytes.decode("utf-8")
        except AttributeError:
            symbol = str(symbol_bytes)
        symbols.append(symbol)

    return symbols
