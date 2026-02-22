from django.utils import timezone
from datetime import datetime, timedelta, timezone as dt_timezone
from typing import Optional, List, Sequence, cast
from django.db import connection
import math

from core.redis_config import get_redis_connection

r = get_redis_connection()


def get_exchange_symbols(exchange, contract_type="perpetual"):
    symbols_b = r.execute_command("SMEMBERS", f"symbols:{exchange}:{contract_type}")
    return sorted([symbol.decode("utf-8") for symbol in symbols_b])


def get_historical_kline_data(
    hours,
    symbols,
    exchange,
    contract_type: str = "perpetual",
    end_time: Optional[datetime] = None,
):
    """Get historical ticker data from the database for all KLINE fields.

    Returns time-aligned data with NaN for missing minutes to ensure
    data continuity. This prevents correlation calculations from mixing
    data from different time periods when there are gaps in the database.
    """
    if not symbols or hours <= 0:
        return {}

    if end_time is None:
        end_time = timezone.now().replace(second=0, microsecond=0)
    elif end_time.tzinfo is None:
        end_time = end_time.replace(tzinfo=dt_timezone.utc)
    end_time = end_time.replace(second=0, microsecond=0)
    start_time = end_time - timedelta(hours=hours)
    total_minutes = hours * 60

    symbol_placeholders = ",".join(["%s"] * len(symbols))

    query = f"""
        SELECT
            s.name AS symbol_name,
            k.start_time,
            k.close,
            k.base_volume,
            k.number_of_trades
        FROM cs_klines_1m k
        JOIN cs_symbols s ON k.symbol_id = s.id
        JOIN cs_exchanges e ON k.exchange_id = e.id
        JOIN cs_contract_types ct ON s.contract_type_id = ct.id
        WHERE
            e.name = %s
            AND ct.name = %s
            AND s.name IN ({symbol_placeholders})
            AND k.start_time >= %s
            AND k.start_time < %s
        ORDER BY s.name, k.start_time
    """

    # Initialize with NaN for all minutes to handle gaps
    klines_data = {}
    for sym in symbols:
        klines_data[sym] = {
            "price": [math.nan] * total_minutes,
            "volume": [math.nan] * total_minutes,
            "trades": [math.nan] * total_minutes,
        }

    start_time_utc = start_time.astimezone(dt_timezone.utc)
    end_time_utc = end_time.astimezone(dt_timezone.utc)

    with connection.cursor() as cursor:
        params = [exchange, contract_type] + symbols + [start_time_utc, end_time_utc]
        cursor.execute(query, params)

        for row in cursor.fetchall():
            symbol_name = row[0]
            row_time = row[1]
            # Calculate the minute index from start_time
            minute_idx = int((row_time - start_time_utc).total_seconds() // 60)

            if 0 <= minute_idx < total_minutes and symbol_name in klines_data:
                symbol_data = klines_data[symbol_name]
                symbol_data["price"][minute_idx] = float(row[2])
                symbol_data["volume"][minute_idx] = float(row[3])
                symbol_data["trades"][minute_idx] = float(row[4])

    return klines_data


def get_symbol_kline_data(
    symbols: list,
    exchange: str,
    contract_type: str,
    hours: Optional[int] = None,
    kline_timestamp_ms: Optional[int] = None,
):
    if not symbols:
        return {}
    symbol_placeholders = ",".join(["%s"] * len(symbols))

    if hours is not None:
        if kline_timestamp_ms is not None:
            kline_time = datetime.fromtimestamp(
                kline_timestamp_ms / 1000, tz=dt_timezone.utc
            )
            target_time = kline_time - timedelta(hours=hours)
        else:
            now = timezone.now().replace(second=0, microsecond=0)
            target_time = now - timedelta(hours=hours)
        time_condition = "AND k.start_time >= %s"
        order_direction = "ASC"
        params = [target_time] + [exchange, contract_type] + symbols
    else:
        time_condition = ""
        order_direction = "DESC"
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
            ORDER BY k.start_time {order_direction}
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


def get_symbol_kline_data_at_timestamp(
    symbols: list,
    exchange: str,
    contract_type: str,
    kline_timestamp_ms: int,
):
    if not symbols:
        return {}

    symbol_placeholders = ",".join(["%s"] * len(symbols))
    target_time = datetime.fromtimestamp(kline_timestamp_ms / 1000, tz=dt_timezone.utc)

    query = f"""
        SELECT
            s.name AS symbol_name,
            k.close,
            k.base_volume,
            k.number_of_trades
        FROM cs_klines_1m k
        JOIN cs_symbols s ON k.symbol_id = s.id
        JOIN cs_exchanges e ON k.exchange_id = e.id
        JOIN cs_contract_types ct ON s.contract_type_id = ct.id
        WHERE
            e.name = %s
            AND ct.name = %s
            AND s.name IN ({symbol_placeholders})
            AND k.start_time = %s
    """

    params = [exchange, contract_type] + symbols + [target_time]

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


def get_symbol_kline_data_multi_hours(
    symbols: list,
    exchange: str,
    contract_type: str,
    hours_list: List[int],
    kline_timestamp_ms: Optional[int] = None,
):
    """
    Fetch the exact kline for each symbol at each time offset (hours ago).

    For incremental processors, timestamp alignment must be exact to avoid
    removing a value from a different minute than the one that was added.
    Returns: Dict[hours, Dict[symbol, {price, volume, trades}]]
    """
    if not hours_list or not symbols:
        return {}

    symbol_placeholders = ",".join(["%s"] * len(symbols))

    if kline_timestamp_ms is not None:
        base_time = datetime.fromtimestamp(
            kline_timestamp_ms / 1000, tz=dt_timezone.utc
        )
    else:
        base_time = timezone.now().replace(second=0, microsecond=0)

    query = f"""
        SELECT
            h.hours_offset,
            s.name AS symbol_name,
            k.close,
            k.base_volume,
            k.number_of_trades
        FROM cs_symbols s
        JOIN cs_exchanges e ON s.exchange_id = e.id
        JOIN cs_contract_types ct ON s.contract_type_id = ct.id
        CROSS JOIN LATERAL (SELECT unnest(%s::int[]) AS hours_offset) h
        CROSS JOIN LATERAL (
            SELECT close, base_volume, number_of_trades
            FROM cs_klines_1m k
            WHERE
                k.symbol_id = s.id
                AND k.exchange_id = e.id
                AND k.start_time = %s - (h.hours_offset || ' hours')::interval
            LIMIT 1
        ) k
        WHERE
            e.name = %s
            AND ct.name = %s
            AND s.name IN ({symbol_placeholders})
    """

    params = [
        hours_list,
        base_time,
        exchange,
        contract_type,
    ] + symbols

    result = {h: {} for h in hours_list}

    with connection.cursor() as cursor:
        cursor.execute(query, params)
        for row in cursor.fetchall():
            hours_offset = row[0]
            symbol_name = row[1]
            result[hours_offset][symbol_name] = {
                "price": float(row[2]),
                "volume": float(row[3]),
                "trades": float(row[4]),
            }

    return result


def get_top_market_cap_symbols(
    limit: int, exchange: str, contract_type: str = "perpetual"
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
