from django.db import connection
from exchange_connections.models import Symbol


def get_average_price_change_by_day(symbol, exchange, start_time_utc, contract_type):
    """
    Calculate average price change per day of week using raw SQL for performance.
    Returns aggregated data ready for display.
    """
    symbol_obj = Symbol.objects.get(
        name=symbol,
        exchange__name=exchange,
        contract_type__name=contract_type,
    )

    query = """
        WITH daily_prices AS (
            SELECT
                DATE_TRUNC('day', start_time) AS day,
                EXTRACT(DOW FROM start_time) AS weekday,
                (FIRST_VALUE(open) OVER (PARTITION BY DATE_TRUNC('day', start_time) ORDER BY start_time ASC))::float AS first_open,
                (FIRST_VALUE(close) OVER (PARTITION BY DATE_TRUNC('day', start_time) ORDER BY start_time DESC))::float AS last_close
            FROM cs_klines_1m
            WHERE symbol_id = %s
                AND start_time >= %s
        ),
        daily_pct AS (
            SELECT DISTINCT
                day,
                weekday,
                ((last_close - first_open) / last_close * 100) AS pct_change
            FROM daily_prices
        )
        SELECT
            weekday,
            AVG(pct_change) AS avg_pct_change
        FROM daily_pct
        GROUP BY weekday
        ORDER BY weekday
    """

    with connection.cursor() as cursor:
        cursor.execute(query, [symbol_obj.id, start_time_utc])  # type: ignore
        rows = cursor.fetchall()

    weekday_data = {}
    for row in rows:
        weekday = int(row[0])
        avg_pct = float(row[1]) if row[1] is not None else 0.0
        weekday_data[weekday] = avg_pct

    return weekday_data


def get_average_price_change_by_hour(symbol, exchange, start_time_utc, contract_type):
    """
    Calculate average price change per hour of day using raw SQL for performance.
    Returns aggregated data ready for display.
    """
    symbol_obj = Symbol.objects.get(
        name=symbol,
        exchange__name=exchange,
        contract_type__name=contract_type,
    )

    query = """
        WITH hourly_prices AS (
            SELECT
                DATE_TRUNC('hour', start_time) AS hour_block,
                EXTRACT(HOUR FROM start_time) AS hour,
                (FIRST_VALUE(open) OVER (PARTITION BY DATE_TRUNC('hour', start_time) ORDER BY start_time ASC))::float AS first_open,
                (FIRST_VALUE(close) OVER (PARTITION BY DATE_TRUNC('hour', start_time) ORDER BY start_time DESC))::float AS last_close
            FROM cs_klines_1m
            WHERE symbol_id = %s
                AND start_time >= %s
        ),
        hourly_pct AS (
            SELECT DISTINCT
                hour_block,
                hour,
                ((last_close - first_open) / last_close * 100) AS pct_change
            FROM hourly_prices
        )
        SELECT
            hour,
            AVG(pct_change) AS avg_pct_change
        FROM hourly_pct
        GROUP BY hour
        ORDER BY hour
    """

    with connection.cursor() as cursor:
        cursor.execute(query, [symbol_obj.id, start_time_utc])  # type: ignore
        rows = cursor.fetchall()

    hour_data = {}
    for row in rows:
        hour = int(row[0])
        avg_pct = float(row[1]) if row[1] is not None else 0.0
        hour_data[hour] = avg_pct

    return hour_data
