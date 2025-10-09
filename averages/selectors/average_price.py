from django.db import connection
from exchange_connections.models import Symbol

from averages.constants import TimePeriod


def get_average_price_change_by_period(
    symbol, exchange, start_time_utc, contract_type, period_type: TimePeriod
):
    """
    Calculate average price change per time period using raw SQL for performance.

    Args:
        symbol: Symbol name
        exchange: Exchange name
        start_time_utc: Start time for data
        contract_type: Contract type name
        period_type: TimePeriod enum (DAY or HOUR)

    Returns:
        Dictionary mapping period (weekday 0-6 or hour 0-23) to average percent change
    """
    symbol_obj = Symbol.objects.get(
        name=symbol,
        exchange__name=exchange,
        contract_type__name=contract_type,
    )

    if period_type == TimePeriod.DAY:
        trunc_unit = "day"
        extract_unit = "DOW"
    else:
        trunc_unit = "hour"
        extract_unit = "HOUR"

    query = f"""
        WITH time_range AS (
            SELECT
                generate_series(
                    DATE_TRUNC('{trunc_unit}', %s::timestamptz),
                    DATE_TRUNC('{trunc_unit}', NOW()),
                    '1 {trunc_unit}'::interval
                ) AS period_start
        ),
        period_agg AS (
            SELECT
                tr.period_start,
                EXTRACT({extract_unit} FROM tr.period_start) AS period,
                (SELECT open FROM cs_klines_1m
                 WHERE symbol_id = %s
                 AND start_time >= tr.period_start
                 AND start_time < tr.period_start + '1 {trunc_unit}'::interval
                 ORDER BY start_time ASC LIMIT 1) AS first_open,
                (SELECT close FROM cs_klines_1m
                 WHERE symbol_id = %s
                 AND start_time >= tr.period_start
                 AND start_time < tr.period_start + '1 {trunc_unit}'::interval
                 ORDER BY start_time DESC LIMIT 1) AS last_close
            FROM time_range tr
        )
        SELECT
            period,
            AVG(((last_close - first_open) / last_close * 100)) AS avg_pct_change
        FROM period_agg
        WHERE first_open IS NOT NULL
        GROUP BY period
        ORDER BY period
    """

    with connection.cursor() as cursor:
        cursor.execute(query, [start_time_utc, symbol_obj.id, symbol_obj.id])  # type: ignore
        rows = cursor.fetchall()

    period_data = {}
    for row in rows:
        period = int(row[0])
        avg_pct = float(row[1]) if row[1] is not None else 0.0
        period_data[period] = avg_pct

    return period_data
