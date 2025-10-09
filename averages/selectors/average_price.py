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
        cte_name = "daily"
    else:
        trunc_unit = "hour"
        extract_unit = "HOUR"
        cte_name = "hourly"

    query = f"""
        WITH {cte_name}_prices AS (
            SELECT
                DATE_TRUNC('{trunc_unit}', start_time) AS period_block,
                EXTRACT({extract_unit} FROM start_time) AS period,
                (FIRST_VALUE(open) OVER (PARTITION BY DATE_TRUNC('{trunc_unit}', start_time) ORDER BY start_time ASC))::float AS first_open,
                (FIRST_VALUE(close) OVER (PARTITION BY DATE_TRUNC('{trunc_unit}', start_time) ORDER BY start_time DESC))::float AS last_close
            FROM cs_klines_1m
            WHERE symbol_id = %s
                AND start_time >= %s
        ),
        {cte_name}_pct AS (
            SELECT DISTINCT
                period_block,
                period,
                ((last_close - first_open) / last_close * 100) AS pct_change
            FROM {cte_name}_prices
        )
        SELECT
            period,
            AVG(pct_change) AS avg_pct_change
        FROM {cte_name}_pct
        GROUP BY period
        ORDER BY period
    """

    with connection.cursor() as cursor:
        cursor.execute(query, [symbol_obj.id, start_time_utc])  # type: ignore
        rows = cursor.fetchall()

    period_data = {}
    for row in rows:
        period = int(row[0])
        avg_pct = float(row[1]) if row[1] is not None else 0.0
        period_data[period] = avg_pct

    return period_data
