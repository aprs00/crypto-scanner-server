from exchange_connections.models import Kline1m, Symbol


def get_average_symbol_data(symbol, exchange, start_time_utc, group_by, contract_type):
    group_by_mapping = {
        "day": {"extract_function": "dow", "column_name": "day_of_week"},
        "hour": {"extract_function": "hour", "column_name": "hour_of_day"},
    }

    settings = group_by_mapping.get(group_by)
    if not settings:
        raise ValueError(f"Invalid group_by value: {group_by}")

    symbol_obj = Symbol.objects.get(
        name=symbol,
        exchange__name=exchange,
        contract_type__name=contract_type,
    )

    query = f"""
        WITH ranked_data AS (
            SELECT
                id,
                start_time,
                open,
                close,
                symbol_id,
                ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('{group_by}', start_time), symbol_id ORDER BY start_time) AS row_asc,
                ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('{group_by}', start_time), symbol_id ORDER BY start_time DESC) AS row_desc
            FROM
                "cs_klines_1m"
            WHERE
                symbol_id = %s
                AND start_time >= %s
        )
        SELECT
            MAX(id) as id,
            EXTRACT({settings["extract_function"]} FROM DATE_TRUNC('{group_by}', start_time)) AS {settings["column_name"]},
            MAX(CASE WHEN row_asc = 1 THEN open END) AS open,
            MAX(CASE WHEN row_desc = 1 THEN close END) AS close
        FROM
            ranked_data
        GROUP BY
            DATE_TRUNC('{group_by}', start_time)
        ORDER BY
            DATE_TRUNC('{group_by}', start_time)
    """

    return list(Kline1m.objects.raw(query, [symbol_obj.pk, start_time_utc]))
