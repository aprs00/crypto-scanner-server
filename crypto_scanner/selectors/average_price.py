from crypto_scanner.models import BinanceSpotKline5m


def get_market_data(symbol, start_time_utc, group_by):
    extract_function = None
    column_name = None

    if group_by == "day":
        extract_function = "dow"
        column_name = "day_of_week"
    elif group_by == "hour":
        extract_function = "hour"
        column_name = "hour_of_day"

    query = f"""
        WITH ranked_data AS (
            SELECT
                id,
                start_time,
                open,
                close,
                ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('{group_by}', start_time), ticker ORDER BY start_time) AS row_asc,
                ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('{group_by}', start_time), ticker ORDER BY start_time DESC) AS row_desc
            FROM
                "crypto_scanner_binance_spot_kline_5m"
            WHERE
                ticker = '{symbol}'
                AND start_time >= '{start_time_utc}'
        )
        SELECT
            MAX(id) as id,
            extract({extract_function} from DATE_TRUNC('{group_by}', start_time)) AS {column_name},
            MAX(CASE WHEN row_asc = 1 THEN open END) AS open,
            MAX(CASE WHEN row_desc = 1 THEN close END) AS close
        FROM
            ranked_data
        GROUP BY
            DATE_TRUNC('{group_by}', start_time)
    """

    return BinanceSpotKline5m.objects.raw(query)
