from django.db.models import F, CharField, Func, Value
from zscore.models import ZScoreHistory
from django.utils import timezone
from datetime import timedelta
from django.db import connection
import redis

from exchange_connections.models import Kline1m
from utils.lists import get_min_length
from filters.constants import tf_options
from exchange_connections.constants import tickers, KLINE_FIELD_MAP
from zscore.constants import zscore_data_types

r = redis.Redis(host="redis")


def get_tickers_data_z_score(duration):
    duration_hours = tf_options["zscore"][duration]

    end_time = timezone.now()
    start_time = end_time - timedelta(hours=duration_hours)
    start_time_utc = start_time.astimezone(timezone.utc)
    end_time_utc = end_time.astimezone(timezone.utc)

    trades_volume_price_tickers_data = {}

    for ticker in tickers:
        qs = (
            Kline1m.objects.filter(
                symbol__name=ticker,
                start_time__gte=start_time_utc,
                start_time__lte=end_time_utc,
            )
            .values_list("base_volume", "close", "number_of_trades", "start_time")
            .order_by("start_time")
        )
        trades_volume_price_tickers_data[ticker] = qs

    trades_volume_price_tickers_data = get_min_length(
        trades_volume_price_tickers_data, tickers
    )

    return trades_volume_price_tickers_data


def get_zscore_history_data(hours):
    query = f"""
        SELECT 
            czh.price, 
            czh.volume, 
            czh.trades, 
            czh.hours, 
            to_char(czh.calculated_at, 'HH24:MI:SS') AS "time", 
            css.name AS symbol_name
        FROM cs_zscore_history czh
        INNER JOIN cs_symbols css ON czh.symbol_id = css.id
        WHERE czh.calculated_at >= %s
        ORDER BY czh.calculated_at
    """

    with connection.cursor() as cursor:
        last_hours = timezone.now() - timezone.timedelta(hours=hours)
        cursor.execute(query, [last_hours])
        results = cursor.fetchall()
        columns = [col[0] for col in cursor.description or []]
        results = [dict(zip(columns, row)) for row in results]

    return results
