from django.db.models import F, CharField, Func, Value
from zscore.models import ZScoreHistory
from django.utils import timezone
from datetime import timedelta
import redis

from exchange_connections.models import Kline1m
from utils.lists import get_min_length
from filters.constants import tf_options
from exchange_connections.constants import tickers

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
    last_hours = timezone.now() - timezone.timedelta(hours=hours)

    zscore_data = (
        ZScoreHistory.objects.select_related("symbol")
        .filter(calculated_at__gte=last_hours)
        .annotate(
            time=Func(
                F("calculated_at"),
                Value("HH24:MI:SS"),
                function="to_char",
                output_field=CharField(),
            )
        )
        .values(
            "price",
            "volume",
            "trades",
            "time",
            "hours",
            "symbol__name",
        )
        .order_by("calculated_at")
    )

    return list(zscore_data)
