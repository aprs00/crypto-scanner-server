from django.utils import timezone
from datetime import timedelta

import redis

from crypto_scanner.models import BinanceSpotKline5m

from crypto_scanner.utils import get_min_length
from crypto_scanner.constants import (
    stats_select_options_all,
    tickers,
)

r = redis.Redis(host="redis", port=6379, decode_responses=True)


def get_tickers_data_z_score(duration):
    duration_hours = stats_select_options_all[duration]

    end_time = timezone.now()
    start_time = end_time - timedelta(hours=duration_hours)
    start_time_utc = start_time.astimezone(timezone.utc)
    end_time_utc = end_time.astimezone(timezone.utc)

    trades_volume_price_tickers_data = {}

    for ticker in tickers:
        trades_volume_price_tickers_data[ticker] = (
            BinanceSpotKline5m.objects.filter(
                ticker=ticker,
                start_time__gte=start_time_utc,
                start_time__lte=end_time_utc,
            )
            .values_list("base_volume", "close", "number_of_trades", "start_time")
            .order_by("start_time")
        )

    trades_volume_price_tickers_data = get_min_length(
        trades_volume_price_tickers_data, tickers
    )

    return trades_volume_price_tickers_data
