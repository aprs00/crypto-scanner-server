from django.db.models import FloatField
from django.db.models.functions import Cast
from django.utils import timezone
from datetime import timedelta

import numpy as np

from crypto_scanner.constants import (
    stats_select_options_all,
    tickers,
)
from crypto_scanner.models import BinanceSpotKline5m
from crypto_scanner.utils import get_min_length


def get_tickers_data(duration):
    query_tickers_data = {}
    duration_hours = stats_select_options_all[duration]

    end_time = timezone.now()
    start_time = end_time - timedelta(hours=duration_hours)

    start_time_utc = start_time.astimezone(timezone.utc)
    end_time_utc = end_time.astimezone(timezone.utc)

    for ticker in tickers:
        query_tickers_data[ticker] = (
            BinanceSpotKline5m.objects.filter(
                ticker=ticker,
                start_time__gte=start_time_utc,
                start_time__lte=end_time_utc,
            )
            .annotate(close_as_float=Cast("close", FloatField()))
            .values_list("close_as_float", flat=True)
            .order_by("start_time")
        )

    query_tickers_data = get_min_length(query_tickers_data, tickers)
    query_tickers_data = {k: np.array(v) for k, v in query_tickers_data.items()}

    return query_tickers_data
