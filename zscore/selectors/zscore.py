from django.db.models import F, CharField, Func, Value
from zscore.models import ZScoreHistorical

from django.utils import timezone
from datetime import timedelta

import redis

from exchange_connections.models import BinanceSpotKline5m

from utils.lists import get_min_length
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


def get_all_tickers_data_z_score(duration, type):
    now = timezone.now()
    last_24_hours = now - timezone.timedelta(hours=duration)

    type_mapper = {
        "price": "price_z_score",
        "volume": "volume_z_score",
        "trades": "trades_z_score",
    }

    z_score_data = (
        ZScoreHistorical.objects.select_related("ticker_name", "ticker_quote")
        .filter(calculated_at__gte=last_24_hours)
        .annotate(
            time_string=Func(
                F("calculated_at"),
                Value("HH24:MI:SS"),
                function="to_char",
                output_field=CharField(),
            )
        )
        .values(
            base=F("ticker_name__name"),
            quote=F("ticker_quote__name"),
            time=F("time_string"),
            z_score=F(type_mapper[type]),
            # price=F("price_z_score"),
            # volume=F("volume_z_score"),
            # trades=F("trades_z_score"),
        )
        .order_by("calculated_at")
    )

    return list(z_score_data)
