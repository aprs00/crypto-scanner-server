from django.db.models import F, CharField, Func, Value
from zscore.models import ZScoreHistorical
from django.utils import timezone
from datetime import timedelta
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


def get_oldest_kline_for_timeframe(symbol, tf_hours):
    """
    Get the oldest kline (most recent outside the rolling window) that should be removed
    for a specific timeframe. Dynamically applies KLINE_FIELD_MAP for zscore_data_types.
    """
    cutoff_time = timezone.now() - timedelta(hours=tf_hours)

    fields_map = {
        data_type: KLINE_FIELD_MAP[data_type]
        for data_type in zscore_data_types
        if data_type in KLINE_FIELD_MAP
    }

    oldest_kline = (
        Kline1m.objects.filter(symbol__name=symbol, start_time__lte=cutoff_time)
        .values(*fields_map.values())
        .order_by("-start_time")
        .first()
    )

    if oldest_kline:
        return {
            data_type: float(oldest_kline[field_name])
            for data_type, field_name in fields_map.items()
        }

    return None
