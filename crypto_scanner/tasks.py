from __future__ import absolute_import, unicode_literals
from django.core.cache import cache
from celery import shared_task
from binance.client import Client
from django.db import IntegrityError

import time
import redis

from crypto_scanner.constants import (
    tickers,
    test_socket_symbols,
    stats_select_options_htf,
    stats_select_options_ltf,
    large_correlations_timeframes,
    redis_ts_data_types,
    large_correlation_types,
)
from crypto_scanner.api import (
    correlations,
    z_score,
)
from crypto_scanner.services.correlations import calculate_pearson_correlation_high_tf
from crypto_scanner.utils import create_kline_object
from crypto_scanner.models import BinanceSpotKline5m

client = Client()

r = redis.Redis(host="redis", port=6379, decode_responses=True)


@shared_task
def calculate_options_z_score_matrix(calculate_ltf=False):
    if calculate_ltf:
        durations = stats_select_options_ltf
    else:
        durations = stats_select_options_htf

    for duration in durations:
        response = z_score.calculate_z_score_matrix(duration)

        cache.set(f"z_score_{duration}", response)


@shared_task
def calculate_options_large_z_score_matrix():
    time.sleep(2)

    z_score.calculate_large_z_score_matrix()


@shared_task
def calculate_z_score_history():
    duration = "12h"

    response = z_score.calculate_z_score_history(duration)

    cache.set(f"z_score_history_{duration}", response)


@shared_task
def calculate_options_pearson_correlation(calculate_ltf=False):
    durations = stats_select_options_ltf if calculate_ltf else stats_select_options_htf

    for duration in durations.keys():
        response = calculate_pearson_correlation_high_tf(duration)

        cache.set(f"pearson_correlation_{duration}", response)


@shared_task
def calculate_all_large_correlations():
    for correlation_type in large_correlation_types:
        for tf in large_correlations_timeframes:
            for data_type in redis_ts_data_types:
                correlation = None

                if correlation_type == "pearson":
                    correlation = correlations.format_large_pearson_response(
                        tf, data_type, correlation_type, test_socket_symbols
                    )
                elif correlation_type == "spearman":
                    correlation = correlations.format_large_pearson_response(
                        tf, data_type, correlation_type, test_socket_symbols, False
                    )

                cache.set(
                    f"{correlation_type}_correlation_large_{data_type}_{tf}",
                    correlation,
                )


@shared_task
def fetch_all_klines(limit=25):
    model = BinanceSpotKline5m
    interval = Client.KLINE_INTERVAL_5MINUTE

    for ticker in tickers:
        klines = client.get_klines(symbol=ticker, interval=interval, limit=limit)

        kline_objects = []
        for kline in klines:
            kline_object = create_kline_object(model, ticker, kline)
            if kline_object:
                kline_objects.append(kline_object)

        if kline_objects:
            try:
                model.objects.bulk_create(kline_objects, ignore_conflicts=True)
            except IntegrityError as e:
                print("IntegrityError:", str(e))
                pass

        time.sleep(2)
