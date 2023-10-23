from __future__ import absolute_import, unicode_literals
from django.core.cache import cache
from celery import shared_task
from binance.client import Client

import time
import redis

from crypto_scanner.constants import (
    tickers,
    stats_select_options_htf,
    stats_select_options_ltf,
)
from crypto_scanner.api import (
    pearson,
    z_score,
)
from crypto_scanner.utils import create_kline_object

# from crypto_scanner.models import BinanceSpotKline5m
BinanceSpotKline5m = None

# client = Client()
client = None

r = redis.Redis(host="redis", port=6379, decode_responses=True)


@shared_task
def testing_celery():
    print("TESTING CELERY")

    return "TESTING CELERY DONE"


@shared_task
def calculate_options_z_score_matrix(calculate_ltf=False):
    if calculate_ltf:
        time.sleep(72)
        durations = stats_select_options_ltf
    else:
        durations = stats_select_options_htf

    for duration in durations:
        response = z_score.calculate_z_score_matrix(duration)

        cache.set(f"z_score_{duration}", response)

    return "Done"


@shared_task
def calculate_z_score_history():
    print("KALKULIRAMMMM CALCULATE Z SCORE HISTORY")
    time.sleep(88)
    duration = "12h"

    response = z_score.calculate_z_score_history(duration)

    cache.set(f"z_score_history_{duration}", response)

    return "Done"


@shared_task
def calculate_options_pearson_correlation(calculate_ltf=False):
    if calculate_ltf:
        durations = stats_select_options_ltf
        time.sleep(99)
    else:
        durations = stats_select_options_htf
        time.sleep(25)

    for duration in durations.keys():
        response = pearson.calculate_pearson_correlation(duration)

        cache.set(f"pearson_correlation_{duration}", response)

    return "Done"


@shared_task
def fetch_all_klines(tf, limit=25):
    model = BinanceSpotKline5m
    interval = Client.KLINE_INTERVAL_5MINUTE

    for ticker in tickers:
        klines = client.get_klines(symbol=ticker, interval=interval, limit=limit)

        kline_objects = []
        for kline in klines:
            kline_object = create_kline_object(model, ticker, kline, True)
            if kline_object:
                kline_objects.append(kline_object)

        if kline_objects:
            model.objects.bulk_create(kline_objects)

        time.sleep(6)

    print(
        "FETCH ALL KLINES, FETCH ALL KLINES, FETCH ALL KLINES, FETCH ALL KLINES, FETCH ALL KLINES"
    )

    return "Done"


@shared_task
def reconnect_binance_1s_klines_sockets():
    r.publish("binance_1s_data", "reconnect_apis")


# crypto_scanner.tasks.fetch_all_klines("5m", 450)
