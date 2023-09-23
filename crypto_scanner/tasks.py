from __future__ import absolute_import, unicode_literals
from django.core.cache import cache
from celery import shared_task
from binance.client import Client

import time

from crypto_scanner.constants import (
    tickers,
    stats_select_options_htf,
    stats_select_options_ltf,
)
from crypto_scanner.api import (
    calculate_pearson_correlation,
    calculate_z_score,
    calculate_z_score_history,
)
from crypto_scanner.utils import create_kline_object, get_interval_model

client = Client()


@shared_task
def calculate_options_z_score(calculate_ltf=False):
    if calculate_ltf:
        time.sleep(70)
        durations = stats_select_options_ltf
    else:
        durations = stats_select_options_htf
        time.sleep(10)

    for duration in durations:
        response = calculate_z_score(duration)

        cache.set(f"z_score_{duration}", response)


@shared_task
def calculate_options_pearson_correlation(calculate_ltf=False):
    if calculate_ltf:
        durations = stats_select_options_ltf
        time.sleep(80)
    else:
        durations = stats_select_options_htf
        time.sleep(20)

    for duration in durations.keys():
        response = calculate_pearson_correlation(duration)

        cache.set(f"pearson_correlation_{duration}", response)


@shared_task
def calculate_z_score_history():
    time.sleep(16)
    duration = "1d"

    response = calculate_z_score_history(duration)

    cache.set(f"z_score_data_{duration}", response)


@shared_task
def fetch_all_klines(tf, limit=25):
    model, interval = get_interval_model(tf)

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
