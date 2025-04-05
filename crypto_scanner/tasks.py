from __future__ import absolute_import, unicode_literals
from django.core.cache import cache
from celery import shared_task
from binance.client import Client
from django.db import IntegrityError
import os
import time
import redis

from crypto_scanner.constants import (
    tickers,
    stats_select_options_htf,
    stats_select_options_ltf,
)
from crypto_scanner.api import (
    z_score,
)

from crypto_scanner.utils import create_kline_object
from crypto_scanner.models import BinanceSpotKline5m

client = Client()

r = redis.Redis(host="redis")


@shared_task
def calculate_options_z_score_matrix(htf=False):
    time.sleep(30)
    durations = stats_select_options_htf if htf else stats_select_options_ltf

    for duration in durations:
        cache.set(f"z_score_{duration}", z_score.calculate_z_score_matrix(duration))


@shared_task
def calculate_z_score_history(duration="12h"):
    cache.set(
        f"z_score_history_{duration}", z_score.calculate_z_score_history(duration)
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

        if kline_objects and os.getenv("MODE") != "dev":
            try:
                model.objects.bulk_create(kline_objects, ignore_conflicts=True)
            except IntegrityError as e:
                print("IntegrityError:", str(e))
                pass

        time.sleep(2)

    r.publish("klines_fetched", "")
