from __future__ import absolute_import, unicode_literals
from django.utils import timezone
from crypto_scanner.models import BinanceSpotKline1m, BinanceSpotKline5m
from celery import shared_task
from binance.client import Client
from datetime import datetime

import os
import time
import redis

from crypto_scanner.constants import tickers, stats_select_options
from crypto_scanner.utils import format_options
from crypto_scanner.views import pearson_correlation

client = Client()

r = redis.Redis(host="localhost", port=6379, db=0)


def get_interval_model(tf):
    if tf == "1m":
        return BinanceSpotKline1m, Client.KLINE_INTERVAL_1MINUTE
    elif tf == "5m":
        return BinanceSpotKline5m, Client.KLINE_INTERVAL_5MINUTE


@shared_task
def calculate_all_options_pearson_correlation():
    for duration in stats_select_options.keys():
        response = pearson_correlation(duration)

        r.set(f"pearson_correlation_{duration}", response)


def test_redis():
    r.set("testing", "otorinolaringologija")


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


def populate_all_klines(tf, start_date, end_date=None, batch=40000):
    if end_date is None:
        end_date = datetime.now()

    for ticker in tickers:
        populate_kline(tf, ticker, start_date, end_date, batch)


def populate_kline(tf, ticker, start_date, end_date=None, batch=40000):
    if end_date is None:
        end_date = datetime.now()

    model, interval = get_interval_model(tf)

    klines = client.get_historical_klines(ticker, interval, start_date, end_date)

    kline_objects = []
    for kline in klines:
        kline_object = create_kline_object(model, ticker, kline, True)
        if kline_object:
            kline_objects.append(kline_object)

    if kline_objects:
        for i in range(0, len(kline_objects), batch):
            model.objects.bulk_create(kline_objects[i : i + batch])


def create_kline_object(model, ticker, kline, check_exists=False):
    start_time = timezone.make_aware(
        datetime.fromtimestamp(kline[0] / 1000), timezone.utc
    )

    if check_exists:
        if model.objects.filter(ticker=ticker, start_time=start_time):
            return None

    end_time = timezone.make_aware(
        datetime.fromtimestamp(kline[6] / 1000), timezone.utc
    )

    kline_obj = model(
        ticker=ticker,
        start_time=start_time,
        end_time=end_time,
        open=kline[1],
        close=kline[4],
        high=kline[2],
        low=kline[3],
        base_volume=kline[5],
        number_of_trades=kline[8],
        quote_asset_volume=kline[7],
        taker_buy_base_asset_volume=kline[9],
        taker_buy_quote_asset_volume=kline[10],
    )

    return kline_obj
