from __future__ import absolute_import, unicode_literals
from django.utils import timezone
from crypto_scanner.models import (
    BinanceSpotKline1m,
)
from celery import shared_task
from binance.client import Client
from datetime import datetime

import redis

client = Client()

r = redis.Redis(host="localhost", port=6379, db=0)


tickers = [
    "BTCUSDT",
    "ETHUSDT",
    "XRPUSDT",
    "BNBUSDT",
    "SOLUSDT",
    "DOTUSDT",
    "DOGEUSDT",
    "LTCUSDT",
    "LINKUSDT",
    "BCHUSDT",
    "MATICUSDT",
    "AVAXUSDT",
    "SHIBUSDT",
]


@shared_task
def fetch_all_1m_klines(limit=25):
    for ticker in tickers:
        klines = client.get_klines(
            symbol=ticker, interval=Client.KLINE_INTERVAL_1MINUTE, limit=limit
        )

        kline_objects = []
        for kline in klines:
            kline_object = create_kline_object(ticker, kline)
            if kline_object:
                kline_objects.append(kline_object)

        if kline_objects:
            for i in range(0, len(kline_objects), 50000):
                BinanceSpotKline1m.objects.bulk_create(kline_objects[i : i + 100])


def populate_all_klines_date(date):
    for ticker in tickers:
        populate_kline(ticker, date)


def populate_kline(ticker, date):
    klines = client.get_historical_klines(ticker, Client.KLINE_INTERVAL_1MINUTE, date)

    kline_objects = []
    for kline in klines:
        kline_object = create_kline_object(ticker, kline)
        if kline_object:
            kline_objects.append(kline_object)

    if kline_objects:
        BinanceSpotKline1m.objects.bulk_create(kline_objects)


def create_kline_object(ticker, kline):
    start_time = timezone.make_aware(
        datetime.fromtimestamp(kline[0] / 1000), timezone.utc
    )

    if BinanceSpotKline1m.objects.filter(ticker=ticker, start_time=start_time).exists():
        return None

    end_time = timezone.make_aware(
        datetime.fromtimestamp(kline[6] / 1000), timezone.utc
    )

    kline_obj = BinanceSpotKline1m(
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
