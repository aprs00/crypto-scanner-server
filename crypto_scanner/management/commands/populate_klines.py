from binance.client import Client
from datetime import datetime
from django.db import IntegrityError

import time

from crypto_scanner.constants import tickers
from crypto_scanner.models import BinanceSpotKline5m
from crypto_scanner.utils import create_kline_object

client = Client()


def populate_all_klines(tf, start_date, end_date=None, batch=40000):
    if end_date is None:
        end_date = datetime.now()

    for ticker in tickers:
        populate_kline(tf, ticker, start_date, end_date, batch)
        time.sleep(10)


def populate_kline(tf, ticker, start_date, end_date=None, batch=40000):
    if end_date is None:
        end_date = datetime.now()

    model = BinanceSpotKline5m
    interval = Client.KLINE_INTERVAL_5MINUTE

    klines = client.get_historical_klines(ticker, interval, start_date, end_date)

    kline_objects = []
    for kline in klines:
        kline_object = create_kline_object(model, ticker, kline)
        if kline_object:
            kline_objects.append(kline_object)

    if kline_objects:
        for i in range(0, len(kline_objects), batch):
            try:
                model.objects.bulk_create(
                    kline_objects[i : i + batch], ignore_conflicts=True
                )
            except IntegrityError as e:
                print("IntegrityError:", str(e))
                pass


# utils.populate_all_klines("5m", "06 Aug 2023", "08 Feb 2024")
# crypto_scanner.tasks.fetch_all_klines(450)
# utils.populate_kline("5m", "SHIBUSDT", "23 Apr 2023", "23 Oct 2023")
(
    crypto_scanner.management.commands.populate_klines.populate_all_klines(
        "5m", "15 Mar 2024", "13 Sep 2024"
    )
)
