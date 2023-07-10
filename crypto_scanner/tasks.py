from __future__ import absolute_import, unicode_literals
from crypto_scanner.models import BtcPrice

from celery import shared_task
import redis
import pickle

import numpy as np
from binance.client import Client

client = Client()

r = redis.Redis(host="localhost", port=6379, db=0)


@shared_task
def run_every_15_seconds():
    btc_price = client.get_symbol_ticker(symbol="BTCUSDT")
    btc_price = float(btc_price["price"])
    btc_price_obj = BtcPrice(price=btc_price)
    btc_price_obj.save()
    return f"Price of BTC is {btc_price}"


def something():
    klines = client.get_historical_klines(
        "BTCUSDT", Client.KLINE_INTERVAL_1MINUTE, "1 day ago UTC"
    )
    print(len(klines))
    print("fewfe32fq3f3qfq3fq33")
