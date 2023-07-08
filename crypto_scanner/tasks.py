from __future__ import absolute_import, unicode_literals

from celery import shared_task
import redis
import pickle

import numpy as np
from binance.client import Client

client = Client()

r = redis.Redis(host="localhost", port=6379, db=0)


# @shared_task
# def run_every_15_seconds():
#     btc_price = client.get_symbol_ticker(symbol="BTCUSDT")
#     btc_price = float(btc_price["price"])
#     print(f"Current BTC price: {btc_price}")
#     return "SUCCESSFUL RUN EVERY 15 SECONDS"


def something():
    # fetch all available 1 minute klines of BTCUSDT market
    klines = client.get_historical_klines(
        "BTCUSDT", Client.KLINE_INTERVAL_1MINUTE, "1 day ago UTC"
    )
    print(len(klines))
    print("fewfe32fq3f3qfq3fq33")
