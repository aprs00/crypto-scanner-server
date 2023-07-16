from __future__ import absolute_import, unicode_literals
from django.db.models import Count
from django.utils import timezone
from crypto_scanner.models import (
    BtcKline1m,
    EthKline1m,
    XrpKline1m,
    BnbKline1m,
    SolKline1m,
    AdaKline1m,
    DotKline1m,
    DogeKline1m,
    UniKline1m,
    LtcKline1m,
    LinkKline1m,
    BchKline1m,
    MaticKline1m,
    AvaxKline1m,
    ShibKline1m,
    BtcPrice,
)
from celery import shared_task
from binance.client import Client
from datetime import datetime

import redis

client = Client()

r = redis.Redis(host="localhost", port=6379, db=0)

databaseModels = {
    "BTCUSDT": BtcKline1m,
    "ETHUSDT": EthKline1m,
    "XRPUSDT": XrpKline1m,
    "BNBUSDT": BnbKline1m,
    "SOLUSDT": SolKline1m,
    "ADAUSDT": AdaKline1m,
    "DOTUSDT": DotKline1m,
    "DOGEUSDT": DogeKline1m,
    "UNIUSDT": UniKline1m,
    "LTCUSDT": LtcKline1m,
    "LINKUSDT": LinkKline1m,
    "BCHUSDT": BchKline1m,
    "MATICUSDT": MaticKline1m,
    "AVAXUSDT": AvaxKline1m,
    "SHIBUSDT": ShibKline1m,
}


@shared_task
def fetch_all_1m_klines():
    print("fetching all 1m klines")

    for ticker in databaseModels:
        klines = client.get_klines(
            symbol=ticker, interval=Client.KLINE_INTERVAL_1MINUTE, limit=11
        )

        for kline in klines:
            store_kline(ticker, kline)


def populate_all_klines_date(date):
    for ticker in databaseModels:
        populate_kline(ticker, date)


def populate_kline(ticker, date):
    client = Client()

    klines = client.get_historical_klines(ticker, Client.KLINE_INTERVAL_1MINUTE, date)

    for kline in klines:
        store_kline(ticker, kline)


def store_kline(ticker, kline):
    start_time = timezone.make_aware(
        datetime.fromtimestamp(kline[0] / 1000), timezone.utc
    )

    model = databaseModels[ticker]
    if model.objects.filter(start_time=start_time).exists():
        return

    end_time = timezone.make_aware(
        datetime.fromtimestamp(kline[6] / 1000), timezone.utc
    )

    kline_obj = model(
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

    kline_obj.save()
