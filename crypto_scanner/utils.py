from django.utils import timezone
from binance.client import Client
from datetime import datetime

import time

from crypto_scanner.constants import tickers

# from crypto_scanner.models import BinanceSpotKline5m
BinanceSpotKline5m = None


# client = Client()
client = None


def format_options(options, type="dict"):
    if type == "dict":
        return [{"value": k, "label": k} for k, _ in options.items()]
    elif type == "list":
        return [{"value": i, "label": i} for i in options]


def populate_all_klines(tf, start_date, end_date=None, batch=40000):
    if end_date is None:
        end_date = datetime.now()

    for ticker in tickers:
        populate_kline(tf, ticker, start_date, end_date, batch)
        time.sleep(1800)


def populate_kline(tf, ticker, start_date, end_date=None, batch=40000):
    if end_date is None:
        end_date = datetime.now()

    model = BinanceSpotKline5m
    interval = Client.KLINE_INTERVAL_5MINUTE

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

    # if check_exists:
    #     if model.objects.filter(ticker__name=ticker, start_time=start_time).exists():
    #         return None

    end_time = timezone.make_aware(
        datetime.fromtimestamp(kline[6] / 1000), timezone.utc
    )

    # ticker, _ = BinanceSpotTickers.objects.get_or_create(name=ticker)

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
