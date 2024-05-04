from django.utils import timezone
from datetime import datetime


from crypto_scanner.constants import tickers


def get_min_length(symbols_data, symbols=tickers):
    min_length = min([len(data) for data in symbols_data.values()])
    for symbol in symbols:
        symbols_data[symbol] = symbols_data[symbol][:min_length]

    return symbols_data


def format_options(options, type="dict", label_to_upper=False):
    if type == "dict":
        return [
            {"value": k, "label": k.capitalize() if label_to_upper else k}
            for k, _ in options.items()
        ]
    elif type == "list":
        return [
            {"value": i, "label": i.capitalize() if label_to_upper else i}
            for i in options
        ]


def create_kline_object(model, ticker, kline):
    start_time = timezone.make_aware(
        datetime.fromtimestamp(kline[0] / 1000), timezone.utc
    )

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
