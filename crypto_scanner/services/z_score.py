from django.core.cache import cache
from django.utils import timezone
import redis
import time
import os

from crypto_scanner.models import ZScoreHistorical, Ticker
from crypto_scanner.selectors.z_score import get_tickers_data_z_score
from crypto_scanner.formulas.z_score import (
    calculate_current_z_score,
    calculate_z_scores,
)
from crypto_scanner.constants import (
    ticker_colors,
    test_socket_symbols,
    large_correlations_timeframes,
    redis_ts_data_types,
)

r = redis.Redis(host="redis", port=6379, decode_responses=True)


def calculate_z_score_matrix(duration):
    tickers_data_z_scores = get_tickers_data_z_score(duration)
    z_scores = {}

    for ticker, data in tickers_data_z_scores.items():
        volume_values, price_values, trades_values, _ = zip(*data)

        z_scores[ticker] = {
            "volume": calculate_current_z_score(volume_values),
            "price": calculate_current_z_score(price_values),
            "trades": calculate_current_z_score(trades_values),
        }

    return z_scores


def calculate_large_z_score_matrix():
    for tf in large_correlations_timeframes:
        current_time_ms = int(time.time() * 1000)
        parsed_tf = int(tf[:-1])

        ago_ms = current_time_ms - parsed_tf * 60 * 1000

        z_scores = {}
        z_scores_to_insert = []

        for symbol in test_socket_symbols:
            z_scores[symbol] = {}

            for type in redis_ts_data_types:
                redis_data = r.execute_command(
                    f"TS.RANGE 1s:{type}:{symbol} {ago_ms} +"
                )
                data = [float(x[1]) for x in redis_data]

                z_scores[symbol][type] = calculate_current_z_score(data)

        current_time = timezone.now()

        if tf == "15m":
            for symbol, val in z_scores.items():
                base = symbol[:-4]
                quote = symbol[-4:]

                base_ticker, created = Ticker.objects.get_or_create(name=base)
                quote_ticker, created = Ticker.objects.get_or_create(name=quote)

                z_scores_to_insert.append(
                    ZScoreHistorical(
                        ticker_name=base_ticker,
                        ticker_quote=quote_ticker,
                        volume_z_score=val["volume"],
                        price_z_score=val["price"],
                        trades_z_score=val["trades"],
                        calculated_at=current_time,
                    )
                )

        if os.getenv("MODE") != "dev":
            ZScoreHistorical.objects.bulk_create(z_scores_to_insert)

        cache.set(f"z_score_matrix_large_{tf}", z_scores)


def format_z_score_matrix_response(data, tickers, x_axis, y_axis, round_by):
    return [
        {
            "type": "scatter",
            "name": ticker,
            "data": [
                [
                    round(data[ticker][x_axis], round_by),
                    round(data[ticker][y_axis], round_by),
                ]
            ],
            "color": ticker_colors[i],
            "symbolSize": 20,
            "emphasis": {"scale": 1.6},
        }
        for i, ticker in enumerate(tickers)
    ]


def calculate_z_score_history(duration):
    tickers_data_z_score = get_tickers_data_z_score(duration)
    z_scores = {}
    start_time_values = None

    for ticker, data in tickers_data_z_score.items():
        volume_values, price_values, trades_values, start_time_values = zip(*data)

        z_scores[ticker] = {
            "volume": calculate_z_scores(volume_values),
            "price": calculate_z_scores(price_values),
            "trades": calculate_z_scores(trades_values),
        }

    return {"data": z_scores, "start_time_values": start_time_values}


def format_z_score_history_response(data, data_type):
    return {
        "data": [
            {
                "name": ticker,
                "type": "line",
                "data": [float(item) for item in data[data_type]],
                "emphasis": {"focus": "self"},
            }
            for ticker, data in data["data"].items()
        ],
        "time": [item.strftime("%H:%M") for item in data["start_time_values"]],
    }
