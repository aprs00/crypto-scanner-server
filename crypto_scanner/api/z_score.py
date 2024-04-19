from django.http import HttpResponse, JsonResponse
from django.core.cache import cache
from django.views.decorators.csrf import csrf_exempt
from django.utils import timezone
import redis
import time

from crypto_scanner.models import BinanceSpotKline5m

from datetime import timedelta

import numpy as np

from crypto_scanner.api.utils import get_min_length
from crypto_scanner.constants import (
    stats_select_options_all,
    tickers,
    ticker_colors,
    invalid_params_error,
    test_socket_symbols,
    large_correlations_timeframes,
    large_correlation_data_types,
)

r = redis.Redis(host="redis", port=6379, decode_responses=True)


def get_tickers_data_z_score(duration, nth_element=1):
    duration_hours = stats_select_options_all[duration]

    end_time = timezone.now()
    start_time = end_time - timedelta(hours=duration_hours)

    start_time_utc = start_time.astimezone(timezone.utc)
    end_time_utc = end_time.astimezone(timezone.utc)

    # tickers_data = BinanceSpotKline5m.objects.filter(
    #     ticker__in=tickers, start_time__gte=start_time_utc, start_time__lte=end_time_utc
    # ).values("ticker", "base_volume", "close", "number_of_trades")

    # print(tickers_data)

    # trades_volume_price_tickers_data = {}
    # for data in tickers_data:
    #     ticker = data["ticker"]
    #     if ticker not in trades_volume_price_tickers_data:
    #         trades_volume_price_tickers_data[ticker] = []
    #     trades_volume_price_tickers_data[ticker].append(
    #         (data["base_volume"], data["close"], data["number_of_trades"])
    #     )

    trades_volume_price_tickers_data = {}

    for ticker in tickers:
        trades_volume_price_tickers_data[ticker] = (
            BinanceSpotKline5m.objects.filter(
                ticker=ticker,
                start_time__gte=start_time_utc,
                start_time__lte=end_time_utc,
            )
            .values_list("base_volume", "close", "number_of_trades", "start_time")
            .order_by("start_time")
        )[::nth_element]

        # print(trades_volume_price_tickers_data[ticker].query)

    trades_volume_price_tickers_data = get_min_length(trades_volume_price_tickers_data)

    return trades_volume_price_tickers_data


def calculate_current_z_score(data):
    mean = np.mean(data)
    std_dev = np.std(data)
    return (data[-1] - mean) / std_dev


def calculate_z_scores(values):
    mean_value = np.mean(values)
    std_dev_value = np.std(values)
    return [(item - mean_value) / std_dev_value for item in values]


def calculate_z_score_matrix(duration):
    every_x_elements = 1

    if duration == "1w":
        every_x_elements = 10
    if duration == "2w":
        every_x_elements = 20
    elif duration == "1m":
        every_x_elements = 50
    elif duration == "3m":
        every_x_elements = 150
    elif duration == "6m":
        every_x_elements = 400

    tickers_data_z_scores = get_tickers_data_z_score(duration, every_x_elements)
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

        data_to_skip = 4

        if parsed_tf == 15:
            data_to_skip = 14

        z_scores = {}

        for symbol in test_socket_symbols:
            z_scores[symbol] = {}

            for type in large_correlation_data_types:
                redis_data = r.execute_command(
                    f"TS.RANGE 1s:{type}:{symbol} {ago_ms} +"
                )
                skipped_data = [float(x[1]) for x in redis_data][::data_to_skip]

                z_scores[symbol][type] = calculate_current_z_score(skipped_data)

        cache.set(f"z_score_matrix_large_{tf}", z_scores)


def format_z_score_matrix_response(data, tickers, xAxis, yAxis, roundBy):
    return [
        {
            "type": "scatter",
            "name": ticker,
            "data": [
                [
                    round(data[ticker][xAxis], roundBy),
                    round(data[ticker][yAxis], roundBy),
                ]
            ],
            "color": ticker_colors[i],
            "symbolSize": 20,
            "emphasis": {"scale": 1.6},
        }
        for i, ticker in enumerate(tickers)
    ]


@csrf_exempt
def get_large_z_score_matrix(request):
    if request.method != "GET":
        return HttpResponse(status=405)

    xAxis = request.GET.get("x_axis", None)
    yAxis = request.GET.get("y_axis", None)
    tf = request.GET.get("tf", None)

    if xAxis is None or yAxis is None or tf is None:
        return JsonResponse(invalid_params_error, status=400)

    response = cache.get(f"z_score_matrix_large_{tf}")

    if response is None:
        calculate_large_z_score_matrix()
        response = cache.get(f"z_score_matrix_large_{tf}")

    response = format_z_score_matrix_response(
        response, test_socket_symbols, xAxis, yAxis, 2
    )

    return JsonResponse(response, safe=False)


@csrf_exempt
def get_z_score_matrix(request):
    if request.method == "GET":
        xAxis = request.GET.get("x_axis", None)
        yAxis = request.GET.get("y_axis", None)
        duration = request.GET.get("duration", None)

        if xAxis is None or yAxis is None or duration is None:
            return JsonResponse(
                {"error": "Invalid axis", "code": "INVALID_AXIS"}, status=400
            )

        response = cache.get(f"z_score_{duration}")

        if response is None:
            response = calculate_z_score_matrix(duration)
            cache.set(f"z_score_{duration}", response)

        response = format_z_score_matrix_response(response, tickers, xAxis, yAxis, 2)

        return JsonResponse(response, safe=False)

    return HttpResponse(status=405)


def calculate_z_score_history(duration):
    tickers_data_z_score = get_tickers_data_z_score(duration)
    z_scores = {}

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


@csrf_exempt
def get_z_score_history(request):
    if request.method == "GET":
        duration = request.GET.get("duration", None)
        type = request.GET.get("type", None)

        if duration is None or type is None:
            return JsonResponse(invalid_params_error, status=400)

        response = cache.get(f"z_score_history_{duration}")

        if response is None:
            response = calculate_z_score_history(duration)
            cache.set(f"z_score_history_{duration}", response)

        formatted_response = format_z_score_history_response(response, type)

        response = {
            "legend": tickers,
            "data": formatted_response["data"],
            "xAxis": formatted_response["time"],
        }

        return JsonResponse(response, safe=False)

    return HttpResponse(status=405)
