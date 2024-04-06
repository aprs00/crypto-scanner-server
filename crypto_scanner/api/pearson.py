from django.http import HttpResponse, JsonResponse
from django.core.cache import cache
from django.views.decorators.csrf import csrf_exempt
from django.db.models import FloatField, F
from django.db.models.functions import Cast
from django.utils import timezone
from datetime import timedelta
import redis
import time

import numpy as np

from crypto_scanner.constants import test_socket_symbols


r = redis.Redis(host="redis", port=6379, decode_responses=True)

from crypto_scanner.constants import (
    stats_select_options_all,
    tickers,
    invalid_params_error,
    large_pearson_types,
    large_pearson_timeframes,
)

from crypto_scanner.models import BinanceSpotKline5m

from crypto_scanner.api.utils import get_min_length


def calculate_large_pearson_correlation(tf, type):
    current_time_ms = int(time.time() * 1000)
    tf = int(tf[:-1])

    ago_ms = current_time_ms - tf * 60 * 1000
    data = {}

    price_data_to_skip = 4

    if tf == 15:
        price_data_to_skip = 14

    for symbol in test_socket_symbols:
        redis_data = r.execute_command(f"TS.RANGE 1s:{type}:{symbol} {ago_ms} +")
        price_data = [float(x[1]) for x in redis_data][::price_data_to_skip]

        data[symbol] = price_data

    correlations = {}

    for symbol1 in test_socket_symbols:
        for symbol2 in test_socket_symbols:
            correlation_coefficient = np.corrcoef(data[symbol1], data[symbol2])[0, 1]
            correlations[f"{symbol1} - {symbol2}"] = correlation_coefficient

    formatted_tickers = [ticker[:-4] for ticker in test_socket_symbols]

    response = {
        "xAxis": formatted_tickers,
        "yAxis": formatted_tickers,
        "data": [
            [
                i,
                j,
                round(
                    correlations[
                        f"{test_socket_symbols[i]} - {test_socket_symbols[j]}"
                    ],
                    2,
                ),
            ]
            for i in range(len(test_socket_symbols))
            for j in range(i + 1, len(test_socket_symbols))
        ],
    }

    return response


def get_tickers_data(duration, nth_element=1):
    query_tickers_data = {}
    duration_hours = stats_select_options_all[duration]

    end_time = timezone.now()
    start_time = end_time - timedelta(hours=duration_hours)

    start_time_utc = start_time.astimezone(timezone.utc)
    end_time_utc = end_time.astimezone(timezone.utc)

    for ticker in tickers:
        query_tickers_data[ticker] = (
            BinanceSpotKline5m.objects.filter(
                ticker=ticker,
                start_time__gte=start_time_utc,
                start_time__lte=end_time_utc,
            )
            .annotate(close_as_float=Cast("close", FloatField()))
            .values_list("close_as_float", flat=True)
            .order_by("start_time")
        )[::nth_element]

    query_tickers_data = get_min_length(query_tickers_data)

    return query_tickers_data


def calculate_pearson_correlation(duration):
    correlation_results = {}
    every_x_elements = 1

    if duration == "1w":
        every_x_elements = 7
    if duration == "2w":
        every_x_elements = 14
    elif duration == "1m":
        every_x_elements = 32
    elif duration == "3m":
        every_x_elements = 64
    elif duration == "6m":
        every_x_elements = 120

    query_tickers_data = get_tickers_data(duration, nth_element=every_x_elements)

    for ticker1 in tickers:
        for ticker2 in tickers:
            correlation_coefficient = np.corrcoef(
                query_tickers_data[ticker1], query_tickers_data[ticker2]
            )[0, 1]

            correlation_results[f"{ticker1} - {ticker2}"] = correlation_coefficient

    formatted_tickers = [ticker[:-4] for ticker in tickers]

    response = {
        "xAxis": formatted_tickers,
        "yAxis": formatted_tickers,
        "data": [
            [i, j, round(correlation_results[f"{tickers[i]} - {tickers[j]}"], 2)]
            for i in range(len(tickers))
            for j in range(i + 1, len(tickers))
        ],
    }

    return response


@csrf_exempt
def get_large_pearson_correlation(request):
    if request.method != "GET":
        return HttpResponse(status=405)

    tf = request.GET.get("duration", None)
    type = request.GET.get("type", None)

    if tf not in large_pearson_timeframes or type not in large_pearson_types:
        return JsonResponse(invalid_params_error, status=400)

    response = cache.get(f"pearson_correlation_large_{type}_{tf}")

    if response is None:
        response = calculate_large_pearson_correlation(tf, type)
        cache.set(f"pearson_correlation_large_{type}_{tf}", response)

    return JsonResponse(response, safe=False)


@csrf_exempt
def get_pearson_correlation(request):
    if request.method == "GET":
        duration = request.GET.get("duration", None)

        if duration is None:
            return JsonResponse(invalid_params_error, status=400)

        response = cache.get(f"pearson_correlation_{duration}")

        if response is None:
            response = calculate_pearson_correlation(duration)
            cache.set(f"pearson_correlation_{duration}", response)

        return JsonResponse(response, safe=False)

    # Other HTTP methods are not allowed for this view
    return HttpResponse(status=405)
