from django.http import HttpResponse, JsonResponse
from django.core.cache import cache
from django.views.decorators.csrf import csrf_exempt
from django.db.models import FloatField, F
from django.db.models.functions import Cast
from django.utils import timezone
from datetime import timedelta

import numpy as np

from crypto_scanner.constants import (
    stats_select_options_all,
    tickers,
    invalid_params_error,
)

from crypto_scanner.models import BinanceSpotKline5m

from crypto_scanner.api.utils import get_min_length


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
    print(len(query_tickers_data["BTCUSDT"]))

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
