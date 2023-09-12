from django.http import HttpResponse, JsonResponse
from django.core.cache import cache
from django.views.decorators.csrf import csrf_exempt
from django.db.models import Avg, F, FloatField
from django.db.models.functions import ExtractWeekDay, Cast
from django.utils import timezone

from crypto_scanner.models import BinanceSpotKline5m
from datetime import timedelta

import numpy as np

from crypto_scanner.constants import stats_select_options, tickers
from crypto_scanner.utils import format_options


@csrf_exempt
def average_price_change_per_day_of_week(request, symbol, duration):
    if request.method == "GET":
        # Calculate the start date of the week
        current_date = timezone.now()
        current_day_of_week = timezone.now().weekday()
        start_of_week = current_date - timedelta(days=current_date.weekday())
        num_of_days_select_options = stats_select_options[duration]

        if num_of_days_select_options is None:
            return JsonResponse(
                {"error": "Invalid duration", "code": "INVALID_DURATION"}, status=400
            )

        # Calculate the date 'duration + 1' days ago from the start of the week to exclude Monday
        days_ago = start_of_week - timedelta(days=num_of_days_select_options + 1)

        # Group the 5-minute kline candles per day of the week and calculate average price movements
        average_price_changes = (
            BinanceSpotKline5m.objects.filter(ticker=symbol, start_time__gte=days_ago)
            .annotate(day_of_week=ExtractWeekDay("start_time"))
            .values("day_of_week")
            .annotate(average_price_movement=Avg(F("close") - F("open")))
        )

        # Convert Decimal objects to floats for JSON serialization
        for item in average_price_changes:
            item["average_price_movement"] = float(item["average_price_movement"])

        response = {
            "xAxis": ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"],
            "data": [
                {
                    "value": round(item["average_price_movement"], 2),
                    "itemStyle": {
                        "color": "#a90000"
                        # if current_day_of_week == item["day_of_week"]
                        if item["average_price_movement"] < 0
                        else "#00a900"
                    },
                }
                for item in average_price_changes
            ],
        }

        return JsonResponse(response, safe=False)

    return HttpResponse(status=405)


def calculate_pearson_correlation(x, y):
    return np.corrcoef(x, y)[0, 1]


def calculate_percentage_changes(data):
    # Calculate percentage changes between each k-line
    percentage_changes = [
        (data[i] - data[i - 1]) / data[i - 1] * 100 for i in range(1, len(data))
    ]
    return percentage_changes


def calculate_correlation_between_coins(coin1, coin2, duration):
    end_date = timezone.now()
    start_date = end_date - timedelta(days=duration)

    data_coin1 = (
        BinanceSpotKline5m.objects.filter(
            ticker=coin1, start_time__gte=start_date, start_time__lte=end_date
        )
        .annotate(close_as_float=Cast("close", FloatField()))
        .values_list("close_as_float", flat=True)
    )

    data_coin2 = (
        BinanceSpotKline5m.objects.filter(
            ticker=coin2, start_time__gte=start_date, start_time__lte=end_date
        )
        .annotate(close_as_float=Cast("close", FloatField()))
        .values_list("close_as_float", flat=True)
    )

    min_length = min(len(data_coin1), len(data_coin2))
    data_coin1 = data_coin1[:min_length]
    data_coin2 = data_coin2[:min_length]

    percentage_changes_coin1 = calculate_percentage_changes(data_coin1)
    percentage_changes_coin2 = calculate_percentage_changes(data_coin2)

    # Calculate the Pearson correlation between the two coins
    correlation = calculate_pearson_correlation(
        percentage_changes_coin1, percentage_changes_coin2
    )

    return correlation


def format_pearson_correlation_response(duration):
    correlation_results = {}
    duration = stats_select_options[duration]

    for i in range(len(tickers)):
        for j in range(i + 1, len(tickers)):
            coin1 = tickers[i]
            coin2 = tickers[j]

            correlation = calculate_correlation_between_coins(coin1, coin2, duration)

            correlation_results[f"{coin1} - {coin2}"] = correlation

    tickers = [ticker[:-4] for ticker in tickers]

    response = {
        "xAxis": tickers,
        "yAxis": tickers,
        "data": [
            [i, j, round(correlation_results[f"{tickers[i]} - {tickers[j]}"], 2)]
            for i in range(len(tickers))
            for j in range(i + 1, len(tickers))
        ],
    }

    return response


@csrf_exempt
def get_pearson_correlation(request, duration):
    if request.method == "GET":
        print("duration", duration)
        response = cache.get(f"pearson_correlation_{duration}")
        if response is None:
            response = format_pearson_correlation_response(duration)
            cache.set(f"pearson_correlation_{duration}", response)

        return JsonResponse(response, safe=False)

    # Other HTTP methods are not allowed for this view
    return HttpResponse(status=405)


@csrf_exempt
def get_stats_select_options(request):
    if request.method == "GET":
        return JsonResponse(format_options(stats_select_options), safe=False)

    # Other HTTP methods are not allowed for this view
    return HttpResponse(status=405)
