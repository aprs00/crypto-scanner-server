from django.http import HttpResponse, JsonResponse
from django.core.cache import cache
from django.views.decorators.csrf import csrf_exempt
from django.db.models import Avg, F, FloatField
from django.db.models.functions import ExtractWeekDay, Cast
from django.utils import timezone

from crypto_scanner.models import BinanceSpotKline5m
from datetime import timedelta

import numpy as np

from crypto_scanner.constants import (
    stats_select_options,
    stats_select_options_all,
    tickers,
)
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
        days_ago = start_of_week - timedelta(hours=num_of_days_select_options + 1)

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
                        "color": "#a50f15"
                        # if current_day_of_week == item["day_of_week"]
                        if item["average_price_movement"] < 0
                        else "#4393c3"
                    },
                }
                for item in average_price_changes
            ],
        }

        return JsonResponse(response, safe=False)

    return HttpResponse(status=405)


# PEARSON CORRELATION
def get_tickers_data(duration):
    query_tickers_data = {}
    duration = stats_select_options_all[duration]

    for ticker in tickers:
        query_tickers_data[ticker] = (
            BinanceSpotKline5m.objects.filter(
                ticker=ticker,
                start_time__gte=timezone.now() - timedelta(days=duration),
            )
            .annotate(close_as_float=Cast("close", FloatField()))
            .values_list("close_as_float", flat=True)
        )

    min_length = min([len(data) for data in query_tickers_data.values()])
    for ticker in tickers:
        query_tickers_data[ticker] = query_tickers_data[ticker][:min_length]

    return query_tickers_data


def get_min_length(query_tickers_data):
    min_length = min([len(data) for data in query_tickers_data.values()])
    for ticker in tickers:
        query_tickers_data[ticker] = query_tickers_data[ticker][:min_length]

    return query_tickers_data


def calculate_pearson_correlation(duration):
    correlation_results = {}

    query_tickers_data = get_tickers_data(duration)
    query_tickers_data = get_min_length(query_tickers_data)

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
def get_pearson_correlation(request, duration):
    if request.method == "GET":
        print("duration", duration)
        response = cache.get(f"pearson_correlation_{duration}")
        if response is None:
            print("response is None: ", response)
            response = calculate_pearson_correlation(duration)
            cache.set(f"pearson_correlation_{duration}", response)

        return JsonResponse(response, safe=False)

    # Other HTTP methods are not allowed for this view
    return HttpResponse(status=405)


@csrf_exempt
def get_stats_select_options(request):
    if request.method == "GET":
        include_ltf = request.GET.get("include_ltf", False)

        if include_ltf:
            combined_options = stats_select_options_all
        else:
            combined_options = stats_select_options

        return JsonResponse(format_options(combined_options), safe=False)

    # Other HTTP methods are not allowed for this view
    return HttpResponse(status=405)
