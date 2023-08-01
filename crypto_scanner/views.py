from django.http import HttpResponse, JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.db.models import Avg, F, FloatField
from django.db.models.functions import ExtractWeekDay, Cast
from django.utils import timezone

from rest_framework.parsers import JSONParser
from crypto_scanner.models import Snippet, BinanceSpotKline5m
from crypto_scanner.serializers import SnippetSerializer
from datetime import timedelta

import numpy as np
import redis

from crypto_scanner.constants import stats_select_options, tickers
from crypto_scanner.utils import format_options


r = redis.Redis(host="localhost", port=6379, db=0)


@csrf_exempt
def snippet_list(request):
    """
    List all code snippets, or create a new snippet.
    """
    if request.method == "GET":
        snippets = Snippet.objects.all()
        serializer = SnippetSerializer(snippets, many=True)
        return JsonResponse(serializer.data, safe=False)

    elif request.method == "POST":
        data = JSONParser().parse(request)
        serializer = SnippetSerializer(data=data)
        if serializer.is_valid():
            serializer.save()
            return JsonResponse(serializer.data, status=201)
        return JsonResponse(serializer.errors, status=400)


@csrf_exempt
def snippet_detail(request, pk):
    """
    Retrieve, update or delete a code snippet.
    """
    try:
        snippet = Snippet.objects.get(pk=pk)
    except Snippet.DoesNotExist:
        return HttpResponse(status=404)

    if request.method == "GET":
        serializer = SnippetSerializer(snippet)
        return JsonResponse(serializer.data)

    elif request.method == "PUT":
        data = JSONParser().parse(request)
        serializer = SnippetSerializer(snippet, data=data)
        if serializer.is_valid():
            serializer.save()
            return JsonResponse(serializer.data)
        return JsonResponse(serializer.errors, status=400)

    elif request.method == "DELETE":
        snippet.delete()
        return HttpResponse(status=204)


@csrf_exempt
def average_price_change_per_day_of_week(request, symbol, duration):
    if request.method == "GET":
        # Calculate the start date of the week
        current_date = timezone.now()
        start_of_week = current_date - timedelta(days=current_date.weekday())

        # Calculate the date 'duration + 1' days ago from the start of the week to exclude Monday
        days_ago = start_of_week - timedelta(days=stats_select_options[duration] + 1)

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

        return JsonResponse(list(average_price_changes), safe=False)

    # Other HTTP methods are not allowed for this view
    return HttpResponse(status=405)


def calculate_pearson_correlation(x, y):
    print(len(x), len(y))
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


def pearson_correlation(duration):
    # if request.method == "GET":
    correlation_results = {}
    duration = stats_select_options[duration]

    for i in range(len(tickers)):
        for j in range(i + 1, len(tickers)):
            coin1 = tickers[i]
            coin2 = tickers[j]

            correlation = calculate_correlation_between_coins(coin1, coin2, duration)

            correlation_results[f"{coin1} - {coin2}"] = correlation

    response = {
        "xAxes": tickers,
        "yAxes": tickers,
        "data": [
            [i, j, correlation_results[f"{tickers[i]} - {tickers[j]}"]]
            for i in range(len(tickers))
            for j in range(i + 1, len(tickers))
        ],
    }

    return response

    # return JsonResponse(response, safe=False)

    # return HttpResponse(status=405)


def get_pearson_correlation(request, duration):
    if request.method == "GET":
        # return JsonResponse(pearson_correlation(duration), safe=False)
        r.get(f"pearson_correlation_{duration}", pearson_correlation(duration))

    # Other HTTP methods are not allowed for this view
    return HttpResponse(status=405)


@csrf_exempt
def average_price_change_per_day_of_week_select(request):
    if request.method == "GET":
        return JsonResponse(format_options(stats_select_options), safe=False)

    # Other HTTP methods are not allowed for this view
    return HttpResponse(status=405)
