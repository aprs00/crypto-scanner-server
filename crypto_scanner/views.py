from django.http import HttpResponse, JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.db.models import Avg, F
from django.db.models.functions import ExtractWeekDay
from django.utils import timezone

from rest_framework.parsers import JSONParser
from crypto_scanner.models import Snippet, BinanceSpotKline5m
from crypto_scanner.serializers import SnippetSerializer
from datetime import timedelta

from crypto_scanner.constants import avg_price_change_per_week_options
from crypto_scanner.utils import format_options


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
        days_ago = start_of_week - timedelta(
            days=avg_price_change_per_week_options[duration] + 1
        )

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


@csrf_exempt
def average_price_change_per_day_of_week_select(request):
    if request.method == "GET":
        return JsonResponse(
            format_options(avg_price_change_per_week_options), safe=False
        )

    # Other HTTP methods are not allowed for this view
    return HttpResponse(status=405)
