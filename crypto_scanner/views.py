from django.http import HttpResponse, JsonResponse
from django.views.decorators.csrf import csrf_exempt
from rest_framework.parsers import JSONParser
from crypto_scanner.models import Snippet, BinanceSpotKline1m
from crypto_scanner.serializers import SnippetSerializer, BinanceSpotKline1mSerializer


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


# return average price of a symbol for a given duration, given duration can be 1m: 1 month, 1w: 1 week, 1m: 1 month, 1y: 1 year
# return average price per day of week, for example, how much does BTCUSDT go up on Mondays, etc...
# example: /average-price/BTCUSDT/1m/
# get data from database
@csrf_exempt
def average_price(request, symbol, duration):
    """
    Retrieve average coin movement for each day of the week.
    """
    try:
        klines = BinanceSpotKline1m.objects.filter(ticker=symbol)
    except BinanceSpotKline1m.DoesNotExist:
        return JsonResponse({"error": "Ticker not found"}, status=404)

    if request.method == "GET":
        serializer = BinanceSpotKline1mSerializer(klines.first())
        return JsonResponse(serializer.data)
