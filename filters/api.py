from django.http import HttpResponse, JsonResponse
from django.views.decorators.csrf import csrf_exempt


from filters.constants import tf_options
from exchange_connections.constants import tickers, KLINE_FIELD_MAP


@csrf_exempt
def get_tickers_options(request):
    if request.method != "GET":
        return HttpResponse(status=405)

    return JsonResponse(tickers, safe=False)


@csrf_exempt
def get_stats_select_options(request):
    if request.method != "GET":
        return HttpResponse(status=405)

    tf_options_str = {
        k: {tk: str(tv) for tk, tv in v.items()} for k, v in tf_options.items()
    }
    return JsonResponse(tf_options_str, safe=False)


@csrf_exempt
def get_large_pearson_types(request):
    if request.method != "GET":
        return HttpResponse(status=405)

    return JsonResponse(list(KLINE_FIELD_MAP.keys()), safe=False)
