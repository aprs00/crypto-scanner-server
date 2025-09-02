from django.http import HttpResponse, JsonResponse
from django.views.decorators.csrf import csrf_exempt


from filters.constants import tf_options
from exchange_connections.constants import tickers, KLINE_FIELD_MAP
from filters.utils import format_options


@csrf_exempt
def get_tickers_options(request):
    if request.method != "GET":
        return HttpResponse(status=405)

    response = format_options(tickers, "list")

    return JsonResponse(response, safe=False)


@csrf_exempt
def get_stats_select_options(request):
    if request.method != "GET":
        return HttpResponse(status=405)

    result = {
        tf_type: format_options(options) for tf_type, options in tf_options.items()
    }

    return JsonResponse(result, safe=False)


@csrf_exempt
def get_large_pearson_types(request):
    if request.method != "GET":
        return HttpResponse(status=405)

    response = format_options(KLINE_FIELD_MAP.keys(), "list", True)

    return JsonResponse(response, safe=False)
