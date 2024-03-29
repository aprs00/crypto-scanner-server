from django.http import HttpResponse, JsonResponse
from django.views.decorators.csrf import csrf_exempt


from crypto_scanner.constants import (
    stats_select_options_htf,
    stats_select_options_ltf,
    stats_select_options_all,
    tickers,
    large_pearson_types,
    large_pearson_timeframes,
)
from crypto_scanner.utils import format_options


@csrf_exempt
def get_tickers_options(request):
    if request.method == "GET":
        response = format_options(tickers, "list")

        return JsonResponse(response, safe=False)

    return HttpResponse(status=405)


@csrf_exempt
def get_stats_select_options(request):
    if request.method == "GET":
        response = {
            "htf": format_options(stats_select_options_htf),
            "ltf": format_options(stats_select_options_ltf),
            "all": format_options(stats_select_options_all),
        }

        return JsonResponse(response, safe=False)

    # Other HTTP methods are not allowed for this view
    return HttpResponse(status=405)


@csrf_exempt
def get_large_pearson_types(request):
    if request.method == "GET":
        response = format_options(large_pearson_types, "list", True)

        return JsonResponse(response, safe=False)

    return HttpResponse(status=405)


@csrf_exempt
def get_large_pearson_timeframes(request):
    if request.method == "GET":
        response = format_options(large_pearson_timeframes, "list")

        return JsonResponse(response, safe=False)

    return HttpResponse(status=405)
