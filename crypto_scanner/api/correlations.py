from django.http import HttpResponse, JsonResponse
from django.core.cache import cache
from django.views.decorators.csrf import csrf_exempt

import redis


from crypto_scanner.constants import (
    test_socket_symbols,
    invalid_params_error,
    large_correlation_data_types,
    large_correlations_timeframes,
)
from crypto_scanner.services.correlations import (
    format_large_pearson_response,
    calculate_pearson_correlation_high_tf,
)


r = redis.Redis(host="redis", port=6379, decode_responses=True)


@csrf_exempt
def get_large_pearson_correlation(request):
    if request.method != "GET":
        return HttpResponse(status=405)

    tf = request.GET.get("duration", None)
    data_type = request.GET.get("type", None)

    if (
        tf not in large_correlations_timeframes
        or data_type not in large_correlation_data_types
    ):
        return JsonResponse(invalid_params_error, status=400)

    formatted_tickers = [ticker[:-4] for ticker in test_socket_symbols]

    pearson_correlations = cache.get(f"pearson_correlation_large_{data_type}_{tf}")
    spearman_correlations = cache.get(f"spearman_correlation_large_{data_type}_{tf}")

    if pearson_correlations is None:
        pearson_correlations = format_large_pearson_response(
            tf,
            data_type,
            "pearson",
            test_socket_symbols,
        )
        cache.set(f"pearson_correlation_large_{data_type}_{tf}", pearson_correlations)

    if spearman_correlations is None:
        spearman_correlations = format_large_pearson_response(
            tf, data_type, "spearman", test_socket_symbols, False
        )
        cache.set(f"spearman_correlation_large_{data_type}_{tf}", spearman_correlations)

    response = {
        "xAxis": formatted_tickers,
        "yAxis": formatted_tickers,
        "data": pearson_correlations + spearman_correlations,
    }

    return JsonResponse(response, safe=False)


@csrf_exempt
def get_pearson_correlation(request):
    if request.method != "GET":
        return HttpResponse(status=405)

    duration = request.GET.get("duration", None)

    if duration is None:
        return JsonResponse(invalid_params_error, status=400)

    response = cache.get(f"pearson_correlation_{duration}")

    if response is None:
        response = calculate_pearson_correlation_high_tf(duration)
        cache.set(f"pearson_correlation_{duration}", response)

    return JsonResponse(response, safe=False)
