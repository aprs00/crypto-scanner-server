from django.http import HttpResponse, JsonResponse
from django.core.cache import cache
from django.views.decorators.csrf import csrf_exempt

import redis
import msgpack


from crypto_scanner.constants import (
    test_socket_symbols,
    invalid_params_error,
    redis_ts_data_types,
    large_correlations_timeframes,
    large_correlation_types,
)
from crypto_scanner.services.correlations import (
    calculate_pearson_correlation_high_tf,
)


r = redis.Redis(host="redis", port=6379)


@csrf_exempt
def get_large_pearson_correlation(request):
    if request.method != "GET":
        return HttpResponse(status=405)

    tf = request.GET.get("duration", None)
    data_type = request.GET.get("type", None)

    if tf not in large_correlations_timeframes or data_type not in redis_ts_data_types:
        return JsonResponse(invalid_params_error, status=400)

    pearson_correlations = msgpack.unpackb(
        r.execute_command("GET", f"pearson_correlation_large_{data_type}_{tf}")
    )

    spearman_correlations = (
        msgpack.unpackb(
            r.execute_command("GET", f"spearman_correlation_large_{data_type}_{tf}")
        )
        if "spearman" in large_correlation_types
        else []
    )

    formatted_tickers = [ticker[:-4] for ticker in test_socket_symbols]

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

    tf = request.GET.get("duration", None)

    if tf is None:
        return JsonResponse(invalid_params_error, status=400)

    response = cache.get(f"pearson_correlation_{tf}")

    if response is None:
        response = calculate_pearson_correlation_high_tf(tf)
        cache.set(f"pearson_correlation_{tf}", response)

    return JsonResponse(response, safe=False)
