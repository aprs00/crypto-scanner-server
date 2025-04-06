from django.http import HttpResponse, JsonResponse
from django.views.decorators.csrf import csrf_exempt

import redis
import msgpack


from core.constants import invalid_params_error
from correlations.constants import (
    large_correlations_timeframes,
    large_correlation_types,
)
from filters.constants import stats_select_options_all
from exchange_connections.constants import (
    test_socket_symbols,
    tickers,
    redis_time_series_data_types,
)
from utils.time import convert_timeframe_to_seconds


r = redis.Redis(host="redis")


@csrf_exempt
def get_large_pearson_correlation(request):
    if request.method != "GET":
        return HttpResponse(status=405)

    tf = request.GET.get("duration", None)
    data_type = request.GET.get("type", None)

    if (
        tf not in large_correlations_timeframes
        or data_type not in redis_time_series_data_types
    ):
        return JsonResponse(invalid_params_error, status=400)

    tf = convert_timeframe_to_seconds(tf)

    pearson_correlations = msgpack.unpackb(
        r.execute_command("GET", f"pearson:{data_type}:{tf}:REDIS")
    )

    spearman_correlations = (
        msgpack.unpackb(r.execute_command("GET", f"spearman:{data_type}:{tf}:REDIS"))
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

    tf = stats_select_options_all[tf]

    formatted_tickers = [ticker[:-4] for ticker in tickers]

    pearson_correlations = msgpack.unpackb(
        r.execute_command("GET", f"pearson:price:{tf}:DB")
    )

    spearman_correlations = (
        msgpack.unpackb(r.execute_command("GET", f"spearman:price:{tf}:DB"))
        if "spearman" in large_correlation_types
        else []
    )

    response = {
        "xAxis": formatted_tickers,
        "yAxis": formatted_tickers,
        "data": pearson_correlations + spearman_correlations,
    }

    return JsonResponse(response, safe=False)
