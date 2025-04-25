from django.http import HttpResponse, JsonResponse
from django.views.decorators.csrf import csrf_exempt

import redis
import msgpack


from core.constants import invalid_params_error
from filters.constants import tf_options
from exchange_connections.constants import (
    tickers,
    redis_time_series_data_types,
)
from exchange_connections.selectors import get_exchange_symbols


r = redis.Redis(host="redis")


@csrf_exempt
def get_pearson_correlation(request):
    if request.method != "GET":
        return HttpResponse(status=405)

    tf = request.GET.get("duration", None)
    data_type = request.GET.get("type", None)

    if not tf_options[tf] or data_type not in redis_time_series_data_types:
        return JsonResponse(invalid_params_error, status=400)

    tf = tf_options[tf]

    symbols = get_exchange_symbols()

    pearson_correlations = msgpack.unpackb(
        r.execute_command("GET", f"correlations:{data_type}:{tf}")
    )

    response = {
        "axis": [ticker[:-4] for ticker in symbols],
        "data": pearson_correlations,
    }

    return JsonResponse(response, safe=False)
