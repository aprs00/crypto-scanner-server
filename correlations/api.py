from django.http import HttpResponse, JsonResponse
from django.views.decorators.csrf import csrf_exempt
import msgpack


from core.constants import invalid_params_error
from filters.constants import tf_options
from exchange_connections.constants import correlations_data_types
from core.redis_config import get_redis_connection


r = get_redis_connection()


@csrf_exempt
def get_pearson_correlation(request):
    if request.method != "GET":
        return HttpResponse(status=405)

    tf = request.GET.get("duration", None)
    data_type = request.GET.get("type", None)

    if not tf_options["correlation"][tf] or data_type not in correlations_data_types:
        return JsonResponse(invalid_params_error, status=400)

    hours = tf_options["correlation"][tf]

    symbols = msgpack.unpackb(
        r.execute_command("GET", "correlations:symbols:binance:perpetual")
    )

    pearson_correlations = msgpack.unpackb(
        r.execute_command("GET", f"correlations:{data_type}:{hours}:binance:perpetual")
    )

    response = {
        "axis": [ticker[:-4] for ticker in symbols],
        "data": pearson_correlations,
        "type": "correlation",
    }

    return JsonResponse(response, safe=False)
