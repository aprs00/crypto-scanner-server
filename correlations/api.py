from django.http import HttpResponse, JsonResponse
from django.views.decorators.csrf import csrf_exempt
import msgpack

from core.redis_config import get_redis_connection


r = get_redis_connection()


@csrf_exempt
def get_pearson_correlation(request):
    if request.method != "GET":
        return HttpResponse(status=405)

    data_type = request.GET.get("type", None)
    hours = request.GET.get("hours", None)
    hours = int(hours)

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
