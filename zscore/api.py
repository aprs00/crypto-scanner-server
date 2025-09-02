from django.http import HttpResponse, JsonResponse
from django.views.decorators.csrf import csrf_exempt
import msgpack

from zscore.utils import format_z_score_matrix_response
from core.redis_config import get_redis_connection

r = get_redis_connection()


@csrf_exempt
def get_z_score_matrix(request):
    if request.method != "GET":
        return HttpResponse(status=405)

    x_axis = request.GET.get("xAxis", None)
    y_axis = request.GET.get("yAxis", None)
    hours = request.GET.get("hours", None)
    hours = int(hours)

    hours_data = msgpack.unpackb(
        r.execute_command("GET", f"zscore:binance:perpetual:{hours}"), raw=False
    )

    response = format_z_score_matrix_response(
        data=hours_data,
        x_axis=x_axis,
        y_axis=y_axis,
    )

    return JsonResponse(response, safe=False)


@csrf_exempt
def get_z_score_heatmap(request):
    if request.method != "GET":
        return HttpResponse(status=405)

    type = request.GET.get("type", None)
    hours = request.GET.get("hours", None)
    hours = int(hours)
    # TODO: also add rolling hours select option

    zscore_data = msgpack.unpackb(
        r.execute_command("GET", f"zscore:heatmap:binance:perpetual:{hours}")
    )

    transformed_zscore_data = {}
    times = []

    for record in zscore_data:
        if record["hours"] != 1:
            continue

        transformed_zscore_data.setdefault(record["symbol__name"], []).append(
            record[type]
        )
        if record["symbol__name"] == "BTCUSDT":
            times.append(record["time"])

    matrix = [value for values in transformed_zscore_data.values() for value in values]

    response = {
        "data": matrix,
        "y_axis": [symbol[:4] for symbol in list(transformed_zscore_data.keys())],
        "x_axis": list(times),
        "type": "grid",
    }

    return JsonResponse(response, safe=False)
