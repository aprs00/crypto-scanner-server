from django.http import HttpResponse, JsonResponse
from django.views.decorators.csrf import csrf_exempt
import redis
import msgpack

from zscore.selectors.zscore import get_zscore_history_data
from zscore.utils import format_z_score_history_response, format_z_score_matrix_response
from filters.constants import tf_options
from core.constants import invalid_params_error
from exchange_connections.constants import tickers

r = redis.Redis(host="redis")


@csrf_exempt
def get_z_score_matrix(request):
    if request.method == "GET":
        x_axis = request.GET.get("xAxis", None)
        y_axis = request.GET.get("yAxis", None)
        duration = request.GET.get("duration", None)

        if x_axis is None or y_axis is None or duration is None:
            return JsonResponse(
                {"error": "Invalid axis", "code": "INVALID_AXIS"}, status=400
            )

        tf = tf_options["zscore"][duration]

        tf_data = msgpack.unpackb(
            r.execute_command("GET", f"zscore:binance:perpetual:{tf}"), raw=False
        )

        response = format_z_score_matrix_response(
            data=tf_data,
            x_axis=x_axis,
            y_axis=y_axis,
        )

        return JsonResponse(response, safe=False)

    return HttpResponse(status=405)


@csrf_exempt
def get_z_score_history(request):
    if request.method == "GET":
        duration = request.GET.get("duration", None)
        type = request.GET.get("type", None)

        if duration is None or type is None:
            return JsonResponse(invalid_params_error, status=400)

        formatted_response = format_z_score_history_response(
            r.execute_command("GET", f"z_score_history_{duration}"), type
        )

        response = {
            "legend": tickers,
            "data": formatted_response["data"],
            "xAxis": formatted_response["time"],
        }

        return JsonResponse(response, safe=False)

    return HttpResponse(status=405)


@csrf_exempt
def get_z_score_heatmap(request):
    if request.method != "GET":
        return HttpResponse(status=405)

    type = request.GET.get("type", None)
    hours = request.GET.get("hours", None)

    if type is None:
        return JsonResponse(invalid_params_error, status=400)

    zscore_data = msgpack.unpackb(
        r.execute_command("GET", f"zscore:heatmap:binance:perpetual:{hours}")
    )

    transformed_zscore_data = {}
    time = []
    matrix = []

    for record in zscore_data:
        if record["hours"] != 1:
            continue

        name = record["symbol_name"]

        transformed_zscore_data.setdefault(name, []).append(record.get(type))

        if name == "BTCUSDT":
            time.append(record["time"])

    for row, (_, values) in enumerate(transformed_zscore_data.items()):
        for col, zscore in enumerate(values):
            matrix.append([col, row, round(zscore, 2)])

    response = {
        "data": matrix,
        "y_axis": list(transformed_zscore_data.keys()),
        "x_axis": time,
    }

    return JsonResponse(response, safe=False)
