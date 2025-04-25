from django.http import HttpResponse, JsonResponse
from django.views.decorators.csrf import csrf_exempt
import redis
import msgpack

from zscore.selectors.zscore import get_all_tickers_data_z_score
from zscore.utils import (
    format_z_score_history_response,
)
from filters.constants import tf_options
from zscore.utils import format_z_score_matrix_response
from core.constants import invalid_params_error
from exchange_connections.constants import test_socket_symbols, tickers
from exchange_connections.selectors import get_exchange_symbols

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

        tf = tf_options[duration]
        symbols = get_exchange_symbols()

        print(msgpack.unpackb(r.execute_command("GET", f"zscore:{tf}")))

        response = format_z_score_matrix_response(
            msgpack.unpackb(r.execute_command("GET", f"zscore:{tf}")),
            symbols,
            x_axis,
            y_axis,
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

    if type is None:
        return JsonResponse(invalid_params_error, status=400)

    z_score_data = get_all_tickers_data_z_score(2, type)

    transformed_z_score_data = {}
    time = []
    matrix = []

    for coin in z_score_data:
        name = coin["base"]

        if name not in transformed_z_score_data:
            transformed_z_score_data[name] = []

        transformed_z_score_data[name].append(coin["z_score"])

        if name == "BTC":
            time.append(coin["time"])

    for row, (key, values) in enumerate(transformed_z_score_data.items()):
        for col, price in enumerate(values):
            matrix.append([col, row, round(price, 2)])

    response = {
        "data": matrix,
        "yAxis": list(transformed_z_score_data.keys()),
        "xAxis": time,
    }

    return JsonResponse(response, safe=False)
