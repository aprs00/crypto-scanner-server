from django.http import HttpResponse, JsonResponse
from django.views.decorators.csrf import csrf_exempt
import msgpack
import numpy as np
import json

from zscore.utils import format_z_score_matrix_response
from core.redis_config import get_redis_connection

r = get_redis_connection()


@csrf_exempt
def get_z_score_matrix(request):
    if request.method != "POST":
        return HttpResponse(status=405)

    body = json.loads(request.body)
    x_axis = body.get("xAxis")
    y_axis = body.get("yAxis")
    z_axis = body.get("zAxis")
    hours = body.get("hours")
    exchange = body.get("exchange")
    contract_type = body.get("contractType")
    symbols = body.get("symbols", [])

    if not hours or not exchange or not contract_type:
        return JsonResponse(
            {"error": "Missing required parameters: hours, exchange, contractType"},
            status=400,
        )

    if not symbols:
        return JsonResponse([], safe=False)

    hours = int(hours)

    redis_key = f"zscore:{exchange}:{contract_type}:{hours}"
    redis_data = r.execute_command("GET", redis_key)
    if not redis_data:
        return JsonResponse(
            {"error": f"Z-score data not available for {exchange}"}, status=503
        )

    hours_data = msgpack.unpackb(redis_data, raw=False)

    response = format_z_score_matrix_response(
        data=hours_data,
        x_axis=x_axis,
        y_axis=y_axis,
        z_axis=z_axis,
        symbols=symbols,
    )

    return JsonResponse(response, safe=False)


@csrf_exempt
def get_z_score_heatmap(request):
    if request.method != "POST":
        return HttpResponse(status=405)

    body = json.loads(request.body)
    data_type = body.get("type")
    hours = body.get("hours")
    exchange = body.get("exchange")
    contract_type = body.get("contractType")
    requested_symbols = body.get("symbols", [])

    if not hours or not exchange or not contract_type:
        return JsonResponse(
            {"error": "Missing required parameters: hours, exchange, contractType"},
            status=400,
        )

    if len(requested_symbols) == 0:
        return JsonResponse([], safe=False)

    hours = int(hours)

    redis_key = f"zscore:heatmap:{exchange}:{contract_type}:{hours}"
    redis_data = r.execute_command("GET", redis_key)
    if not redis_data:
        return JsonResponse(
            {"error": f"Heatmap data not available for {exchange}"}, status=503
        )

    zscore_data = msgpack.unpackb(redis_data)

    requested_symbols_set = set(requested_symbols) if requested_symbols else None

    transformed_zscore_data = {}
    times = []

    for record in zscore_data:
        if record["hours"] != 1:
            continue

        symbol = record["symbol__name"]

        if requested_symbols_set and symbol not in requested_symbols_set:
            continue

        transformed_zscore_data.setdefault(symbol, []).append(
            round(record[data_type], 3)
        )

        time = record["time"]
        if not times or times[-1] != time:
            times.append(time)

    matrix = [value for values in transformed_zscore_data.values() for value in values]

    response = {
        "data": matrix,
        "y_axis": list(transformed_zscore_data.keys()),
        "x_axis": list(times),
        "type": "grid",
    }

    return JsonResponse(response, safe=False)
