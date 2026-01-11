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
    contract_type = body.get("contractType", "perpetual")
    symbols = body.get("symbols", [])

    if not hours or not exchange:
        return JsonResponse(
            {"error": "Missing required parameters: hours, exchange"},
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
    contract_type = body.get("contractType", "perpetual")
    requested_symbols = body.get("symbols", [])

    if not hours or not exchange:
        return JsonResponse(
            {"error": "Missing required parameters: hours, exchange"},
            status=400,
        )

    if len(requested_symbols) == 0:
        return JsonResponse([], safe=False)

    if data_type not in {"price", "volume", "trades"}:
        return JsonResponse(
            {"error": "Invalid type. Expected one of: price, volume, trades"},
            status=400,
        )

    hours = int(hours)

    redis_key = f"zscore:heatmap:{exchange}:{contract_type}:{hours}"
    redis_data = r.execute_command("GET", redis_key)
    if not redis_data:
        return JsonResponse(
            {"error": f"Heatmap data not available for {exchange}"}, status=503
        )

    zscore_data = msgpack.unpackb(redis_data, raw=False)

    requested_symbols_set = set(requested_symbols)

    symbol_to_time_to_value = {}
    times = []

    for record in zscore_data:
        if record["hours"] != hours:
            continue

        symbol = record["symbol__name"]
        if symbol not in requested_symbols_set:
            continue

        time = record["time"]
        value = round(record[data_type], 3)

        symbol_to_time_to_value.setdefault(symbol, {})[time] = value

        if not times or times[-1] != time:
            times.append(time)

    y_axis = [s for s in requested_symbols if s in symbol_to_time_to_value]
    matrix_rows = [[symbol_to_time_to_value[s].get(t) for t in times] for s in y_axis]
    matrix = [v for row in matrix_rows for v in row]

    response = {"data": matrix, "y_axis": y_axis, "x_axis": times, "type": "grid"}

    return JsonResponse(response, safe=False)
