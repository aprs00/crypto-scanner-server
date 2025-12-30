from django.http import HttpResponse, JsonResponse
from django.views.decorators.csrf import csrf_exempt
import msgpack
import numpy as np

from zscore.utils import format_z_score_matrix_response
from core.redis_config import get_redis_connection
from exchange_connections.selectors import get_historical_kline_data
from exchange_connections.constants import get_btc_symbol

r = get_redis_connection()


def print_btc_zscore_comparison(exchange="binance", contract_type="perpetual"):
    """Print BTC 1h z-score comparison between numpy calculation and Redis."""
    btc_symbol = get_btc_symbol(exchange)

    btc_data = get_historical_kline_data(hours=1, symbols=[btc_symbol], exchange=exchange)
    if btc_symbol in btc_data and "price" in btc_data[btc_symbol]:
        prices = np.array(btc_data[btc_symbol]["price"])
        if len(prices) > 0 and np.std(prices) > 0:
            btc_zscore = (prices[-1] - np.mean(prices)) / np.std(prices)
            print("----------")
            print(f"[{exchange}] Len prices: {len(prices)}")
            print(f"[{exchange}] BTC 1h Z-Score price (numpy): {btc_zscore:.4f}")

    redis_key = f"zscore:{exchange}:{contract_type}:1"
    redis_data = r.execute_command("GET", redis_key)
    if redis_data:
        redis_1h_data = msgpack.unpackb(redis_data, raw=False)
        btc_redis_zscore = redis_1h_data.get(btc_symbol, {}).get("price", "N/A")
        print(f"[{exchange}] BTC 1h Z-Score price (redis): {btc_redis_zscore}")
        print("----------")


@csrf_exempt
def get_z_score_matrix(request):
    if request.method != "GET":
        return HttpResponse(status=405)

    x_axis = request.GET.get("xAxis", None)
    y_axis = request.GET.get("yAxis", None)
    z_axis = request.GET.get("zAxis", None)
    hours = request.GET.get("hours", None)
    exchange = request.GET.get("exchange")
    contract_type = request.GET.get("contractType")

    if not hours or not exchange or not contract_type:
        return JsonResponse(
            {"error": "Missing required parameters: hours, exchange, contractType"},
            status=400,
        )

    hours = int(hours)

    print_btc_zscore_comparison(exchange=exchange, contract_type=contract_type)

    redis_key = f"zscore:{exchange}:{contract_type}:{hours}"
    redis_data = r.execute_command("GET", redis_key)
    if not redis_data:
        return JsonResponse({"error": f"Z-score data not available for {exchange}"}, status=503)

    hours_data = msgpack.unpackb(redis_data, raw=False)

    response = format_z_score_matrix_response(
        data=hours_data,
        x_axis=x_axis,
        y_axis=y_axis,
        z_axis=z_axis,
    )

    return JsonResponse(response, safe=False)


@csrf_exempt
def get_z_score_heatmap(request):
    if request.method != "GET":
        return HttpResponse(status=405)

    type = request.GET.get("type", None)
    hours = request.GET.get("hours", None)
    exchange = request.GET.get("exchange")
    contract_type = request.GET.get("contractType")

    if not hours or not exchange or not contract_type:
        return JsonResponse(
            {"error": "Missing required parameters: hours, exchange, contractType"},
            status=400,
        )

    hours = int(hours)

    redis_key = f"zscore:heatmap:{exchange}:{contract_type}:{hours}"
    redis_data = r.execute_command("GET", redis_key)
    if not redis_data:
        return JsonResponse({"error": f"Heatmap data not available for {exchange}"}, status=503)

    zscore_data = msgpack.unpackb(redis_data)

    ref_symbol = get_btc_symbol(exchange)

    transformed_zscore_data = {}
    times = []

    for record in zscore_data:
        if record["hours"] != 1:
            continue

        transformed_zscore_data.setdefault(record["symbol__name"], []).append(
            round(record[type], 3)
        )
        if record["symbol__name"] == ref_symbol:
            times.append(record["time"])

    matrix = [value for values in transformed_zscore_data.values() for value in values]

    response = {
        "data": matrix,
        "y_axis": list(transformed_zscore_data.keys()),
        "x_axis": list(times),
        "type": "grid",
    }

    return JsonResponse(response, safe=False)
