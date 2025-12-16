from django.http import HttpResponse, JsonResponse
from django.views.decorators.csrf import csrf_exempt
import msgpack
import numpy as np

from zscore.utils import format_z_score_matrix_response
from core.redis_config import get_redis_connection
from exchange_connections.selectors import get_historical_kline_data

r = get_redis_connection()


def print_btc_zscore_comparison():
    """Print BTC 1h z-score comparison between numpy calculation and Redis."""
    btc_data = get_historical_kline_data(hours=1, symbols=["BTCUSDT"])
    if "BTCUSDT" in btc_data and "price" in btc_data["BTCUSDT"]:
        prices = np.array(btc_data["BTCUSDT"]["price"])
        btc_zscore = (prices[-1] - np.mean(prices)) / np.std(prices)
        print("----------")
        print("----------")
        print("----------")
        print("----------")
        print(f"Len prices: {len(prices)}")
        print(f"BTC 1h Z-Score price (numpy): {btc_zscore:.4f}")

    redis_1h_data = msgpack.unpackb(
        r.execute_command("GET", "zscore:binance:perpetual:1"), raw=False
    )
    btc_redis_zscore = redis_1h_data.get("BTCUSDT", {}).get("price", "N/A")
    print(f"BTC 1h Z-Score price (redis): {btc_redis_zscore}")
    print("----------")


@csrf_exempt
def get_z_score_matrix(request):
    if request.method != "GET":
        return HttpResponse(status=405)

    x_axis = request.GET.get("xAxis", None)
    y_axis = request.GET.get("yAxis", None)
    z_axis = request.GET.get("zAxis", None)
    hours = request.GET.get("hours", None)
    hours = int(hours)

    print_btc_zscore_comparison()

    hours_data = msgpack.unpackb(
        r.execute_command("GET", f"zscore:binance:perpetual:{hours}"), raw=False
    )

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
    hours = int(hours)

    zscore_data = msgpack.unpackb(
        r.execute_command("GET", f"zscore:heatmap:binance:perpetual:{hours}")
    )

    transformed_zscore_data = {}
    times = []

    for record in zscore_data:
        if record["hours"] != 1:
            continue

        transformed_zscore_data.setdefault(record["symbol__name"], []).append(
            round(record[type], 3)
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
