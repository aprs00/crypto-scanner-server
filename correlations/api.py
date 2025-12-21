import json
from itertools import combinations

from django.http import HttpResponse, JsonResponse
from django.views.decorators.csrf import csrf_exempt
import msgpack
import logging
import numpy as np

from core.redis_config import get_redis_connection
from exchange_connections.selectors import (
    get_exchange_symbols,
    get_historical_kline_data,
)
from correlations.selectors import get_symbol_pair_correlation_history

logger = logging.getLogger(__name__)
r = get_redis_connection()
_request_count = 0


def _flatten_upper_index(i: int, j: int, size: int) -> int:
    """Return the position of (i, j) in a flattened upper-triangle array."""
    if i == j:
        raise ValueError("Cannot compute index for identical coordinates")
    if i > j:
        i, j = j, i
    return i * size - (i * (i + 1)) // 2 + j - i - 1


def debug_correlation():
    global _request_count
    _request_count += 1
    if _request_count != 10:
        return
    _request_count = 0

    try:
        kline_data = get_historical_kline_data(hours=2, symbols=["SOLUSDT", "BTCUSDT"])

        if "SOLUSDT" in kline_data and "BTCUSDT" in kline_data:
            sol_prices = np.array(kline_data["SOLUSDT"]["price"][-60:])
            btc_prices = np.array(kline_data["BTCUSDT"]["price"][-60:])
            print("SOL prices:", len(sol_prices))
            print("BTC prices:", len(btc_prices))
            corr_matrix = np.corrcoef(sol_prices, btc_prices)
            pair_correlation = float(corr_matrix[0, 1])
            print("----------------")
            print("----------------")
            print("----------------")
            print("----------------")
            print("----------------")
            print("----------------")
            print("----------------")
            print(f"SOLUSDT/BTCUSDT correlation (numpy): {pair_correlation:.4f}")

        # Get redis correlation value
        symbols = get_exchange_symbols()
        if symbols:
            try:
                sol_idx = symbols.index("SOLUSDT")
                btc_idx = symbols.index("BTCUSDT")

                correlation_key = "correlations:price:1:binance:perpetual"
                correlation_blob = r.get(correlation_key)
                if correlation_blob:
                    pearson_correlations = msgpack.unpackb(
                        correlation_blob, use_list=True, raw=False
                    )
                    total_symbols = len(symbols)
                    pair_idx = _flatten_upper_index(sol_idx, btc_idx, total_symbols)
                    redis_correlation = pearson_correlations[pair_idx]
                    print(
                        f"SOLUSDT/BTCUSDT correlation (redis): {redis_correlation:.4f}"
                    )
                else:
                    print("SOLUSDT/BTCUSDT correlation (redis): N/A")
            except (ValueError, IndexError) as e:
                print(f"SOLUSDT/BTCUSDT correlation (redis): N/A (error: {e})")
        print("----------------")
    except Exception as e:
        logger.error("Error calculating SOLUSDT/BTCUSDT correlation: %s", e)


@csrf_exempt
def get_pearson_correlation(request):
    if request.method != "POST":
        return HttpResponse(status=405)

    try:
        body = json.loads(request.body)
    except json.JSONDecodeError:
        return JsonResponse({"error": "Invalid JSON body"}, status=400)

    data_type = body.get("type")
    hours = body.get("hours")
    requested_symbols = body.get("symbols", [])

    if not data_type or not hours:
        return JsonResponse(
            {"error": "Parameters 'type' and 'hours' are required."}, status=400
        )

    if not requested_symbols:
        return JsonResponse({"axis": [], "data": [], "type": "correlation"})

    try:
        symbols = get_exchange_symbols()
        if not symbols:
            logger.error("Symbols data not found in Redis")
            return JsonResponse({"error": "Symbols data not available"}, status=503)

        correlation_key = f"correlations:{data_type}:{hours}:binance:perpetual"
        correlation_blob = r.get(correlation_key)
        if not correlation_blob:
            logger.error("Correlation data not found for key %s", correlation_key)
            return JsonResponse(
                {"error": "Correlation data not available for specified parameters"},
                status=503,
            )

        pearson_correlations = [
            round(v, 3)
            for v in msgpack.unpackb(correlation_blob, use_list=True, raw=False)
        ]
        axis = [ticker[:-4] if len(ticker) > 4 else ticker for ticker in symbols]

        symbol_lookup = {ticker: idx for idx, ticker in enumerate(symbols)}

        selected_indices = [
            symbol_lookup[symbol]
            for symbol in requested_symbols
            if symbol in symbol_lookup
        ]

        total_symbols = len(symbols)
        expected_length = total_symbols * (total_symbols - 1) // 2
        if len(pearson_correlations) != expected_length:
            print(
                f"Correlation vector size mismatch: expected {expected_length}, got {len(pearson_correlations)}",
            )
            return JsonResponse({"error": "Correlation data invalid"}, status=503)

        if selected_indices:
            axis = [axis[idx] for idx in selected_indices]
            if len(selected_indices) >= 2:
                filtered = []
                for i_idx, j_idx in combinations(selected_indices, 2):
                    pair_idx = _flatten_upper_index(i_idx, j_idx, total_symbols)
                    if 0 <= pair_idx < len(pearson_correlations):
                        filtered.append(pearson_correlations[pair_idx])
                pearson_correlations = filtered
            else:
                pearson_correlations = []

        debug_correlation()

        return JsonResponse(
            {"axis": axis, "data": pearson_correlations, "type": "correlation"}
        )

    except Exception as exc:
        logger.error("Error in get_pearson_correlation: %s", exc, exc_info=True)
        return JsonResponse({"error": "Internal server error"}, status=500)


@csrf_exempt
def get_correlation_pair_history(request):
    """
    Get historical correlation values for base_symbol against multiple symbols.
    """
    if request.method != "POST":
        return HttpResponse(status=405)

    try:
        body = json.loads(request.body)
    except json.JSONDecodeError:
        return JsonResponse({"error": "Invalid JSON body"}, status=400)

    base_symbol = body.get("baseSymbol")
    comparison_symbols = body.get("comparisonSymbols", [])
    data_type = body.get("type")
    hours = body.get("hours")
    if hours is None:
        return JsonResponse({"error": "Parameter 'hours' is required"}, status=400)
    hours = int(hours)

    try:
        return JsonResponse(
            {
                "history": get_symbol_pair_correlation_history(
                    base_symbol=base_symbol,
                    comparison_symbols=comparison_symbols,
                    data_type=data_type,
                    hours=hours,
                ),
            },
            safe=False,
        )

    except Exception as e:
        logger.error(f"Error in get_correlation_pair_history: {str(e)}")
        return JsonResponse({"error": "Internal server error"}, status=500)
