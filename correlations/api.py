import json
from itertools import combinations

from django.http import HttpResponse, JsonResponse
from django.views.decorators.csrf import csrf_exempt
import msgpack

from core.redis_config import get_redis_connection
from exchange_connections.selectors import get_exchange_symbols
from correlations.selectors import get_symbol_pair_correlation_history

r = get_redis_connection()


def _flatten_upper_index(i: int, j: int, size: int) -> int:
    """Return the position of (i, j) in a flattened upper-triangle array."""
    if i == j:
        raise ValueError("Cannot compute index for identical coordinates")
    if i > j:
        i, j = j, i
    return i * size - (i * (i + 1)) // 2 + j - i - 1


def _decode_correlation_blob(correlation_blob):
    """Decode backward/forward compatible correlation payloads from Redis."""
    unpacked = msgpack.unpackb(correlation_blob, use_list=True, raw=False)

    if isinstance(unpacked, dict):
        values = unpacked.get("values")
        symbols = unpacked.get("symbols")
        if isinstance(values, list):
            return values, symbols if isinstance(symbols, list) else None

    if isinstance(unpacked, list):
        return unpacked, None

    return [], None


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
    exchange = body.get("exchange")
    contract_type = body.get("contractType", "perpetual")

    if not data_type or not hours:
        return JsonResponse(
            {"error": "Parameters 'type' and 'hours' are required."}, status=400
        )

    if not requested_symbols:
        return JsonResponse({"axis": [], "data": [], "type": "correlation"})

    try:
        symbols = get_exchange_symbols(exchange=exchange, contract_type=contract_type)
        if not symbols:
            print(f"Symbols data not found in Redis for {exchange}:{contract_type}")
            return JsonResponse({"error": "Symbols data not available"}, status=503)

        correlation_key = f"correlations:{data_type}:{hours}:{exchange}:{contract_type}"
        correlation_blob = r.get(correlation_key)
        if not correlation_blob:
            print("Correlation data not found for key", correlation_key)
            return JsonResponse(
                {"error": "Correlation data not available for specified parameters"},
                status=503,
            )

        raw_correlations, packed_symbols = _decode_correlation_blob(correlation_blob)
        pearson_correlations = [round(v, 3) for v in raw_correlations]
        axis = list(packed_symbols) if packed_symbols else list(symbols)

        symbol_lookup = {ticker: idx for idx, ticker in enumerate(axis)}

        selected_indices = [
            symbol_lookup[symbol]
            for symbol in requested_symbols
            if symbol in symbol_lookup
        ]

        total_symbols = len(axis)
        expected_length = total_symbols * (total_symbols - 1) // 2
        if len(pearson_correlations) != expected_length:
            print(
                f"Correlation vector size mismatch: expected {expected_length}, got {len(pearson_correlations)} "
                f"(axis={len(axis)} exchange_symbols={len(symbols)})",
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

        return JsonResponse(
            {"axis": axis, "data": pearson_correlations, "type": "correlation"}
        )

    except Exception as exc:
        print("Error in get_pearson_correlation:", exc)
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
    exchange = body.get("exchange")
    contract_type = body.get("contractType", "perpetual")

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
                    exchange=exchange,
                    contract_type=contract_type,
                ),
            },
            safe=False,
        )

    except Exception as e:
        print("Error in get_correlation_pair_history:", e)
        return JsonResponse({"error": "Internal server error"}, status=500)
