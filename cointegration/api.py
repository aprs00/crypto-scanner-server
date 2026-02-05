import json
import math

from django.http import HttpResponse, JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.db.models.functions import Abs

from cointegration.models import CointegrationPair
from cointegration.selectors import get_cointegration_pair_history as fetch_cointegration_pair_history


@csrf_exempt
def get_cointegration_live_table(request):
    if request.method != "GET":
        return HttpResponse(status=405)

    exchange = request.GET.get("exchange")
    window = request.GET.get("window")
    contract_type = request.GET.get("contractType", "perpetual")
    limit = int(request.GET.get("limit", 200))
    offset = int(request.GET.get("offset", 0))
    sort = request.GET.get("sort", "abs_z")
    sort_direction = request.GET.get("sortDirection") or request.GET.get("sortDir")

    if not exchange or not window:
        return JsonResponse(
            {"error": "Required parameters are: exchange, window"}, status=400
        )

    try:
        window_minutes = int(window)
    except ValueError:
        return JsonResponse({"error": "Invalid window value"}, status=400)

    qs = CointegrationPair.objects.filter(
        exchange__name=exchange,
        contract_type__name=contract_type,
        window_minutes=window_minutes,
    ).select_related("symbol1", "symbol2")

    if sort_direction:
        sort_direction = sort_direction.lower()
        if sort_direction not in {"asc", "desc"}:
            sort_direction = None

    default_sort_desc = {
        "abs_z": True,
        "half_life": False,
        "adf_t": False,
        "updated": True,
        "updated_at": True,
        "calculated_at": True,
    }

    desc = (
        default_sort_desc.get(sort, True)
        if sort_direction is None
        else sort_direction == "desc"
    )

    if sort == "abs_z":
        qs = qs.annotate(abs_z=Abs("spread_z"))
        order_field = "abs_z"
    elif sort == "half_life":
        order_field = "half_life"
    elif sort == "adf_t":
        order_field = "adf_t"
    elif sort in {"updated", "updated_at", "calculated_at"}:
        order_field = "calculated_at"
    else:
        order_field = "calculated_at"

    order_prefix = "-" if desc else ""
    qs = qs.order_by(f"{order_prefix}{order_field}")

    rows = qs[offset : offset + limit]

    data = []
    for row in rows:
        half_life = row.half_life
        if half_life is None or not math.isfinite(half_life):
            half_life_value = None
        else:
            half_life_value = round(half_life, 2)

        data.append(
            {
                "pair": f"{row.symbol1.name}-{row.symbol2.name}",
                "symbol1": row.symbol1.name,
                "symbol2": row.symbol2.name,
                "spread_z": round(row.spread_z, 3),
                "half_life": half_life_value,
                "adf_t": round(row.adf_t, 3),
                "hedge_ratio": round(row.hedge_ratio, 6),
                "intercept": round(row.intercept, 6),
                "spread_std": round(row.spread_std, 6),
                "updated_at": row.calculated_at.isoformat(),
            }
        )

    return JsonResponse({"data": data}, safe=False)


@csrf_exempt
def get_cointegration_pair_history(request):
    if request.method != "POST":
        return HttpResponse(status=405)

    try:
        body = json.loads(request.body)
    except json.JSONDecodeError:
        return JsonResponse({"error": "Invalid JSON body"}, status=400)

    exchange = body.get("exchange")
    contract_type = body.get("contractType", "perpetual")
    base_symbol = body.get("baseSymbol") or body.get("symbol1")
    comparison_symbols = body.get("comparisonSymbols")
    if comparison_symbols is None:
        symbol2 = body.get("symbol2")
        comparison_symbols = [symbol2] if symbol2 else []
    window = body.get("window")
    hours = body.get("hours")
    metric = body.get("metric", "spread_z")

    if (
        not exchange
        or not base_symbol
        or window is None
        or hours is None
        or comparison_symbols is None
    ):
        return JsonResponse(
            {
                "error": "Parameters 'exchange', 'baseSymbol', 'comparisonSymbols', 'window', and 'hours' are required"
            },
            status=400,
        )

    if not isinstance(comparison_symbols, list):
        return JsonResponse(
            {"error": "comparisonSymbols must be a list"}, status=400
        )

    cleaned_comparisons = []
    seen = set()
    for symbol in comparison_symbols:
        if not symbol or symbol == base_symbol or symbol in seen:
            continue
        seen.add(symbol)
        cleaned_comparisons.append(symbol)

    try:
        window_minutes = int(window)
        hours_value = int(hours)
    except (TypeError, ValueError):
        return JsonResponse({"error": "Invalid window or hours value"}, status=400)

    if hours_value <= 0:
        return JsonResponse({"error": "Hours must be greater than 0"}, status=400)

    allowed_metrics = {
        "spread_z",
        "half_life",
        "adf_t",
        "hedge_ratio",
        "spread_std",
    }

    if metric not in allowed_metrics:
        return JsonResponse({"error": "Invalid metric value"}, status=400)

    try:
        if not cleaned_comparisons:
            return JsonResponse(
                {
                    "history": [],
                    "metric": metric,
                    "base_symbol": base_symbol,
                    "comparison_symbols": [],
                    "window_minutes": window_minutes,
                },
                safe=False,
            )

        rows_by_pair = fetch_cointegration_pair_history(
            exchange=exchange,
            contract_type=contract_type,
            base_symbol=base_symbol,
            comparison_symbols=cleaned_comparisons,
            window_minutes=window_minutes,
            hours=hours_value,
        )

        metric_decimals = {
            "spread_z": 3,
            "half_life": 2,
            "adf_t": 3,
            "hedge_ratio": 3,
            "spread_std": 3,
        }

        history = []
        for rows in rows_by_pair:
            series = []
            for row in rows:
                value = row.get(metric)
                if value is None or not math.isfinite(value):
                    value = None
                else:
                    value = round(value, metric_decimals.get(metric, 3))
                series.append([row["calculated_at"].isoformat(), value])
            history.append(series)

        return JsonResponse(
            {
                "history": history,
                "metric": metric,
                "base_symbol": base_symbol,
                "comparison_symbols": cleaned_comparisons,
                "window_minutes": window_minutes,
            },
            safe=False,
        )

    except Exception as exc:
        print("Error in get_cointegration_pair_history:", exc)
        return JsonResponse({"error": "Internal server error"}, status=500)
