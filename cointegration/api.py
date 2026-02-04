import math

from django.http import HttpResponse, JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.db.models.functions import Abs

from cointegration.models import CointegrationPair


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
