from __future__ import annotations

from decimal import Decimal

from django.http import HttpRequest, JsonResponse
from django.utils import timezone
from django.views.decorators.http import require_GET

from footprints.models import FootprintMarket


def _error(message: str, code: str, http_status: int = 400, **extra) -> JsonResponse:
    return JsonResponse({"error": message, "code": code, **extra}, status=http_status)


def market_by_key(market_key: str) -> FootprintMarket | None:
    parts = market_key.split(":", 2)
    if len(parts) != 3:
        return None
    return FootprintMarket.objects.filter(
        exchange=parts[0],
        contract_type=parts[1],
        symbol=parts[2],
        enabled=True,
    ).first()


@require_GET
def markets(_request: HttpRequest) -> JsonResponse:
    result = []
    now = timezone.now()
    for market in FootprintMarket.objects.filter(enabled=True):
        spec = (
            market.instrument_specs.filter(effective_from__lte=now)
            .order_by("-effective_from")
            .first()
        )
        if spec is None:
            continue
        result.append(
            {
                "marketKey": market.market_key,
                "exchange": market.exchange,
                "symbol": market.symbol,
                "baseAsset": market.base_asset,
                "currentTickSize": _decimal_string(spec.tick_size),
            }
        )
    response = JsonResponse({"markets": result})
    response["Cache-Control"] = "public, max-age=30"
    return response


def _decimal_string(value: Decimal) -> str:
    rendered = format(value.normalize(), "f")
    return rendered if "." not in rendered else rendered.rstrip("0").rstrip(".")
