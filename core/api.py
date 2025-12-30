from django.http import HttpResponse, JsonResponse
from django.views.decorators.csrf import csrf_exempt

from core.constants import tf_options
from exchange_connections.constants import KLINE_FIELD_MAP, get_btc_symbol
from exchange_connections.selectors import (
    get_exchange_symbols,
    get_top_market_cap_symbols,
)


def get_chart_defaults(exchange: str, market_cap_symbols: list[str]) -> dict:
    """
    Returns exchange-specific chart defaults.
    Uses market cap sorted symbols for beta_heatmap and comparison_symbols.
    """
    default_symbol = get_btc_symbol(exchange)

    return {
        "heatmap": {
            "symbols": market_cap_symbols[:50],
        },
        "price_change_percentage": {
            "symbol": default_symbol,
        },
        "correlation_pair_history": {
            "base_symbol": default_symbol,
            "comparison_symbols": [
                s for s in market_cap_symbols if s != default_symbol
            ][:10],
        },
    }


@csrf_exempt
def bootstrap(request):
    """
    Bootstrap endpoint that returns all necessary data for the frontend
    in a single request to reduce API calls.
    """
    if request.method != "GET":
        return HttpResponse(status=405)

    exchange = request.GET.get("exchange")
    contract_type = request.GET.get("contractType")
    symbols = get_exchange_symbols(exchange=exchange, contract_type=contract_type)
    market_cap_symbols = get_top_market_cap_symbols(
        exchange=exchange, contract_type=contract_type
    )

    data = {
        "hours_options": {
            k: {tk: str(tv) for tk, tv in v.items()} for k, v in tf_options.items()
        },
        "data_types": list(KLINE_FIELD_MAP.keys()),
        "symbols": symbols,
        "exchange": exchange,
        "contract_type": contract_type,
        "chart_defaults": get_chart_defaults(exchange, market_cap_symbols),
    }

    return JsonResponse(data, safe=False)
