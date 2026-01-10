from django.http import HttpResponse, JsonResponse
from django.views.decorators.csrf import csrf_exempt

from core.constants import EXCHANGE_CONFIG
from exchange_connections.constants import get_btc_symbol
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
    Bootstrap endpoint that returns all necessary data for all exchanges
    in a single request to reduce API calls.
    """
    if request.method != "GET":
        return HttpResponse(status=405)

    contract_type = "perpetual"

    exchange_data = {
        exchange_id: {
            "symbols": symbols,
            "data_types": exchange_config["data_types"],
            "hours_options": {
                k: {tk: str(tv) for tk, tv in v.items()}
                for k, v in exchange_config["hours_options"].items()
            },
            "chart_defaults": get_chart_defaults(exchange_id, market_cap_symbols),
        }
        for exchange_id, exchange_config in EXCHANGE_CONFIG.items()
        for symbols in [get_exchange_symbols(exchange=exchange_id, contract_type=contract_type)]
        for market_cap_symbols in [get_top_market_cap_symbols(exchange=exchange_id, contract_type=contract_type)]
    }

    data = {
        "exchanges": [{"id": k, "name": v["name"]} for k, v in EXCHANGE_CONFIG.items()],
        "exchange_data": exchange_data,
    }

    return JsonResponse(data, safe=False)
