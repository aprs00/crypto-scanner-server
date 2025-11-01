from django.http import HttpResponse, JsonResponse
from django.views.decorators.csrf import csrf_exempt

from core.constants import tf_options
from exchange_connections.constants import KLINE_FIELD_MAP
from exchange_connections.selectors import (
    get_exchange_symbols,
    get_top_market_cap_symbols,
)


@csrf_exempt
def bootstrap(request):
    """
    Bootstrap endpoint that returns all necessary data for the frontend
    in a single request to reduce API calls.
    """
    if request.method != "GET":
        return HttpResponse(status=405)

    data = {
        "hours_options": {
            k: {tk: str(tv) for tk, tv in v.items()} for k, v in tf_options.items()
        },
        "data_types": list(KLINE_FIELD_MAP.keys()),
        "symbols": get_exchange_symbols(),
        "market_cap_symbols": get_top_market_cap_symbols(),
    }

    return JsonResponse(data, safe=False)
