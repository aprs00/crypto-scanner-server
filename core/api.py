from django.http import HttpResponse, JsonResponse
from django.views.decorators.csrf import csrf_exempt

from core.constants import tf_options
from exchange_connections.constants import tickers, KLINE_FIELD_MAP


@csrf_exempt
def bootstrap(request):
    """
    Bootstrap endpoint that returns all necessary data for the frontend
    in a single request to reduce API calls.
    """
    if request.method != "GET":
        return HttpResponse(status=405)

    data = {
        "tickers": tickers,
        "hours_options": {
            k: {tk: str(tv) for tk, tv in v.items()} for k, v in tf_options.items()
        },
        "data_types": list(KLINE_FIELD_MAP.keys()),
    }

    return JsonResponse(data, safe=False)
