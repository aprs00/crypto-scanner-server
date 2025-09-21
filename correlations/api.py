from django.http import HttpResponse, JsonResponse
from django.views.decorators.csrf import csrf_exempt
import msgpack
import logging

from core.redis_config import get_redis_connection
from exchange_connections.selectors import get_exchange_symbols

logger = logging.getLogger(__name__)
r = get_redis_connection()


@csrf_exempt
def get_pearson_correlation(request):
    if request.method != "GET":
        return HttpResponse(status=405)

    data_type = request.GET.get("type", None)
    hours = request.GET.get("hours", None)

    try:
        symbols = get_exchange_symbols()
        if not symbols:
            logger.error("Symbols data not found in Redis")
            return JsonResponse({"error": "Symbols data not available"}, status=503)

        correlation_key = f"correlations:{data_type}:{hours}:binance:perpetual"
        correlation_data = r.execute_command("GET", correlation_key)
        if not correlation_data:
            logger.error(f"Correlation data not found for key: {correlation_key}")
            return JsonResponse(
                {"error": "Correlation data not available for specified parameters"},
                status=503,
            )

        pearson_correlations = msgpack.unpackb(correlation_data)

        response = {
            "axis": [ticker[:-4] for ticker in symbols],
            "data": pearson_correlations,
            "type": "correlation",
        }

        return JsonResponse(response, safe=False)

    except Exception as e:
        logger.error(f"Error in get_pearson_correlation: {str(e)}")
        return JsonResponse({"error": "Internal server error"}, status=500)
