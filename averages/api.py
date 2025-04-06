from django.http import HttpResponse, JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.utils import timezone
from datetime import timedelta


from core.constants import invalid_params_error
from filters.constants import stats_select_options_htf
from averages.services.average_price import average_price_change


@csrf_exempt
def get_average_prices(request):
    if request.method != "GET":
        return HttpResponse(status=405)

    duration = request.GET.get("duration", None)
    symbol = request.GET.get("symbol", None)
    type = request.GET.get("type", None)

    if duration is None or symbol is None or type is None:
        return JsonResponse(invalid_params_error, status=400)

    duration_hours = stats_select_options_htf[duration]
    start_time_utc = timezone.now() - timedelta(hours=duration_hours)

    response = average_price_change(duration, symbol, start_time_utc, type)

    return JsonResponse(response, safe=False)
