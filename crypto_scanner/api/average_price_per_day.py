from django.http import HttpResponse, JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.db.models import Avg, F
from django.db.models.functions import ExtractWeekDay
from django.utils import timezone

from crypto_scanner.models import BinanceSpotKline5m
from datetime import timedelta


from crypto_scanner.constants import stats_select_options_htf


@csrf_exempt
def average_price_change_per_day_of_week(request, symbol, duration):
    if request.method == "GET":
        current_date = timezone.now()
        current_day_of_week = timezone.now().weekday()
        start_of_week = current_date - timedelta(days=current_date.weekday())
        duration_hours = stats_select_options_htf[duration]

        if duration_hours is None:
            return JsonResponse(
                {"error": "Invalid duration", "code": "INVALID_DURATION"}, status=400
            )

        end_time = timezone.now()
        start_time = end_time - timedelta(hours=duration_hours)

        start_time_utc = start_time.astimezone(timezone.utc)
        end_time_utc = end_time.astimezone(timezone.utc)

        days_ago = start_of_week - timedelta(hours=duration_hours + 1)

        average_price_changes = (
            BinanceSpotKline5m.objects.filter(
                ticker=symbol, start_time__gte=start_time_utc
            )
            .annotate(day_of_week=ExtractWeekDay("start_time"))
            .values("day_of_week")
            .annotate(average_price_movement=Avg(F("close") - F("open")))
        )

        # Convert Decimal objects to floats for JSON serialization
        for item in average_price_changes:
            item["average_price_movement"] = float(item["average_price_movement"])

        response = {
            "xAxis": ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"],
            "data": [
                {
                    "value": round(item["average_price_movement"], 2),
                    "itemStyle": {
                        "color": "#a50f15"
                        # if current_day_of_week == item["day_of_week"]
                        if item["average_price_movement"] < 0
                        else "#4393c3"
                    },
                }
                for item in average_price_changes
            ],
        }

        return JsonResponse(response, safe=False)

    return HttpResponse(status=405)
