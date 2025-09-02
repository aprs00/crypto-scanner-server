from django.http import HttpResponse, JsonResponse
from django.views.decorators.csrf import csrf_exempt


from averages.services.average_price import average_price_change


@csrf_exempt
def get_average_prices(request):
    if request.method != "GET":
        return HttpResponse(status=405)

    symbol = request.GET.get("symbol", None)
    type = request.GET.get("type", None)
    hours = request.GET.get("hours", None)
    hours = int(hours)

    response = average_price_change(hours, symbol, type)

    return JsonResponse(response, safe=False)
