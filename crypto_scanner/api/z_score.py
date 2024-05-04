from django.http import HttpResponse, JsonResponse
from django.core.cache import cache
from django.views.decorators.csrf import csrf_exempt
import redis

from crypto_scanner.services.z_score import (
    calculate_large_z_score_matrix,
    calculate_z_score_history,
    format_z_score_matrix_response,
    calculate_z_score_matrix,
    format_z_score_history_response,
)
from crypto_scanner.constants import (
    tickers,
    invalid_params_error,
    test_socket_symbols,
)

r = redis.Redis(host="redis", port=6379, decode_responses=True)


@csrf_exempt
def get_large_z_score_matrix(request):
    if request.method != "GET":
        return HttpResponse(status=405)

    x_axis = request.GET.get("x_axis", None)
    y_axis = request.GET.get("y_axis", None)
    tf = request.GET.get("tf", None)

    if x_axis is None or y_axis is None or tf is None:
        return JsonResponse(invalid_params_error, status=400)

    response = cache.get(f"z_score_matrix_large_{tf}")

    if response is None:
        calculate_large_z_score_matrix()
        response = cache.get(f"z_score_matrix_large_{tf}")

    response = format_z_score_matrix_response(
        response, test_socket_symbols, x_axis, y_axis, 2
    )

    return JsonResponse(response, safe=False)


@csrf_exempt
def get_z_score_matrix(request):
    if request.method == "GET":
        x_axis = request.GET.get("x_axis", None)
        y_axis = request.GET.get("y_axis", None)
        duration = request.GET.get("duration", None)

        if x_axis is None or y_axis is None or duration is None:
            return JsonResponse(
                {"error": "Invalid axis", "code": "INVALID_AXIS"}, status=400
            )

        response = cache.get(f"z_score_{duration}")

        if response is None:
            response = calculate_z_score_matrix(duration)
            cache.set(f"z_score_{duration}", response)

        response = format_z_score_matrix_response(response, tickers, x_axis, y_axis, 2)

        return JsonResponse(response, safe=False)

    return HttpResponse(status=405)


@csrf_exempt
def get_z_score_history(request):
    if request.method == "GET":
        duration = request.GET.get("duration", None)
        type = request.GET.get("type", None)

        if duration is None or type is None:
            return JsonResponse(invalid_params_error, status=400)

        response = cache.get(f"z_score_history_{duration}")

        if response is None:
            response = calculate_z_score_history(duration)
            cache.set(f"z_score_history_{duration}", response)

        formatted_response = format_z_score_history_response(response, type)

        response = {
            "legend": tickers,
            "data": formatted_response["data"],
            "xAxis": formatted_response["time"],
        }

        return JsonResponse(response, safe=False)

    return HttpResponse(status=405)
