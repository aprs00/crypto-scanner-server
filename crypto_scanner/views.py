from django.http import HttpResponse, JsonResponse
from django.core.cache import cache
from django.views.decorators.csrf import csrf_exempt
from django.db.models import Avg, F, FloatField
from django.db.models.functions import ExtractWeekDay, Cast
from django.utils import timezone

from crypto_scanner.models import BinanceSpotKline5m
from datetime import timedelta

import numpy as np

from crypto_scanner.constants import (
    stats_select_options_htf,
    stats_select_options_ltf,
    stats_select_options_all,
    tickers,
)
from crypto_scanner.utils import format_options


@csrf_exempt
def get_tickers_options(request):
    if request.method == "GET":
        response = format_options(tickers, "list")

        return JsonResponse(response, safe=False)

    return HttpResponse(status=405)


@csrf_exempt
def get_stats_select_options(request):
    if request.method == "GET":
        response = {
            "htf": format_options(stats_select_options_htf),
            "ltf": format_options(stats_select_options_ltf),
            "all": format_options(stats_select_options_all),
        }

        return JsonResponse(response, safe=False)

    # Other HTTP methods are not allowed for this view
    return HttpResponse(status=405)


@csrf_exempt
def average_price_change_per_day_of_week(request, symbol, duration):
    if request.method == "GET":
        # Calculate the start date of the week
        current_date = timezone.now()
        current_day_of_week = timezone.now().weekday()
        start_of_week = current_date - timedelta(days=current_date.weekday())
        num_of_days_select_options = stats_select_options_htf[duration]

        if num_of_days_select_options is None:
            return JsonResponse(
                {"error": "Invalid duration", "code": "INVALID_DURATION"}, status=400
            )

        days_ago = start_of_week - timedelta(hours=num_of_days_select_options + 1)

        average_price_changes = (
            BinanceSpotKline5m.objects.filter(ticker=symbol, start_time__gte=days_ago)
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


def get_min_length(query_tickers_data):
    min_length = min([len(data) for data in query_tickers_data.values()])
    for ticker in tickers:
        query_tickers_data[ticker] = query_tickers_data[ticker][:min_length]

    return query_tickers_data


#
# Z-SCORE
#
def get_tickers_data_z_score(duration):
    trades_volume_price_tickers_data = {}
    duration_hours = stats_select_options_all[duration]

    end_time = timezone.now()
    start_time = end_time - timedelta(hours=duration_hours)

    for ticker in tickers:
        trades_volume_price_tickers_data[ticker] = BinanceSpotKline5m.objects.filter(
            ticker=ticker, start_time__gte=start_time, start_time__lte=end_time
        ).values_list("base_volume", "close", "number_of_trades")

    for base_volume, close, number_of_trades in trades_volume_price_tickers_data[
        "BTCUSDT"
    ]:
        print(base_volume, close, number_of_trades)

    trades_volume_price_tickers_data = get_min_length(trades_volume_price_tickers_data)

    return trades_volume_price_tickers_data


def calculate_z_score(duration):
    data = get_tickers_data_z_score(duration)
    z_scores = {}

    for ticker in tickers:
        mean_volume = np.mean(data[ticker][0])
        mean_price = np.mean(data[ticker][1])
        mean_trades = np.mean(data[ticker][2])

        std_dev_volume = np.std(data[ticker][0])
        std_dev_price = np.std(data[ticker][1])
        std_dev_trades = np.std(data[ticker][2])

        z_scores[ticker] = {
            "volume": (data[ticker][0][-1] - mean_volume) / std_dev_volume,
            "price": (data[ticker][1][-1] - mean_price) / std_dev_price,
            "trades": (data[ticker][2][-1] - mean_trades) / std_dev_trades,
        }

    return z_scores


def get_z_score_matrix(request, duration):
    if request.method == "GET":
        response = cache.get(f"z_score_{duration}")

        xAxis = request.GET.get("x_axis", None)
        yAxis = request.GET.get("y_axis", None)

        if xAxis is None or yAxis is None:
            return JsonResponse(
                {"error": "Invalid axis", "code": "INVALID_AXIS"}, status=400
            )

        if response is None:
            response = calculate_z_score(duration)
            cache.set(f"z_score_{duration}", response)

        ticker_colors = [
            "#3357CC",  # Lighter blue
            "#7A42FF",  # Lighter purple
            "#4DFF4D",  # Lighter green
            "#1F1F1F",  # Slightly lighter dark grey
            "#6243B6",  # Lighter indigo
            "#00B3B3",  # Lighter cyan
            "#555555",  # Gray (unchanged)
            "#1E57B7",  # Slightly lighter blue
            "#B30000",  # Lighter red
            "#6B7F4F",  # Lighter olive green
            "#666666",  # Gray (unchanged)
        ]

        response = [
            {
                "type": "scatter",
                "name": ticker,
                "data": [
                    [
                        round(response[ticker][xAxis], 2),
                        round(response[ticker][yAxis], 2),
                    ]
                ],
                "color": ticker_colors[i],
                "symbolSize": 20,
            }
            for i, ticker in enumerate(tickers)
        ]

        return JsonResponse(response, safe=False)

    return HttpResponse(status=405)


#
# PEARSON CORRELATION
#
def get_tickers_data(duration):
    query_tickers_data = {}
    duration = stats_select_options_all[duration]

    for ticker in tickers:
        query_tickers_data[ticker] = (
            BinanceSpotKline5m.objects.filter(
                ticker=ticker,
                start_time__gte=timezone.now() - timedelta(hours=duration),
            )
            .annotate(close_as_float=Cast("close", FloatField()))
            .values_list("close_as_float", flat=True)
        )

    query_tickers_data = get_min_length(query_tickers_data)

    return query_tickers_data


def calculate_pearson_correlation(duration):
    correlation_results = {}

    query_tickers_data = get_tickers_data(duration)

    for ticker1 in tickers:
        for ticker2 in tickers:
            correlation_coefficient = np.corrcoef(
                query_tickers_data[ticker1], query_tickers_data[ticker2]
            )[0, 1]

            correlation_results[f"{ticker1} - {ticker2}"] = correlation_coefficient

    formatted_tickers = [ticker[:-4] for ticker in tickers]

    response = {
        "xAxis": formatted_tickers,
        "yAxis": formatted_tickers,
        "data": [
            [i, j, round(correlation_results[f"{tickers[i]} - {tickers[j]}"], 2)]
            for i in range(len(tickers))
            for j in range(i + 1, len(tickers))
        ],
    }

    return response


@csrf_exempt
def get_pearson_correlation(request, duration):
    if request.method == "GET":
        response = cache.get(f"pearson_correlation_{duration}")

        if response is None:
            print("response is None: ", response)
            response = calculate_pearson_correlation(duration)
            cache.set(f"pearson_correlation_{duration}", response)

        return JsonResponse(response, safe=False)

    # Other HTTP methods are not allowed for this view
    return HttpResponse(status=405)
