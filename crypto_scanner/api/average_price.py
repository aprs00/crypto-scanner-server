from django.http import HttpResponse, JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.utils import timezone
import numpy as np

from crypto_scanner.models import BinanceSpotKline5m

# BinanceSpotKline5m = None
from datetime import timedelta


from crypto_scanner.constants import stats_select_options_htf, invalid_params_error


def extract_db_data(symbol, start_time_utc, group_by):
    extract_function = None
    column_name = None

    if group_by == "day":
        extract_function = "dow"
        column_name = "day_of_week"
    elif group_by == "hour":
        extract_function = "hour"
        column_name = "hour_of_day"

    query = f"""
        WITH ranked_data AS (
            SELECT
                id,
                start_time,
                open,
                close,
                ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('{group_by}', start_time), ticker ORDER BY start_time) AS row_asc,
                ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('{group_by}', start_time), ticker ORDER BY start_time DESC) AS row_desc
            FROM
                "crypto_scanner_binance_spot_kline_5m"
            WHERE
                ticker = '{symbol}'
                AND start_time >= '{start_time_utc}'
        )
        SELECT
            MAX(id) as id,
            extract({extract_function} from DATE_TRUNC('{group_by}', start_time)) AS {column_name},
            MAX(CASE WHEN row_asc = 1 THEN open END) AS open,
            MAX(CASE WHEN row_desc = 1 THEN close END) AS close
        FROM
            ranked_data
        GROUP BY
            DATE_TRUNC('{group_by}', start_time)
    """

    # query = f"""
    #     WITH ranked_data AS (
    #         SELECT
    #             id,
    #             start_time,
    #             open,
    #             close,
    #             ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('{group_by}', start_time), ticker ORDER BY start_time) AS row_asc,
    #             ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('{group_by}', start_time), ticker ORDER BY start_time DESC) AS row_desc
    #         FROM
    #             "crypto_scanner_binance_spot_kline_5m"
    #         LEFT JOIN
    #             crypto_scanner_binance_spot_tickers AS tickers
    #         ON
    #             kline.ticker_id = tickers.id
    #         WHERE
    #             tickers.name = '{symbol}'
    #             AND start_time >= '{start_time_utc}'
    #     )
    #     SELECT
    #         MAX(id) as id,
    #         extract({extract_function} from DATE_TRUNC('{group_by}', start_time)) AS {column_name},
    #         MAX(CASE WHEN row_asc = 1 THEN open END) AS open,
    #         MAX(CASE WHEN row_desc = 1 THEN close END) AS close
    #     FROM
    #         ranked_data
    #     GROUP BY
    #         DATE_TRUNC('{group_by}', start_time)
    # """
    price_changes = BinanceSpotKline5m.objects.raw(query)
    print(price_changes.query)

    return price_changes


def format_data(data):
    formatted_data = []

    for value in data.values():
        item_style = {
            "color": "#4393c3" if np.average(value) > 0 else "#a50f15",
        }
        formatted_data.append(
            {"itemStyle": item_style, "value": round(np.average(value), 2)}
        )

    return formatted_data


def calculate_dict_percentage(data, grouped_by):
    calculated_data = {}

    for entry in data:
        grouped_by_value = getattr(entry, grouped_by)
        percentage = (entry.close - entry.open) / entry.close * 100

        if grouped_by_value not in calculated_data:
            calculated_data[grouped_by_value] = [percentage]
        else:
            calculated_data[grouped_by_value].append(percentage)

    return calculated_data


def average_price_change_per_day_of_week(duration, symbol, start_time_utc):
    if duration is None or symbol is None:
        return JsonResponse(invalid_params_error, status=400)

    daily_price_changes = extract_db_data(symbol, start_time_utc, "day")

    weekdays = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]

    weekdays_dict_values = calculate_dict_percentage(daily_price_changes, "day_of_week")

    formatted_data = format_data(weekdays_dict_values)
    xAxis = []

    int_values_array = [int(float(str(item))) for item in weekdays_dict_values.keys()]

    for day in int_values_array:
        xAxis.append(weekdays[day])

    response = {
        "data": formatted_data,
        "xAxis": xAxis,
    }

    return response


def average_price_change_per_hour_of_day(duration, symbol, start_time_utc):
    if duration is None or symbol is None:
        return JsonResponse(invalid_params_error, status=400)

    hourly_price_changes = extract_db_data(symbol, start_time_utc, "hour")

    hours_dict_values = calculate_dict_percentage(hourly_price_changes, "hour_of_day")

    hours_dict_values = dict(sorted(hours_dict_values.items()))

    formatted_data = format_data(hours_dict_values)
    xAxis = []

    int_values_array = [int(float(str(item))) for item in hours_dict_values.keys()]

    for hour in int_values_array:
        if hour < 10:
            xAxis.append(f"0{hour}:00")
        else:
            xAxis.append(f"{hour}:00")

    response = {
        "data": formatted_data,
        "xAxis": xAxis,
    }

    return response


@csrf_exempt
def get_average_prices(request):
    if request.method == "GET":
        duration = request.GET.get("duration", None)
        symbol = request.GET.get("symbol", None)
        type = request.GET.get("type", None)

        if duration is None or symbol is None or type is None:
            return JsonResponse(invalid_params_error, status=400)

        duration_hours = stats_select_options_htf[duration]
        start_time_utc = timezone.now() - timedelta(hours=duration_hours)

        if type == "day":
            response = average_price_change_per_day_of_week(
                duration, symbol, start_time_utc
            )
        elif type == "hour":
            response = average_price_change_per_hour_of_day(
                duration, symbol, start_time_utc
            )

        return JsonResponse(response, safe=False)

    return HttpResponse(status=405)
