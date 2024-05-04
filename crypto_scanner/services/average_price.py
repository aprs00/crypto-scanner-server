from django.http import JsonResponse

import numpy as np


from crypto_scanner.constants import invalid_params_error
from crypto_scanner.selectors.average_price import get_market_data


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


def average_price_change(duration, symbol, start_time_utc, time_period):
    if duration is None or symbol is None:
        return JsonResponse(invalid_params_error, status=400)

    if time_period == "day":
        price_changes = get_market_data(symbol, start_time_utc, "day")
        time_dict_values = calculate_dict_percentage(price_changes, "day_of_week")
        time_labels = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]
    elif time_period == "hour":
        price_changes = get_market_data(symbol, start_time_utc, "hour")
        time_dict_values = calculate_dict_percentage(price_changes, "hour_of_day")
        time_dict_values = dict(sorted(time_dict_values.items()))
        time_labels = [
            f"{hour}:00" if hour >= 10 else f"0{hour}:00" for hour in range(24)
        ]
    else:
        return JsonResponse(
            {"error": "Invalid time period. Choose either 'day' or 'hour'."}, status=400
        )

    formatted_data = format_data(time_dict_values)
    x_axis = [time_labels[int(float(str(item)))] for item in time_dict_values.keys()]

    response = {
        "data": formatted_data,
        "xAxis": x_axis,
    }

    return response


# def average_price_change_per_day_of_week(duration, symbol, start_time_utc):
#     if duration is None or symbol is None:
#         return JsonResponse(invalid_params_error, status=400)
#
#     daily_price_changes = get_market_data(symbol, start_time_utc, "day")
#
#     weekdays = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]
#
#     weekdays_dict_values = calculate_dict_percentage(daily_price_changes, "day_of_week")
#
#     formatted_data = format_data(weekdays_dict_values)
#     x_axis = []
#
#     int_values_array = [int(float(str(item))) for item in weekdays_dict_values.keys()]
#
#     for day in int_values_array:
#         x_axis.append(weekdays[day])
#
#     response = {
#         "data": formatted_data,
#         "xAxis": x_axis,
#     }
#
#     return response
#
#
# def average_price_change_per_hour_of_day(duration, symbol, start_time_utc):
#     if duration is None or symbol is None:
#         return JsonResponse(invalid_params_error, status=400)
#
#     hourly_price_changes = get_market_data(symbol, start_time_utc, "hour")
#
#     hours_dict_values = calculate_dict_percentage(hourly_price_changes, "hour_of_day")
#
#     hours_dict_values = dict(sorted(hours_dict_values.items()))
#
#     formatted_data = format_data(hours_dict_values)
#     xAxis = []
#
#     int_values_array = [int(float(str(item))) for item in hours_dict_values.keys()]
#
#     for hour in int_values_array:
#         if hour < 10:
#             xAxis.append(f"0{hour}:00")
#         else:
#             xAxis.append(f"{hour}:00")
#
#     response = {
#         "data": formatted_data,
#         "xAxis": xAxis,
#     }
#
#     return response
