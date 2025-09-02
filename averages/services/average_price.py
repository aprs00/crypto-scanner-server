from django.http import JsonResponse
from typing import Literal
from django.utils import timezone
from datetime import timedelta
import numpy as np

from core.constants import invalid_params_error
from averages.selectors.average_price import get_average_symbol_data


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


def average_price_change(hours, symbol, time_period: Literal["hour", "day"]):
    start_time_utc = timezone.now() - timedelta(hours=hours)

    price_changes = get_average_symbol_data(
        symbol=symbol,
        exchange="binance",
        start_time_utc=start_time_utc,
        group_by=time_period,
        contract_type="perpetual",
    )

    if time_period == "day":
        time_dict_values = calculate_dict_percentage(price_changes, "day_of_week")
        time_labels = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]
    elif time_period == "hour":
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
