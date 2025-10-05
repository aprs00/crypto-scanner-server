from django.http import JsonResponse
from django.utils import timezone
from datetime import timedelta
import numpy as np
from collections import defaultdict
from enum import Enum

from averages.selectors.average_price import get_average_symbol_data


class TimePeriod(Enum):
    DAY = "day"
    HOUR = "hour"


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


def calculate_dict_percentage(klines, group_by: TimePeriod):
    """Calculate percentage change grouped by time period using numpy."""
    if not klines:
        return {}, []

    grouped_periods = defaultdict(list)

    for kline in klines:
        match group_by:
            case TimePeriod.DAY:
                group_key = kline["start_time"].weekday()
            case TimePeriod.HOUR:
                group_key = kline["start_time"].hour

        grouped_periods[group_key].append(kline)

    calculated_data = {}
    for group_key, period_klines in grouped_periods.items():
        individual_periods = defaultdict(list)

        for kline in period_klines:
            match group_by:
                case TimePeriod.DAY:
                    period_key = kline["start_time"].date()
                case TimePeriod.HOUR:
                    period_key = kline["start_time"].replace(
                        minute=0, second=0, microsecond=0
                    )

            individual_periods[period_key].append(kline)

        percentages = []
        for period_data in individual_periods.values():
            first_open = period_data[0]["open"]
            last_close = period_data[-1]["close"]
            percentage = (last_close - first_open) / last_close * 100
            percentages.append(percentage)

        calculated_data[group_key] = percentages

    match group_by:
        case TimePeriod.DAY:
            time_labels = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]
        case TimePeriod.HOUR:
            calculated_data = dict(sorted(calculated_data.items()))
            time_labels = [
                f"{hour}:00" if hour >= 10 else f"0{hour}:00" for hour in range(24)
            ]

    return calculated_data, time_labels


def average_price_change(hours, symbol, time_period: str):
    start_time_utc = timezone.now() - timedelta(hours=hours)

    try:
        period_enum = TimePeriod(time_period)
    except ValueError:
        return JsonResponse(
            {"error": "Invalid time period. Choose either 'day' or 'hour'."}, status=400
        )

    price_changes = get_average_symbol_data(
        symbol=symbol,
        exchange="binance",
        start_time_utc=start_time_utc,
        group_by=time_period,
        contract_type="perpetual",
    )

    time_dict_values, time_labels = calculate_dict_percentage(
        price_changes, period_enum
    )

    formatted_data = format_data(time_dict_values)
    x_axis = [time_labels[int(float(str(item)))] for item in time_dict_values.keys()]

    response = {
        "data": formatted_data,
        "xAxis": x_axis,
    }

    return response
