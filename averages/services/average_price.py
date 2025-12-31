from django.http import JsonResponse
from django.utils import timezone
from datetime import timedelta

from averages.selectors.average_price import get_average_price_change_by_period
from averages.constants import TimePeriod


def format_data(data, time_period: TimePeriod):
    """Format data for the frontend with colors based on positive/negative values."""
    match time_period:
        case TimePeriod.DAY:
            time_labels = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]
            formatted_data = []
            for i in range(7):
                value = data.get(i, 0.0)
                item_style = {"color": "#4393c3" if value > 0 else "#a50f15"}
                formatted_data.append(
                    {"itemStyle": item_style, "value": round(value, 2)}
                )
        case TimePeriod.HOUR:
            time_labels = [f"{hour:02d}:00" for hour in range(24)]
            formatted_data = []
            for hour in range(24):
                value = data.get(hour, 0.0)
                item_style = {"color": "#4393c3" if value > 0 else "#a50f15"}
                formatted_data.append(
                    {"itemStyle": item_style, "value": round(value, 2)}
                )

    return formatted_data, time_labels


def average_price_change(
    hours, symbol, time_period: str, exchange: str, contract_type: str
):
    start_time_utc = timezone.now() - timedelta(hours=hours)

    try:
        period_enum = TimePeriod(time_period)
    except ValueError:
        return JsonResponse(
            {"error": "Invalid time period. Choose either 'day' or 'hour'."}, status=400
        )

    price_change_data = get_average_price_change_by_period(
        symbol=symbol,
        exchange=exchange,
        start_time_utc=start_time_utc,
        contract_type=contract_type,
        period_type=period_enum,
    )

    formatted_data, time_labels = format_data(price_change_data, period_enum)

    response = {
        "data": formatted_data,
        "xAxis": time_labels,
    }

    return response
