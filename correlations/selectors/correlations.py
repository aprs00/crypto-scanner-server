from django.utils import timezone
from datetime import timedelta

from correlations.models import CorrelationPairHistory


def get_correlation_pair_history(symbol_a_name, symbol_b_name, data_type, hours, days_back=30):
    """
    Retrieve historical correlation data for a specific symbol pair.

    Args:
        symbol_a_name: First symbol name (e.g., "BTCUSDT")
        symbol_b_name: Second symbol name (e.g., "ETHUSDT")
        data_type: Type of data (price, volume, or trades)
        hours: The timeframe in hours (e.g., 1, 4, 12, 24)
        days_back: Number of days of history to retrieve

    Returns:
        List of dictionaries with structure:
        [
            {'timestamp': '2024-01-01T00:00:00', 'correlation': 0.85},
            {'timestamp': '2024-01-01T01:00:00', 'correlation': 0.87},
            ...
        ]
    """
    cutoff_time = timezone.now() - timedelta(days=days_back)

    history_records = CorrelationPairHistory.objects.filter(
        symbol_a__name=symbol_a_name,
        symbol_b__name=symbol_b_name,
        data_type=data_type,
        hours=hours,
        calculated_at__gte=cutoff_time
    ).select_related('symbol_a', 'symbol_b').order_by('calculated_at')

    result = [
        {
            'timestamp': record.calculated_at.isoformat(),
            'correlation': round(record.correlation_value, 2)
        }
        for record in history_records
    ]

    return result


