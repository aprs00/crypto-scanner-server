from datetime import datetime, timezone as dt_timezone
from django.utils import timezone


def ms_to_aware_datetime(ms) -> datetime:
    """Convert a millisecond timestamp (epoch, UTC) to a timezone-aware UTC datetime.

    Accepts int/float/Decimal/str. Returns django timezone-aware datetime in UTC.
    """
    return timezone.make_aware(datetime.fromtimestamp(int(ms) / 1000), dt_timezone.utc)
