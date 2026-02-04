from datetime import datetime, timezone as dt_timezone


def ms_to_aware_datetime(ms) -> datetime:
    """Convert a millisecond timestamp (epoch, UTC) to a timezone-aware UTC datetime.

    Accepts int/float/Decimal/str. Returns django timezone-aware datetime in UTC.
    """
    return datetime.fromtimestamp(int(ms) / 1000, tz=dt_timezone.utc)
