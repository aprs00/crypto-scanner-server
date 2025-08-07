from datetime import datetime
from django.utils import timezone


def ms_to_datetime(ms_timestamp):
    try:
        timestamp_sec = int(ms_timestamp) / 1000.0
        return datetime.fromtimestamp(timestamp_sec, tz=timezone.utc)
    except (ValueError, TypeError) as e:
        return None
