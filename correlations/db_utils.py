from django.db import connection
from datetime import timedelta
from django.utils import timezone


def cleanup_old_correlation_data(retention_hours):
    cutoff_time = timezone.now() - timedelta(hours=retention_hours)

    with connection.cursor() as cursor:
        cursor.execute(
            "DELETE FROM cs_correlation_pair_history WHERE calculated_at < %s",
            [cutoff_time],
        )

        print(f"Deleted {cursor.rowcount} rows older than {cutoff_time}")
