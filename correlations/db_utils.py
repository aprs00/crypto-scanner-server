from django.db import connection
from datetime import datetime, timedelta
from django.utils import timezone


def cleanup_old_correlation_data(retention_hours):
    """
    Drop old hourly partitions that are outside the retention window.
    Instant operation - no dead tuples, no bloat.
    """
    cutoff_time = timezone.now() - timedelta(hours=retention_hours)
    total_dropped = 0

    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT tablename FROM pg_tables
            WHERE schemaname = 'public'
            AND tablename LIKE 'cs_correlation_pair_history_%%'
            AND tablename ~ '^cs_correlation_pair_history_[0-9]{10}$'
            """
        )
        partitions = cursor.fetchall()

        for (partition_name,) in partitions:
            try:
                ts_str = partition_name.replace("cs_correlation_pair_history_", "")
                partition_time = datetime.strptime(ts_str, "%Y%m%d%H")
                partition_time = timezone.make_aware(partition_time)

                if partition_time < cutoff_time - timedelta(hours=1):
                    cursor.execute(f"DROP TABLE IF EXISTS {partition_name}")
                    total_dropped += 1
            except (ValueError, TypeError):
                continue

    if total_dropped > 0:
        print(f"Dropped {total_dropped} partitions older than {cutoff_time}")


def ensure_partition_exists(target_time):
    """
    Ensure partition exists for the given hour (or current hour if not specified).
    Creates partition if it doesn't exist.
    """
    hour_start = target_time.replace(minute=0, second=0, microsecond=0)
    hour_end = hour_start + timedelta(hours=1)
    partition_name = f"cs_correlation_pair_history_{hour_start.strftime('%Y%m%d%H')}"

    with connection.cursor() as cursor:
        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {partition_name}
            PARTITION OF cs_correlation_pair_history
            FOR VALUES FROM (%s) TO (%s)
            """,
            [hour_start, hour_end],
        )

    return partition_name
