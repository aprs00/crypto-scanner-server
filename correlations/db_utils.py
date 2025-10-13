from datetime import timedelta
from django.db import connection
from django.utils import timezone

from correlations.models import CorrelationPairHistory


def cleanup_old_correlation_data(retention_hours, batch_size=30000) -> int:
    """
    Delete correlation records older than the specified retention period.

    Returns:
        Total number of records deleted
    """
    try:
        cutoff_time = timezone.now() - timedelta(hours=retention_hours)
        total_deleted = 0

        with connection.cursor() as cursor:
            while True:
                cursor.execute(
                    f"""
                    DELETE FROM {CorrelationPairHistory._meta.db_table}
                    WHERE id IN (
                        SELECT id FROM {CorrelationPairHistory._meta.db_table}
                        WHERE calculated_at < %s
                        LIMIT %s
                    )
                    """,
                    [cutoff_time, batch_size],
                )

                deleted_in_batch = cursor.rowcount
                total_deleted += deleted_in_batch

                if deleted_in_batch == 0:
                    break

                if total_deleted % (batch_size * 10) == 0:
                    print(f"Deleted {total_deleted} correlation records so far...")

        if total_deleted > 0:
            print(
                f"Cleaned up {total_deleted} correlation records older than {retention_hours} hours "
                f"(before {cutoff_time})"
            )

        return total_deleted

    except Exception as e:
        print(f"Error cleaning up old correlation data: {e}")
        return 0
