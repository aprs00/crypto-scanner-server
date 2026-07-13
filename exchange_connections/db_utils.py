from datetime import timedelta

from django.db import connection
from django.utils import timezone


DEFAULT_KLINE_CLEANUP_BATCH_SIZE = 50_000
DEFAULT_KLINE_CLEANUP_MAX_BATCHES = 4


def cleanup_old_kline_data(
    retention_days: int,
    batch_size: int = DEFAULT_KLINE_CLEANUP_BATCH_SIZE,
    max_batches: int = DEFAULT_KLINE_CLEANUP_MAX_BATCHES,
) -> int:
    """Delete expired one-minute klines in bounded transactions.

    Kline ingestion is continuous and the table is large, so deleting the
    entire expired range in one statement can create a long transaction and a
    large WAL spike. Each statement removes at most ``batch_size`` rows and the
    number of statements per maintenance run is capped by ``max_batches``.
    """
    if retention_days <= 0:
        raise ValueError("retention_days must be greater than zero")
    if batch_size <= 0:
        raise ValueError("batch_size must be greater than zero")
    if max_batches <= 0:
        raise ValueError("max_batches must be greater than zero")

    cutoff_time = timezone.now() - timedelta(days=retention_days)
    total_deleted = 0

    for _ in range(max_batches):
        with connection.cursor() as cursor:
            cursor.execute(
                """
                WITH expired AS MATERIALIZED (
                    SELECT id
                    FROM cs_klines_1m
                    WHERE start_time < %s
                    ORDER BY start_time, id
                    LIMIT %s
                ),
                deleted AS (
                    DELETE FROM cs_klines_1m AS kline
                    USING expired
                    WHERE kline.id = expired.id
                    RETURNING 1
                )
                SELECT COUNT(*) FROM deleted
                """,
                [cutoff_time, batch_size],
            )
            row = cursor.fetchone()
            deleted_count = int(row[0]) if row else 0

        total_deleted += deleted_count
        if deleted_count < batch_size:
            break

    if total_deleted > 0:
        print(
            f"Cleaned up {total_deleted} klines older than {retention_days} days "
            f"(before {cutoff_time.isoformat()})"
        )

    return total_deleted
