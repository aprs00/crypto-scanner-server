import hashlib
from datetime import datetime, timedelta
from django.db import connection
from django.utils import timezone


def cleanup_old_cointegration_data(retention_hours: int) -> int:
    cutoff_time = timezone.now() - timedelta(hours=retention_hours)
    total_dropped = 0

    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT tablename FROM pg_tables
            WHERE schemaname = 'public'
            AND tablename LIKE 'cs_cointegration_pair_history_%'
            AND tablename ~ '^cs_cointegration_pair_history_[0-9]{10}$'
            """
        )
        partitions = cursor.fetchall()

        for (partition_name,) in partitions:
            try:
                ts_str = partition_name.replace("cs_cointegration_pair_history_", "")
                partition_time = datetime.strptime(ts_str, "%Y%m%d%H")
                partition_time = timezone.make_aware(partition_time)

                if partition_time < cutoff_time - timedelta(hours=1):
                    cursor.execute(f"DROP TABLE IF EXISTS {partition_name}")
                    total_dropped += 1
            except (ValueError, TypeError):
                continue

    if total_dropped > 0:
        print(f"Dropped {total_dropped} cointegration partitions older than {cutoff_time}")

    return total_dropped


def ensure_partition_exists(target_time):
    hour_start = target_time.replace(minute=0, second=0, microsecond=0)
    hour_end = hour_start + timedelta(hours=1)
    partition_name = f"cs_cointegration_pair_history_{hour_start.strftime('%Y%m%d%H')}"

    with connection.cursor() as cursor:
        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {partition_name}
            PARTITION OF cs_cointegration_pair_history
            FOR VALUES FROM (%s) TO (%s)
            """,
            [hour_start, hour_end],
        )

    return partition_name


def _lock_key(exchange: str, contract_type: str, window_minutes: int) -> tuple[int, int]:
    payload = (
        f"cointegration:{exchange}:{contract_type}:{window_minutes}".encode("utf-8")
    )
    digest = hashlib.blake2b(payload, digest_size=8).digest()
    high = int.from_bytes(digest[:4], "big", signed=False)
    low = int.from_bytes(digest[4:], "big", signed=False)

    # pg_try_advisory_lock expects signed 32-bit ints for the (int, int) variant.
    if high >= 2**31:
        high -= 2**32
    if low >= 2**31:
        low -= 2**32

    return high, low


def try_acquire_lock(exchange: str, contract_type: str, window_minutes: int) -> bool:
    lock_id = _lock_key(exchange, contract_type, window_minutes)
    with connection.cursor() as cursor:
        cursor.execute("SELECT pg_try_advisory_lock(%s, %s)", lock_id)
        row = cursor.fetchone()
    return bool(row and row[0])


def release_lock(exchange: str, contract_type: str, window_minutes: int) -> None:
    lock_id = _lock_key(exchange, contract_type, window_minutes)
    with connection.cursor() as cursor:
        cursor.execute("SELECT pg_advisory_unlock(%s, %s)", lock_id)
