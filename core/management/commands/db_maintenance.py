import os
import time
from django.core.management.base import BaseCommand
from django.db import connection
from correlations.db_utils import cleanup_old_correlation_data
from cointegration.db_utils import cleanup_old_cointegration_data
from exchange_connections.db_utils import cleanup_old_kline_data
from zscore.services.db_utils import cleanup_old_zscore_data

INTERVAL_SECONDS = 300
CORRELATION_RETENTION_HOURS = 4
COINTEGRATION_RETENTION_HOURS = 8
ZSCORE_RETENTION_HOURS = 12
KLINE_RETENTION_DAYS = 30


def _env_flag(name: str, *, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _run_cleanup_cycle(*, kline_cleanup_enabled: bool) -> None:
    cleanup_old_correlation_data(retention_hours=CORRELATION_RETENTION_HOURS)
    cleanup_old_cointegration_data(retention_hours=COINTEGRATION_RETENTION_HOURS)
    cleanup_old_zscore_data(retention_hours=ZSCORE_RETENTION_HOURS)
    if kline_cleanup_enabled:
        cleanup_old_kline_data(retention_days=KLINE_RETENTION_DAYS)


class Command(BaseCommand):
    help = "Runs periodic database maintenance tasks"

    def handle(self, *args, **options):
        kline_cleanup_enabled = _env_flag("KLINE_CLEANUP_ENABLED", default=True)
        self.stdout.write(
            self.style.SUCCESS(
                f"Starting DB maintenance loop (interval={INTERVAL_SECONDS}s, "
                f"kline_cleanup_enabled={kline_cleanup_enabled})"
            )
        )

        while True:
            try:
                _run_cleanup_cycle(kline_cleanup_enabled=kline_cleanup_enabled)
            except Exception as e:
                self.stderr.write(self.style.ERROR(f"Cleanup failed: {e}"))
                connection.close()

            time.sleep(INTERVAL_SECONDS)
