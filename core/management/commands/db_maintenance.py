import time
from django.core.management.base import BaseCommand
from django.db import connection
from correlations.db_utils import cleanup_old_correlation_data
from zscore.services.db_utils import cleanup_old_zscore_data

INTERVAL_SECONDS = 300
CORRELATION_RETENTION_HOURS = 4
ZSCORE_RETENTION_HOURS = 12


class Command(BaseCommand):
    help = "Runs periodic database maintenance tasks"

    def handle(self, *args, **options):
        self.stdout.write(
            self.style.SUCCESS(
                f"Starting DB maintenance loop (interval={INTERVAL_SECONDS}s)"
            )
        )

        while True:
            try:
                cleanup_old_correlation_data(
                    retention_hours=CORRELATION_RETENTION_HOURS
                )
                cleanup_old_zscore_data(retention_hours=ZSCORE_RETENTION_HOURS)
            except Exception as e:
                self.stderr.write(self.style.ERROR(f"Cleanup failed: {e}"))
                connection.close()

            time.sleep(INTERVAL_SECONDS)
