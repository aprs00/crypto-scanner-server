import os
from unittest.mock import patch

from django.test import SimpleTestCase

from core.management.commands import db_maintenance


class DatabaseMaintenanceTests(SimpleTestCase):
    def test_kline_cleanup_is_enabled_by_default(self):
        with patch.dict(os.environ, {}, clear=True):
            self.assertTrue(
                db_maintenance._env_flag("KLINE_CLEANUP_ENABLED", default=True)
            )

    def test_explicit_false_disables_kline_cleanup(self):
        with patch.dict(os.environ, {"KLINE_CLEANUP_ENABLED": "false"}):
            self.assertFalse(
                db_maintenance._env_flag("KLINE_CLEANUP_ENABLED", default=True)
            )

    def test_enabled_cycle_runs_every_cleanup(self):
        with (
            patch.object(db_maintenance, "cleanup_old_correlation_data") as correlation,
            patch.object(db_maintenance, "cleanup_old_cointegration_data") as cointegration,
            patch.object(db_maintenance, "cleanup_old_zscore_data") as zscore,
            patch.object(db_maintenance, "cleanup_old_kline_data") as klines,
        ):
            db_maintenance._run_cleanup_cycle(kline_cleanup_enabled=True)

        correlation.assert_called_once_with(
            retention_hours=db_maintenance.CORRELATION_RETENTION_HOURS
        )
        cointegration.assert_called_once_with(
            retention_hours=db_maintenance.COINTEGRATION_RETENTION_HOURS
        )
        zscore.assert_called_once_with(
            retention_hours=db_maintenance.ZSCORE_RETENTION_HOURS
        )
        klines.assert_called_once_with(retention_days=db_maintenance.KLINE_RETENTION_DAYS)

    def test_disabled_cycle_only_skips_kline_cleanup(self):
        with (
            patch.object(db_maintenance, "cleanup_old_correlation_data"),
            patch.object(db_maintenance, "cleanup_old_cointegration_data"),
            patch.object(db_maintenance, "cleanup_old_zscore_data"),
            patch.object(db_maintenance, "cleanup_old_kline_data") as klines,
        ):
            db_maintenance._run_cleanup_cycle(kline_cleanup_enabled=False)

        klines.assert_not_called()
