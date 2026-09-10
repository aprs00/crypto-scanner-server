from datetime import timedelta

from django.db import connection
from django.db.migrations.executor import MigrationExecutor
from django.test import TransactionTestCase
from django.utils import timezone


class ContinuousTrevMigrationTests(TransactionTestCase):
    def test_rebuilds_distinct_trev_lookbacks_once_without_changing_tick(self):
        old_target = [("footprints", "0003_trade_recovery")]
        new_target = [("footprints", "0005_continuous_trev_constraints")]
        executor = MigrationExecutor(connection)
        executor.migrate(old_target)
        try:
            apps = executor.loader.project_state(old_target).apps
            market = apps.get_model("footprints", "FootprintMarket").objects.create(
                exchange="binance",
                symbol="MIGRATIONTEST",
                base_asset="TEST",
            )
            series = apps.get_model("footprints", "ChartSeries")
            now = timezone.now()
            tick = series.objects.create(
                market=market,
                chart_type="tick",
                parameter_a=500,
                lookback_days=5,
                anchor_at=now - timedelta(days=5),
                status="ready",
                checkpoint_state={"tick": True},
            )
            old = [
                series.objects.create(
                    market=market,
                    chart_type="trev",
                    parameter_a=314,
                    parameter_b=471,
                    lookback_days=days,
                    anchor_at=now - timedelta(days=days),
                    status="ready",
                    checkpoint_state={"incompatible_origin": days},
                    last_requested_at=now - timedelta(hours=days),
                )
                for days in (1, 5, 30)
            ]
            executor = MigrationExecutor(connection)
            executor.migrate(new_target)
            apps = executor.loader.project_state(new_target).apps
            rows = apps.get_model("footprints", "ChartSeries").objects.filter(
                market_id=market.pk
            )
            replacement = rows.get(chart_type="trev")
            self.assertEqual(rows.count(), 2)
            self.assertNotIn(replacement.pk, [row.pk for row in old])
            self.assertNotIn(replacement.public_key, [row.public_key for row in old])
            self.assertEqual(replacement.last_requested_at, old[0].last_requested_at)
            self.assertEqual(
                replacement.anchor_at,
                (now - timedelta(days=30)).replace(minute=0, second=0, microsecond=0),
            )
            self.assertIsNone(replacement.lookback_days)
            self.assertIsNone(replacement.checkpoint_state)
            self.assertEqual(replacement.status, "queued")
            unchanged = rows.get(chart_type="tick")
            self.assertEqual(unchanged.pk, tick.pk)
            self.assertEqual(unchanged.public_key, tick.public_key)
            self.assertEqual(unchanged.checkpoint_state, tick.checkpoint_state)
            self.assertEqual(unchanged.anchor_at, tick.anchor_at)
        finally:
            MigrationExecutor(connection).migrate(new_target)
