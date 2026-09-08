import os
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace
from unittest import skipUnless
from unittest.mock import Mock, patch

from django.test import SimpleTestCase, TestCase

from footprints.management.commands.footprint_reconcile import Command
from footprints.models import (
    ChartSeries,
    FootprintMarket,
    InstrumentSpec,
    RecoveryStatus,
    TradeRecoveryDay,
)
from footprints.recovery import HOUR_US, Recovery, normalize_archive_row, publish_import


class ArchiveNormalizationTests(SimpleTestCase):
    def test_bybit_fractional_seconds_match_websocket_millisecond_precision(self):
        row = {
            "trdMatchID": "uuid",
            "timestamp": "1788307200.1431",
            "price": "68000.1",
            "size": "0.003",
            "side": "Buy",
        }
        self.assertEqual(
            normalize_archive_row(row, "bybit", 3, 6),
            ("uuid", 1788307200143000, 68000100, 3000, 1),
        )

    def test_rejects_non_exact_atoms_and_non_finite_numbers(self):
        row = {
            "id": "1",
            "time": "1788307200143",
            "price": "1.2345",
            "qty": "0.003",
            "is_buyer_maker": "false",
        }
        with self.assertRaises(ValueError):
            normalize_archive_row(row, "binance", 3, 6)
        row["price"] = "Infinity"
        with self.assertRaises(ValueError):
            normalize_archive_row(row, "binance", 3, 6)


class RecoveryTests(TestCase):
    @skipUnless(
        os.environ.get("FOOTPRINT_CLICKHOUSE_INTEGRATION") == "1",
        "requires real ClickHouse",
    )
    def test_full_repair_and_chart_build_against_isolated_clickhouse(self):
        from footprints.chart_service import build_series
        from footprints.storage.charts import (
            ClickHouseChartStore,
            ProjectorSpecDivisors,
        )
        from footprints.storage.clickhouse import (
            ClickHouseAccess,
            ClickHouseExecutionStore,
            ClickHouseSettings,
            create_clickhouse_client,
        )
        from footprints.storage.clickhouse_schema import (
            ensure_archive_schema,
            ensure_clickhouse_schema,
        )
        from footprints.storage.executions import StoredExecution
        from footprints.storage.reconciled import ReconciledExecutionStore
        from footprints.types import NormalizedTrade, TakerSide

        database = f"footprints_recovery_it_{uuid.uuid4().hex}"
        access = ClickHouseAccess(
            create_clickhouse_client(
                ClickHouseSettings.from_environment(),
                role="recovery-integration",
                for_inserts=True,
            )
        )
        try:
            ensure_clickhouse_schema(access, database)
            store = ClickHouseExecutionStore(access, database=database)
            store.insert_batch(
                tuple(
                    StoredExecution.from_trade(
                        market_id=self.market.pk,
                        instrument_spec_id=self.spec.pk,
                        source_epoch=1,
                        price_scale=3,
                        quantity_scale=6,
                        trade=NormalizedTrade(
                            market_key=self.market.market_key,
                            trade_id=row[0],
                            venue_sequence=row[6],
                            exchange_timestamp_us=row[1],
                            price=Decimal("100"),
                            quantity=Decimal("0.002"),
                            taker_side=TakerSide.BUY,
                        ),
                    )
                    for row in self.live
                )
            )
            self.schema.side_effect = ensure_archive_schema
            report = Recovery(access, database).repair_day(
                self.market, self.day, repair=True
            )
            self.assertEqual(report["missing_trades"], 1)
            replacement = ChartSeries.objects.get(market=self.market)
            replacement.status = "building"
            replacement.save(update_fields=["status"])
            reader = ReconciledExecutionStore(
                access,
                database=database,
                spec_divisors={self.spec.pk: ProjectorSpecDivisors(1, 1)},
            )
            build_series(
                replacement.pk,
                execution_store=reader,
                bar_store=ClickHouseChartStore(access, database=database),
            )
            replacement.refresh_from_db()
            self.assertEqual(replacement.status, "ready")
            self.assertEqual(
                replacement.checkpoint_state["cursor"],
                [-2, self.us // 86_400_000_000, 0, 1, 2, 0, self.us + 2000, "3"],
            )
            self.assertEqual(
                access.query(
                    f"SELECT count() FROM {database}.raw_executions"
                ).result_rows[0][0],
                2,
            )
            self.assertEqual(
                access.query(
                    f"SELECT count() FROM {database}.archive_executions"
                ).result_rows[0][0],
                3,
            )
        finally:
            # Only this method's validated, UUID-named test database is removed.
            uuid.UUID(hex=database.removeprefix("footprints_recovery_it_"))
            access.command(f"DROP DATABASE IF EXISTS {database} SYNC")
            access.close()

    def setUp(self):
        self.day = datetime.now(timezone.utc).date() - timedelta(days=1)
        self.start = datetime.combine(self.day, datetime.min.time(), timezone.utc)
        self.us = int(self.start.timestamp()) * 1_000_000
        self.market = FootprintMarket.objects.create(
            exchange="binance",
            symbol="RECOVERYTESTUSDT",
            base_asset="BTC",
            price_scale=3,
            quantity_scale=6,
        )
        self.spec = InstrumentSpec.objects.create(
            market=self.market,
            effective_from=self.start - timedelta(days=1),
            tick_size="0.001",
            quantity_step="0.000001",
        )
        self.series = ChartSeries.objects.create(
            market=self.market,
            chart_type="tick",
            parameter_a=2,
            anchor_at=self.start,
            status="ready",
        )
        self.rows = [
            {
                "id": str(i + 1),
                "time": str((self.us + i * 1000) // 1000),
                "price": "100",
                "qty": "0.002",
                "is_buyer_maker": "false",
            }
            for i in range(3)
        ]
        self.live = [
            ("1", self.us, 100000, 2000, True, 1, 1, 0),
            ("3", self.us + 2000, 100000, 2000, True, 1, 3, 0),
        ]
        self.inserted = []
        self.access = Mock()
        self.access.query.side_effect = self.query
        self.access.stream_rows.side_effect = self.stream
        self.access.insert.side_effect = lambda _table, rows, **_kwargs: (
            self.inserted.extend(rows)
        )
        self.recovery = Recovery(self.access, "test_recovery")
        for target, value in (
            ("download_archive", (None, "abc")),
            ("archive_rows", self.rows),
            (
                "candles",
                {
                    self.us + i * HOUR_US: (
                        3 if i == 0 else 0,
                        Decimal("0.006") if i == 0 else Decimal(0),
                    )
                    for i in range(24)
                },
            ),
        ):
            patcher = patch(f"footprints.recovery.{target}", return_value=value)
            patcher.start()
            self.addCleanup(patcher.stop)
        self.schema = patch("footprints.recovery.ensure_archive_schema").start()
        self.addCleanup(patch.stopall)

    def query(self, _sql, *, operation, **kwargs):
        if operation == "recovery_bounds":
            return SimpleNamespace(result_rows=[(self.us, self.us + 2000)])
        if operation in ("resume_archive_batch", "verify_archive_import"):
            return SimpleNamespace(
                result_rows=[
                    (len(self.inserted), len({row[3] for row in self.inserted}))
                ]
            )
        raise AssertionError(operation)

    def stream(self, *_args, **_kwargs):
        yield from self.live

    def test_default_audit_reports_missing_ids_without_writes(self):
        report = self.recovery.repair_day(self.market, self.day)
        self.assertEqual(report["missing_trades"], 1)
        self.assertEqual(Decimal(report["missing_volume"]), Decimal("0.002"))
        self.assertFalse(TradeRecoveryDay.objects.exists())
        self.assertTrue(ChartSeries.objects.filter(pk=self.series.pk).exists())
        self.access.insert.assert_not_called()
        self.schema.assert_not_called()

    def test_publish_replaces_series_and_retries_are_idempotent(self):
        report = self.recovery.repair_day(self.market, self.day, repair=True)
        record = TradeRecoveryDay.objects.get(market=self.market, day=self.day)
        self.assertEqual(record.status, RecoveryStatus.VERIFIED)
        self.assertIsNotNone(record.active_import)
        self.market.refresh_from_db()
        self.assertTrue(self.market.recovery_enabled)
        replacement = ChartSeries.objects.get(market=self.market)
        self.assertNotEqual(replacement.pk, self.series.pk)
        self.assertNotEqual(replacement.public_key, self.series.public_key)
        self.assertEqual(replacement.anchor_at, self.series.anchor_at)
        self.assertEqual(replacement.last_requested_at, self.series.last_requested_at)
        self.assertEqual(replacement.status, "queued")
        self.assertEqual(
            self.recovery.repair_day(self.market, self.day, repair=True), report
        )
        self.recovery.repair_day(self.market, self.day, repair=True, refresh=True)
        self.assertEqual(len(self.inserted), 3)
        self.assertEqual(ChartSeries.objects.get(market=self.market).pk, replacement.pk)

    def test_conflicting_identity_never_publishes(self):
        self.rows[0]["price"] = "101"
        with self.assertRaisesRegex(ValueError, "disagrees with live"):
            self.recovery.repair_day(self.market, self.day, repair=True)
        record = TradeRecoveryDay.objects.get(market=self.market)
        self.assertEqual(record.status, RecoveryStatus.FAILED)
        self.assertIsNone(record.active_import)
        self.assertTrue(ChartSeries.objects.filter(pk=self.series.pk).exists())
        self.assertFalse(self.inserted)

    def test_successful_refresh_clears_failure_without_replacing_identical_import(self):
        self.recovery.repair_day(self.market, self.day, repair=True)
        replacement = ChartSeries.objects.get(market=self.market)
        TradeRecoveryDay.objects.filter(market=self.market).update(
            status=RecoveryStatus.FAILED, error="temporary checksum download failure"
        )
        self.recovery.repair_day(self.market, self.day, repair=True, refresh=True)
        record = TradeRecoveryDay.objects.get(market=self.market)
        self.assertEqual(record.status, RecoveryStatus.VERIFIED)
        self.assertEqual(record.error, "")
        self.assertEqual(ChartSeries.objects.get(market=self.market).pk, replacement.pk)

    def test_retired_worker_cannot_publish_a_checkpoint_to_replacement(self):
        from footprints.chart_service import _pending_build, _persist_build_state

        pending = _pending_build(self.series)
        self.recovery.repair_day(self.market, self.day, repair=True)
        with self.assertRaisesRegex(RuntimeError, "changed during build"):
            _persist_build_state([pending], complete=True)
        replacement = ChartSeries.objects.get(market=self.market)
        self.assertEqual(replacement.status, "queued")
        self.assertIsNone(replacement.checkpoint_state)

    def test_duplicate_archive_identity_is_not_silently_accepted(self):
        self.rows.insert(1, dict(self.rows[0]))
        with self.assertRaisesRegex(ValueError, "duplicate execution"):
            self.recovery.repair_day(self.market, self.day, repair=True)
        self.assertFalse(self.inserted)

    def test_extra_live_execution_blocks_publication(self):
        self.live.append(("4", self.us + 3000, 100000, 2000, True, 1, 4, 0))
        with self.assertRaisesRegex(ValueError, "absent_from_archive=1"):
            self.recovery.repair_day(self.market, self.day, repair=True)

    def test_hourly_disagreement_blocks_publication(self):
        self.rows[1]["qty"] = "0.003"
        with self.assertRaisesRegex(ValueError, "exchange candle"):
            self.recovery.repair_day(self.market, self.day, repair=True)

    def test_new_publisher_cannot_enable_hyperliquid(self):
        self.market.exchange = "hyperliquid"
        self.market.save()
        with self.assertRaises(ValueError):
            publish_import(self.market.pk, self.day, self.us, uuid.uuid4(), "abc", {})
        self.market.refresh_from_db()
        self.assertFalse(self.market.recovery_enabled)

    def test_archive_scheduler_retries_after_six_hours_and_skips_verified_days(self):
        now = datetime.now(timezone.utc)
        TradeRecoveryDay.objects.create(
            market=self.market,
            day=self.day,
            start_us=self.us,
            status=RecoveryStatus.PENDING,
            retry_at=now + timedelta(hours=6),
        )
        self.assertIsNone(Command._next_day(self.market, self.us, now, 30))
        self.assertEqual(
            Command._next_day(self.market, self.us, now + timedelta(hours=7), 30),
            self.day,
        )
