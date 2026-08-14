from __future__ import annotations

import os
import uuid
from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal
from unittest import skipUnless

from django.test import SimpleTestCase

from footprints.storage.charts import (
    ChartLevel,
    ClickHouseChartStore,
    ClickHouseProjectorExecutionStore,
    DetailedChartBar,
    ProjectorSpecDivisors,
)
from footprints.storage.clickhouse import (
    ClickHouseAccess,
    ClickHouseExecutionStore,
    ClickHouseSettings,
    ExecutionIdentityConflict,
    create_clickhouse_client,
    quote_identifier,
)
from footprints.storage.clickhouse_schema import ensure_clickhouse_schema
from footprints.storage.executions import StoredExecution
from footprints.types import NormalizedTrade, TakerSide


INTEGRATION_ENABLED = os.environ.get("FOOTPRINT_CLICKHOUSE_INTEGRATION") == "1"
CURRENT_EVENT_US = int(datetime.now(timezone.utc).timestamp()) * 1_000_000


class _CommitThenLoseAcknowledgement:
    """Commit through the real client, then emulate a lost HTTP response."""

    def __init__(self, delegate: ClickHouseAccess):
        self.delegate = delegate
        self.inserted = False

    def __getattr__(self, name):
        return getattr(self.delegate, name)

    def insert(self, *args, **kwargs):
        result = self.delegate.insert(*args, **kwargs)
        self.inserted = True
        raise TimeoutError("simulated lost ClickHouse insert acknowledgement")


@skipUnless(
    INTEGRATION_ENABLED,
    "set FOOTPRINT_CLICKHOUSE_INTEGRATION=1 to run against real ClickHouse",
)
class RealClickHouseExecutionStoreTests(SimpleTestCase):
    """Real-server contracts in a test-owned UUID database."""

    databases = set()

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.test_database = f"footprints_it_{uuid.uuid4().hex}"
        cls.base_configuration = ClickHouseSettings.from_environment()
        cls.admin_access = ClickHouseAccess(
            create_clickhouse_client(
                cls.base_configuration,
                role="integration-admin",
                bootstrap_database=True,
            )
        )
        cls.data_access = None
        cls.addClassCleanup(cls._cleanup_test_database)

        ensure_clickhouse_schema(cls.admin_access, cls.test_database)
        cls.test_configuration = replace(
            cls.base_configuration,
            database=cls.test_database,
        )
        cls.data_access = ClickHouseAccess(
            create_clickhouse_client(
                cls.test_configuration,
                role="integration-data",
                for_inserts=True,
            )
        )
        cls.store = ClickHouseExecutionStore(
            cls.data_access,
            database=cls.test_database,
        )
        cls.chart_store = ClickHouseChartStore(
            cls.data_access,
            database=cls.test_database,
        )

    @classmethod
    def _cleanup_test_database(cls):
        data_access = getattr(cls, "data_access", None)
        admin_access = getattr(cls, "admin_access", None)
        test_database = getattr(cls, "test_database", "")
        try:
            if data_access is not None:
                data_access.close()
        finally:
            if admin_access is not None:
                try:
                    cls._drop_owned_database(test_database)
                finally:
                    admin_access.close()

    @classmethod
    def _drop_owned_database(cls, database: str):
        prefix = "footprints_it_"
        suffix = database.removeprefix(prefix)
        if not database.startswith(prefix) or len(suffix) != 32:
            raise RuntimeError("refusing to drop a non-integration ClickHouse database")
        uuid.UUID(hex=suffix)
        cls.admin_access.command(
            f"DROP DATABASE IF EXISTS {quote_identifier(database)} SYNC",
            operation="drop_integration_database",
        )

    @staticmethod
    def execution(
        *,
        market_id: int,
        source_epoch: int,
        sequence: int,
        trade_id: str,
        timestamp_us: int,
        price: str = "100.125",
        quantity: str = "0.002500",
    ) -> StoredExecution:
        trade = NormalizedTrade(
            market_key="binance:perpetual:BTCUSDT",
            exchange_timestamp_us=timestamp_us,
            trade_id=trade_id,
            venue_sequence=sequence,
            price=Decimal(price),
            quantity=Decimal(quantity),
            taker_side=TakerSide.BUY,
        )
        return StoredExecution.from_trade(
            market_id=market_id,
            instrument_spec_id=11,
            source_epoch=source_epoch,
            price_scale=3,
            quantity_scale=6,
            trade=trade,
        )

    @staticmethod
    def batch(*executions: StoredExecution) -> tuple[StoredExecution, ...]:
        return executions

    @staticmethod
    def chart_bar(*, series_id: int, bar_index: int) -> DetailedChartBar:
        at = datetime.now(timezone.utc)
        return DetailedChartBar(
            series_id=series_id,
            bar_index=bar_index,
            opened_at=at,
            ended_at=at,
            instrument_spec_id=11,
            open_tick=100,
            high_tick=101,
            low_tick=100,
            close_tick=101,
            buy_units=2,
            sell_units=1,
            trade_count=3,
            break_before=False,
            levels=(ChartLevel(100, 1, 1), ChartLevel(101, 1, 0)),
        )

    @classmethod
    def _raw_rows(cls, market_id: int):
        return cls.data_access.query(
            f"""
            SELECT
                source_epoch, source_sequence, timestamp_us, trade_id,
                price_atoms, quantity_atoms, instrument_spec_id
            FROM {quote_identifier(cls.test_database)}.`raw_executions`
            WHERE market_id = {{market_id:UInt64}}
            ORDER BY source_epoch, source_sequence, timestamp_us, trade_id
            """,
            parameters={"market_id": market_id},
            operation="read_integration_raw_rows",
        ).result_rows

    def test_exact_execution_atoms_are_inserted(self):
        execution = self.execution(
            market_id=101,
            source_epoch=4,
            sequence=9,
            trade_id="exact-1",
            timestamp_us=CURRENT_EVENT_US + 1,
        )

        self.store.insert_batch(self.batch(execution))

        self.assertEqual(
            self._raw_rows(101),
            [
                (
                    4,
                    9,
                    CURRENT_EVENT_US + 1,
                    "exact-1",
                    100_125,
                    2_500,
                    11,
                )
            ],
        )

    def test_same_batch_retry_has_one_physical_row(self):
        execution = self.execution(
            market_id=102,
            source_epoch=1,
            sequence=1,
            trade_id="retry-1",
            timestamp_us=CURRENT_EVENT_US + 100_001,
        )
        batch = self.batch(execution)

        self.store.insert_batch(batch)
        self.store.insert_batch(batch)

        self.assertEqual(len(self._raw_rows(102)), 1)

    def test_lost_ack_retry_uses_the_same_content_hash_without_duplicate(self):
        execution = self.execution(
            market_id=103,
            source_epoch=1,
            sequence=1,
            trade_id="lost-ack-1",
            timestamp_us=CURRENT_EVENT_US + 200_001,
        )
        batch = self.batch(execution)
        ambiguous_access = _CommitThenLoseAcknowledgement(self.data_access)
        ambiguous_store = ClickHouseExecutionStore(
            ambiguous_access,
            database=self.test_database,
        )

        with self.assertRaisesRegex(TimeoutError, "lost ClickHouse"):
            ambiguous_store.insert_batch(batch)
        self.assertTrue(ambiguous_access.inserted)

        self.store.insert_batch(batch)
        self.assertEqual(len(self._raw_rows(103)), 1)

    def test_sparse_chart_bars_round_trip_in_latest_first_order(self):
        self.chart_store.append(
            [
                self.chart_bar(series_id=201, bar_index=1),
                self.chart_bar(series_id=201, bar_index=2),
                self.chart_bar(series_id=202, bar_index=1),
            ]
        )

        snapshot = self.chart_store.read_snapshot(series_id=201, limit=500)
        bounded = self.chart_store.read_snapshot(
            series_id=201,
            limit=500,
            max_levels=3,
        )
        older = self.chart_store.read_snapshot(
            series_id=201,
            limit=500,
            before_bar_index=2,
        )

        self.assertEqual([bar.bar_index for bar in bounded], [2])
        self.assertEqual([bar.bar_index for bar in older], [1])
        self.assertEqual([bar.bar_index for bar in snapshot], [2, 1])
        self.assertEqual(snapshot[0].level_ticks, (100, 101))
        self.assertEqual(snapshot[0].level_buy_units, (1, 1))
        self.assertEqual(snapshot[0].level_sell_units, (1, 0))

    def test_immediate_chart_bar_retry_is_physically_deduplicated(self):
        bar = self.chart_bar(series_id=203, bar_index=1)

        self.chart_store.append([bar])
        self.chart_store.append([bar])

        physical_count = self.data_access.query(
            f"""
            SELECT count()
            FROM {quote_identifier(self.test_database)}.`chart_bars`
            WHERE series_id = 203 AND bar_index = 1
            """,
            operation="count_integration_chart_retry_rows",
        ).result_rows[0][0]
        self.assertEqual(physical_count, 1)

    def test_source_epoch_reads_current_clickhouse_maximum(self):
        rows = (
            self.execution(
                market_id=104,
                source_epoch=3,
                sequence=1,
                trade_id="epoch-3",
                timestamp_us=CURRENT_EVENT_US + 300_001,
            ),
            self.execution(
                market_id=105,
                source_epoch=8,
                sequence=1,
                trade_id="epoch-8",
                timestamp_us=CURRENT_EVENT_US + 300_002,
            ),
        )
        self.store.insert_batch(self.batch(*rows))

        self.assertEqual(self.store.max_source_epoch([104, 105]), 8)

    def test_recent_identities_are_read_newest_first_per_market(self):
        rows = (
            self.execution(
                market_id=107,
                source_epoch=1,
                sequence=999,
                trade_id="older-epoch",
                timestamp_us=CURRENT_EVENT_US + 350_001,
            ),
            self.execution(
                market_id=107,
                source_epoch=2,
                sequence=1,
                trade_id="newer-epoch",
                timestamp_us=CURRENT_EVENT_US + 350_002,
                price="100.126",
            ),
        )
        self.store.insert_batch(self.batch(*rows))

        recent = self.store.recent_execution_identities(
            [107],
            per_market_limit=2,
        )

        self.assertEqual(
            [row.trade_id for row in recent[107]],
            ["newer-epoch", "older-epoch"],
        )
        self.assertEqual(
            recent[107][0].signature,
            (CURRENT_EVENT_US + 350_002, 100_126, 2_500, True, 11),
        )

    def test_projector_cursor_reads_only_strictly_newer_source_order(self):
        rows = (
            self.execution(
                market_id=108,
                source_epoch=1,
                sequence=1,
                trade_id="cursor-1",
                timestamp_us=CURRENT_EVENT_US + 360_001,
            ),
            self.execution(
                market_id=108,
                source_epoch=1,
                sequence=2,
                trade_id="cursor-2",
                timestamp_us=CURRENT_EVENT_US + 360_002,
            ),
            self.execution(
                market_id=108,
                source_epoch=2,
                sequence=1,
                trade_id="cursor-3",
                timestamp_us=CURRENT_EVENT_US + 360_003,
            ),
        )
        self.store.insert_batch(self.batch(*rows))
        projector_store = ClickHouseProjectorExecutionStore(
            self.data_access,
            database=self.test_database,
            spec_divisors={11: ProjectorSpecDivisors(1, 1)},
        )

        executions = list(
            projector_store.iter_executions(
                market_id=108,
                start_timestamp_us=CURRENT_EVENT_US,
                after_cursor=(
                    1,
                    1,
                    0,
                    CURRENT_EVENT_US + 360_001,
                    "cursor-1",
                ),
            )
        )

        self.assertEqual(
            [execution.trade_id for execution in executions],
            ["cursor-2", "cursor-3"],
        )

    def test_conflicting_identity_inside_one_batch_is_rejected(self):
        original = self.execution(
            market_id=106,
            source_epoch=1,
            sequence=1,
            trade_id="conflict",
            timestamp_us=CURRENT_EVENT_US + 400_001,
        )
        conflict = self.execution(
            market_id=106,
            source_epoch=1,
            sequence=1,
            trade_id="conflict",
            timestamp_us=CURRENT_EVENT_US + 400_001,
            price="100.126",
        )

        with self.assertRaises(ExecutionIdentityConflict):
            self.store.insert_batch(self.batch(original, conflict))
        self.assertEqual(self._raw_rows(106), [])
