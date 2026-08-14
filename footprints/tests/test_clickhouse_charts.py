from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

from django.test import SimpleTestCase

from footprints.constants import CHART_SNAPSHOT_MAX_LEVELS
from footprints.storage.charts import (
    ChartDataError,
    ChartLevel,
    ChartSnapshotBar,
    ClickHouseChartStore,
    ClickHouseProjectorExecutionStore,
    DetailedChartBar,
    ProjectorDataError,
    ProjectorSpecDivisors,
)
from footprints.storage.clickhouse_schema import (
    CHART_BAR_COLUMNS,
    RAW_EXECUTION_COLUMNS,
    ensure_clickhouse_schema,
)


OPENED_AT = datetime(2026, 8, 5, 9, 30, tzinfo=timezone.utc)
ENDED_AT = datetime(2026, 8, 5, 9, 31, tzinfo=timezone.utc)


class _FakeAccess:
    def __init__(self, *, stream_rows=(), query_results=()):
        self.stream_result_rows = list(stream_rows)
        self.query_results = list(query_results)
        self.stream_queries = []
        self.queries = []
        self.inserts = []
        self.commands = []

    def stream_rows(self, sql, *, parameters=None, settings=None, operation=""):
        self.stream_queries.append((sql, parameters, settings, operation))
        yield from self.stream_result_rows

    def query(self, sql, *, parameters=None, settings=None, operation=""):
        self.queries.append((sql, parameters, settings, operation))
        rows = self.query_results.pop(0) if self.query_results else ()
        return SimpleNamespace(result_rows=tuple(rows))

    def insert(self, table, rows, *, column_names, settings=None, operation=""):
        self.inserts.append(
            (table, tuple(rows), tuple(column_names), settings, operation)
        )

    def command(self, sql, *, parameters=None, settings=None, operation=""):
        self.commands.append((sql, parameters, settings, operation))


def _raw_projector_row(
    *,
    source_epoch=3,
    source_sequence=41,
    batch_index=2,
    timestamp_us=1_700_000_000_000_123,
    trade_id="trade-41",
    price_atoms=25_050,
    quantity_atoms=125,
    is_taker_buy=True,
    instrument_spec_id=11,
):
    return (
        source_epoch,
        source_sequence,
        batch_index,
        timestamp_us,
        trade_id,
        price_atoms,
        quantity_atoms,
        is_taker_buy,
        instrument_spec_id,
    )


def _bar(*, bar_index=4, levels=None):
    return DetailedChartBar(
        series_id=7,
        bar_index=bar_index,
        opened_at=OPENED_AT,
        ended_at=ENDED_AT,
        instrument_spec_id=11,
        open_tick=1_000,
        high_tick=1_003,
        low_tick=999,
        close_tick=1_002,
        buy_units=30,
        sell_units=20,
        trade_count=5,
        break_before=False,
        levels=(
            levels
            if levels is not None
            else (ChartLevel(999, 0, 4), ChartLevel(1_002, 9, 0))
        ),
    )


class ProjectorExecutionStoreTests(SimpleTestCase):
    def _store(self, access):
        return ClickHouseProjectorExecutionStore(
            access,
            database="footprints",
            spec_divisors={11: ProjectorSpecDivisors(25, 5)},
        )

    def test_streams_exact_native_units_with_global_cursor(self):
        row = _raw_projector_row()
        access = _FakeAccess(stream_rows=(row,))
        after = (3, 40, 1, 1_700_000_000_000_000, "trade-40")

        executions = list(
            self._store(access).iter_executions(
                market_id=7,
                start_timestamp_us=1_699_000_000_000_000,
                after_cursor=after,
                limit=1_000,
            )
        )

        self.assertEqual(executions[0].price_tick, 1_002)
        self.assertEqual(executions[0].quantity_units, 25)
        self.assertEqual(executions[0].cursor, (3, 41, 2, row[3], "trade-41"))
        sql, parameters, _settings, operation = access.stream_queries[0]
        compact_sql = " ".join(sql.split())
        self.assertIn(
            "ORDER BY market_id, source_epoch, source_sequence, batch_index, timestamp_us, trade_id",
            compact_sql,
        )
        self.assertIn(
            "source_epoch > {after_source_epoch:UInt32}",
            compact_sql,
        )
        self.assertIn(
            "source_sequence = {after_source_sequence:UInt64} AND batch_index > {after_batch_index:UInt32}",
            compact_sql,
        )
        self.assertIn(
            "timestamp_us = {after_timestamp_us:Int64} AND trade_id > {after_trade_id:String}",
            compact_sql,
        )
        self.assertEqual(parameters["after_source_epoch"], 3)
        self.assertEqual(operation, "stream_projector_executions")

    def test_rejects_nondivisible_atoms_and_unknown_specification(self):
        nondivisible = _FakeAccess(
            stream_rows=(_raw_projector_row(price_atoms=25_051),)
        )
        with self.assertRaisesRegex(ProjectorDataError, "not exactly divisible"):
            list(
                self._store(nondivisible).iter_executions(
                    market_id=7, start_timestamp_us=1
                )
            )

        unknown = _FakeAccess(
            stream_rows=(_raw_projector_row(instrument_spec_id=99),)
        )
        with self.assertRaisesRegex(ProjectorDataError, "specification 99"):
            list(
                self._store(unknown).iter_executions(
                    market_id=7, start_timestamp_us=1
                )
            )

    def test_loads_and_caches_a_divisor_created_after_store_startup(self):
        access = _FakeAccess(
            stream_rows=(
                _raw_projector_row(instrument_spec_id=99),
                _raw_projector_row(
                    source_sequence=42,
                    trade_id="trade-42",
                    instrument_spec_id=99,
                ),
            )
        )
        loaded_ids = []

        def load_spec_divisor(spec_id):
            loaded_ids.append(spec_id)
            return ProjectorSpecDivisors(25, 5)

        store = ClickHouseProjectorExecutionStore(
            access,
            database="footprints",
            spec_divisors={11: ProjectorSpecDivisors(25, 5)},
            load_spec_divisor=load_spec_divisor,
        )

        executions = list(
            store.iter_executions(market_id=7, start_timestamp_us=1)
        )

        self.assertEqual(loaded_ids, [99])
        self.assertEqual([row.price_tick for row in executions], [1_002, 1_002])
        self.assertEqual([row.quantity_units for row in executions], [25, 25])

    def test_identical_retry_rows_collapse_and_conflicting_cursor_fails(self):
        row = _raw_projector_row()
        identical = list(
            self._store(_FakeAccess(stream_rows=(row, row))).iter_executions(
                market_id=7, start_timestamp_us=1
            )
        )
        self.assertEqual(len(identical), 1)

        conflict = _FakeAccess(stream_rows=(row, _raw_projector_row(quantity_atoms=130)))
        with self.assertRaisesRegex(ProjectorDataError, "conflicting executions"):
            list(
                self._store(conflict).iter_executions(
                    market_id=7, start_timestamp_us=1
                )
            )


class ClickHouseChartStoreTests(SimpleTestCase):
    def test_schema_is_idempotent_sparse_and_v0_only(self):
        access = _FakeAccess()

        ensure_clickhouse_schema(access, "footprints")

        self.assertEqual(len(access.commands), 3)
        raw_ddl = " ".join(access.commands[1][0].split())
        chart_ddl = " ".join(access.commands[2][0].split())
        self.assertIn("CREATE TABLE IF NOT EXISTS `footprints`.`raw_executions`", raw_ddl)
        self.assertIn("ORDER BY (series_id, bar_index)", chart_ddl)
        self.assertIn("ENGINE = ReplacingMergeTree()", chart_ddl)
        self.assertEqual(len(RAW_EXECUTION_COLUMNS), 10)
        self.assertEqual(len(CHART_BAR_COLUMNS), 16)

    def test_append_writes_parallel_sparse_level_arrays(self):
        access = _FakeAccess()
        store = ClickHouseChartStore(access, database="footprints")

        self.assertEqual(store.append([_bar()]), 1)

        table, rows, columns, settings, operation = access.inserts[0]
        self.assertEqual(table, "footprints.chart_bars")
        self.assertEqual(columns, CHART_BAR_COLUMNS)
        mapped = dict(zip(columns, rows[0], strict=True))
        self.assertEqual(mapped["level_ticks"], (999, 1_002))
        self.assertEqual(mapped["level_buy_units"], (0, 9))
        self.assertEqual(mapped["level_sell_units"], (4, 0))
        self.assertEqual(settings["async_insert"], 0)
        self.assertNotIn("wait_for_async_insert", settings)
        self.assertEqual(settings["insert_deduplicate"], 1)
        self.assertEqual(operation, "insert_chart_bars")

    def test_snapshot_read_keeps_validated_native_level_arrays(self):
        bar = _bar()
        row = (
            bar.series_id,
            bar.bar_index,
            bar.opened_at,
            bar.ended_at,
            bar.instrument_spec_id,
            bar.open_tick,
            bar.high_tick,
            bar.low_tick,
            bar.close_tick,
            bar.buy_units,
            bar.sell_units,
            bar.trade_count,
            bar.break_before,
            [999, 1_002],
            [0, 9],
            [4, 0],
        )
        access = _FakeAccess(query_results=((row,),))

        result = ClickHouseChartStore(access, database="footprints").read_snapshot(
            series_id=7,
            limit=500,
        )

        self.assertEqual(len(result), 1)
        snapshot = result[0]
        self.assertIsInstance(snapshot, ChartSnapshotBar)
        self.assertEqual(len(snapshot.level_ticks), 2)
        self.assertEqual(snapshot.level_ticks, (999, 1_002))
        self.assertEqual(snapshot.level_buy_units, (0, 9))
        self.assertEqual(snapshot.level_sell_units, (4, 0))
        sql, parameters, _settings, operation = access.queries[0]
        self.assertIn("FINAL", sql)
        self.assertIn("ORDER BY bar_index DESC", sql)
        self.assertIn("cumulative_levels", sql)
        self.assertEqual(
            parameters,
            {
                "series_id": 7,
                "limit": 500,
                "max_levels": CHART_SNAPSHOT_MAX_LEVELS,
            },
        )
        self.assertEqual(operation, "read_chart_snapshot_bars")

    def test_snapshot_read_rejects_invalid_native_level_volume(self):
        malformed = _FakeAccess(
            query_results=(
                (
                    (
                        7,
                        4,
                        OPENED_AT,
                        ENDED_AT,
                        11,
                        1_000,
                        1_003,
                        999,
                        1_002,
                        30,
                        20,
                        5,
                        False,
                        [999],
                        [0],
                        [0],
                    ),
                ),
            )
        )

        with self.assertRaisesRegex(ChartDataError, "level volume"):
            ClickHouseChartStore(malformed, database="footprints").read_snapshot(
                series_id=7
            )

    def test_snapshot_read_pages_before_an_existing_bar(self):
        access = _FakeAccess()

        ClickHouseChartStore(access, database="footprints").read_snapshot(
            series_id=7,
            limit=250,
            before_bar_index=900,
        )

        sql, parameters, _settings, _operation = access.queries[0]
        self.assertIn("bar_index < {before_bar_index:UInt64}", sql)
        self.assertEqual(parameters["before_bar_index"], 900)
        self.assertEqual(parameters["limit"], 250)

    def test_duplicate_insert_key_and_malformed_levels_are_rejected(self):
        store = ClickHouseChartStore(_FakeAccess(), database="footprints")
        with self.assertRaisesRegex(ChartDataError, "duplicate bar index"):
            store.append([_bar(), _bar()])

        malformed = _FakeAccess(
            query_results=(
                (
                    (
                        7,
                        4,
                        OPENED_AT,
                        ENDED_AT,
                        11,
                        1_000,
                        1_003,
                        999,
                        1_002,
                        30,
                        20,
                        5,
                        False,
                        [999],
                        [1, 2],
                        [3],
                    ),
                ),
            )
        )
        with self.assertRaisesRegex(ChartDataError, "different lengths"):
            ClickHouseChartStore(malformed, database="footprints").read_snapshot(
                series_id=7
            )
