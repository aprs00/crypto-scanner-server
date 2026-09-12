import importlib.util
from datetime import timedelta
from types import SimpleNamespace
from unittest import skipUnless
from unittest.mock import patch

from django.test import TestCase, override_settings
from django.utils import timezone

from footprints import chart_service
from footprints.models import ChartSeries, ChartSeriesStatus, ChartType, FootprintMarket
from footprints.storage.charts import ClickHouseChartStore, ClickHouseProjectorExecutionStore, ProjectorSpecDivisors
from footprints.storage.reconciled import ReconciledExecutionStore, DAY_US
from footprints.tests.test_native_trev import rowbinary


class MemoryAccess:
    def __init__(self, rows):
        self.rows = rows
        self.inserted = {}
        self.streams = 0
        self.closed = 0
        self.fail_insert = False

    def query(self, sql, *, parameters, **kwargs):
        rows = self._rows(parameters)
        return SimpleNamespace(result_rows=[rows[-1][:5]] if rows else [])

    def _rows(self, parameters):
        rows = self.rows
        if "start_timestamp_us" in parameters:
            rows = [r for r in rows if r[3] >= parameters["start_timestamp_us"]]
        if "start" in parameters:
            rows = [r for r in rows if parameters["start"] <= r[3] < parameters["end"]]
        for prefix, compare in (("after", lambda x, y: x > y), ("through", lambda x, y: x <= y)):
            if f"{prefix}_source_epoch" in parameters:
                cursor = tuple(parameters[f"{prefix}_{field}"] for field in (
                    "source_epoch", "source_sequence", "batch_index", "timestamp_us", "trade_id"))
                rows = [r for r in rows if compare(r[:5], cursor)]
        if "ordinal" in parameters:
            rows = [r for r in rows if r[1] > parameters["ordinal"]]
        if "sequence" in parameters:
            cursor = tuple(parameters[field] for field in ("epoch", "sequence", "batch", "time", "id"))
            rows = [r for r in rows if r[:5] > cursor]
        return rows

    def stream_rows(self, sql, *, parameters, **kwargs):
        self.streams += 1
        yield from self._rows(parameters)

    def stream_rowbinary(self, sql, *, parameters, **kwargs):
        self.streams += 1
        data = rowbinary(self._rows(parameters))
        try:
            for offset in range(0, len(data), 137):
                yield data[offset:offset + 137]
        finally:
            self.closed += 1

    def insert(self, table, rows, **kwargs):
        for row in rows:
            self.inserted[row[:2]] = tuple(tuple(value) if isinstance(value, (tuple, list)) else value for value in row)
        if self.fail_insert:
            self.fail_insert = False
            raise TimeoutError("lost insert acknowledgement")


@skipUnless(importlib.util.find_spec("_trev_native"), "native wheel is not installed")
@override_settings(TREV_COLD_BACKEND="rust")
class NativeChartBuildTests(TestCase):
    def setUp(self):
        self.market = FootprintMarket.objects.get(exchange="binance", symbol="BTCUSDT")
        self.spec = self.market.instrument_specs.order_by("effective_from").first()
        self.anchor = chart_service.lookback_anchor(timezone.now())
        self.start = int(self.anchor.timestamp() * 1_000_000)
        chart_service._RUNTIMES.clear()

    def series(self, a=5, b=7, chart_type=ChartType.TREV, **kwargs):
        return ChartSeries.objects.create(
            market=self.market, chart_type=chart_type, parameter_a=a, parameter_b=b,
            status=ChartSeriesStatus.BUILDING,
            lookback_days=None if chart_type == ChartType.TREV else 2,
            anchor_at=kwargs.pop("anchor_at", self.anchor), **kwargs,
        )

    def rows(self):
        return [(1 + i // 11, i, 0, self.start + 10000 - i, str(i), price,
                 1 + i, bool(i % 2), self.spec.pk)
                for i, price in enumerate((100, 100, 105, 118, 113, 90, 101, 102, 103, 105,
                                            100, 99, 98, 97, 105, 111, 110, 108, 104, 90))]

    def stores(self, rows):
        access = MemoryAccess(rows)
        execution = ClickHouseProjectorExecutionStore(
            access, database="test", spec_divisors={self.spec.pk: ProjectorSpecDivisors(1, 1)},
        )
        return access, execution, ClickHouseChartStore(access, database="test")

    def test_grouped_tick_and_trev_resume_matches_python_and_live_handoff(self):
        series = [self.series(), self.series(3, 4), self.series(2, 0, ChartType.TICK)]
        identities = [s.pk for s in series]
        rows = self.rows()
        rows.insert(4, rows[3])
        python_access, executions, bars = self.stores(rows)
        with override_settings(TREV_COLD_BACKEND="python"):
            chart_service.build_series_batch(identities, execution_store=executions, bar_store=bars)
        expected = {}
        for item in series:
            item.refresh_from_db()
            expected[item.pk] = (item.status, item.checkpoint_state)
        ChartSeries.objects.filter(pk__in=identities).update(status=ChartSeriesStatus.BUILDING, checkpoint_state=None)
        access, executions, bars = self.stores(rows)
        with patch.object(chart_service, "CHART_BUILD_EXECUTION_CHUNK", 3):
            self.assertFalse(chart_service.build_series_batch(
                identities, execution_store=executions, bar_store=bars, should_stop=lambda: True,
            ))
            for item in series:
                item.refresh_from_db()
                self.assertEqual(item.checkpoint_state["cursor"], list(rows[2][:5]))
            self.assertEqual(access.closed, 1)
            self.assertTrue(chart_service.build_series_batch(identities, execution_store=executions, bar_store=bars))
        self.assertEqual(access.streams, 2)  # one stream for all configurations per attempt
        self.assertEqual(access.inserted, python_access.inserted)
        for item in series:
            item.refresh_from_db()
            self.assertEqual((item.status, item.checkpoint_state), expected[item.pk])
            engine, cursor = chart_service._decode_checkpoint(item)
            runtime = chart_service._runtime_for(item)
            self.assertEqual(runtime.engine.snapshot(), engine.snapshot())
            self.assertEqual(runtime.cursor, cursor)

    def test_lost_insert_ack_replays_from_the_last_durable_checkpoint(self):
        item = self.series()
        access, executions, bars = self.stores(self.rows())
        access.fail_insert = True
        with self.assertRaises(TimeoutError):
            chart_service.build_series(item.pk, execution_store=executions, bar_store=bars)
        item.refresh_from_db()
        self.assertIsNone(item.checkpoint_state)
        self.assertEqual(access.closed, 1)
        chart_service.build_series(item.pk, execution_store=executions, bar_store=bars)
        item.refresh_from_db()
        expected_access, reference, reference_bars = self.stores(self.rows())
        item.status = ChartSeriesStatus.BUILDING
        checkpoint = item.checkpoint_state
        item.checkpoint_state = None
        item.save()
        with override_settings(TREV_COLD_BACKEND="python"):
            chart_service.build_series(item.pk, execution_store=reference, bar_store=reference_bars)
        item.refresh_from_db()
        self.assertEqual(item.checkpoint_state, checkpoint)
        self.assertEqual(access.inserted, expected_access.inserted)

    def test_expansion_failure_isolated_to_one_configuration(self):
        failing, valid = self.series(1, 1), self.series(4000, 4000)
        rows = self.rows()[:2]
        rows[1] = (*rows[1][:5], 800, *rows[1][6:])
        access, executions, bars = self.stores(rows)
        chart_service.build_series_batch([failing.pk, valid.pk], execution_store=executions, bar_store=bars)
        failing.refresh_from_db()
        valid.refresh_from_db()
        self.assertEqual((failing.status, failing.error_code), (ChartSeriesStatus.FAILED, "TREV_EXPANSION_LIMIT"))
        self.assertEqual(valid.status, ChartSeriesStatus.CATCHING_UP)

    def test_expired_checkpoint_requires_explicit_rebuild_before_reading_deleted_trades(self):
        item = self.series(anchor_at=timezone.now() - timedelta(days=32))
        access, executions, bars = self.stores([])
        with self.assertRaisesRegex(ValueError, "CHART_CHECKPOINT_EXPIRED"):
            chart_service.build_series(item.pk, execution_store=executions, bar_store=bars)
        self.assertEqual(access.streams, 0)

    def test_rolling_retention_preserves_old_origin_when_recent_checkpoint_exists(self):
        from footprints.trev import TrevEngine, TrevConfig

        anchor = timezone.now() - timedelta(days=70)
        engine = TrevEngine(TrevConfig(5, 7))
        engine.push_raw(timestamp_us=self.start, rank=100_000, price_tick=100,
                        quantity_units=1, is_taker_buy=True, spec_id=self.spec.pk)
        cursor = (1, 0, 0, self.start, "old")
        item = self.series(anchor_at=anchor, checkpoint_state={"engine": engine.snapshot(), "cursor": list(cursor)})
        access, executions, bars = self.stores(self.rows()[1:])
        chart_service.build_series(item.pk, execution_store=executions, bar_store=bars)
        item.refresh_from_db()
        self.assertEqual(item.anchor_at, anchor)
        self.assertEqual(item.status, ChartSeriesStatus.CATCHING_UP)
        self.assertEqual(item.checkpoint_state["engine"]["lastOrder"][1], 100_019)

    def test_empty_market_completes_without_opening_a_stream(self):
        item = self.series()
        access, executions, bars = self.stores([])
        chart_service.build_series(item.pk, execution_store=executions, bar_store=bars)
        item.refresh_from_db()
        self.assertEqual(item.status, ChartSeriesStatus.CATCHING_UP)
        self.assertEqual(access.streams, 0)

    def test_reconciled_archive_live_seams_and_resume_match_python(self):
        first_day = self.start // DAY_US
        rows = []
        for day_offset, epoch in ((0, 9), (1, 1), (2, 1), (3, 10)):
            for index, price in enumerate((100, 107, 90, 92)):
                day = first_day + day_offset
                rows.append((epoch, index, 0, day * DAY_US + 10 + index, str(day * 10 + index),
                             price, 1, bool(index % 2), self.spec.pk))
        imports = {first_day + 1: "archive-a", first_day + 2: "archive-b"}

        def store(access):
            instance = ReconciledExecutionStore(
                access, database="test", spec_divisors={self.spec.pk: ProjectorSpecDivisors(1, 1)},
            )
            instance._inputs = lambda market: imports
            instance._fixed[self.market.pk] = imports
            # First seam is continuous; second seam intentionally breaks.
            instance._live_epoch = lambda market, timestamp, identity: 9
            return instance

        after = None
        through = (-2, first_day + 3, 1, *rows[-1][:5])
        expected = list(store(MemoryAccess(rows)).iter_executions(
            market_id=self.market.pk, start_timestamp_us=first_day * DAY_US,
            after_cursor=after, through_cursor=through,
        ))
        native_store = store(MemoryAccess(rows))
        batches = list(native_store.iter_native_batches(
            market_id=self.market.pk, start_timestamp_us=first_day * DAY_US,
            after_cursor=after, through_cursor=through, batch_size=3,
        ))
        actual = []
        for batch in batches:
            records = batch.data.executions(0)
            if batch.seam_break:
                records[0] = (*records[0][:-1], True)
            actual.extend(records)
        self.assertEqual(actual, [tuple(e) for e in expected])
        resume = expected[6].cursor
        native_store = store(MemoryAccess(rows))
        resumed = [row for b in native_store.iter_native_batches(
            market_id=self.market.pk, start_timestamp_us=first_day * DAY_US,
            after_cursor=resume, through_cursor=through, batch_size=3,
        ) for row in b.data.executions(0)]
        # The separate seam flag is intentionally consumed by the service.
        self.assertEqual([r[:10] for r in resumed], [tuple(e)[:10] for e in expected[7:]])
