from __future__ import annotations

from datetime import timedelta
from unittest.mock import MagicMock, Mock, patch

from django.core.management.base import CommandError
from django.test import SimpleTestCase, TestCase
from django.utils import timezone

from footprints import chart_service
from footprints.constants import (
    CHART_BUILD_EXECUTION_CHUNK,
    CHART_MAX_SERIES_PER_MARKET,
)
from footprints.management.commands import footprint_maintenance
from footprints.models import ChartSeries, ChartSeriesStatus, ChartType, FootprintMarket
from footprints.storage.charts import ProjectorExecution


class FakeExecutionStore:
    def __init__(self, executions):
        self.executions = list(executions)
        self.calls = []

    def iter_executions(self, **kwargs):
        self.calls.append(kwargs)
        after = kwargs.get("after_cursor")
        rows = [row for row in self.executions if after is None or row.cursor > after]
        limit = kwargs.get("limit")
        return iter(rows if limit is None else rows[:limit])


class FakeBarStore:
    def __init__(self):
        self.rows = {}

    def append(self, bars):
        for bar in bars:
            self.rows[(bar.series_id, bar.bar_index)] = bar
        return len(bars)


class CommitThenLoseAcknowledgementBarStore(FakeBarStore):
    def __init__(self):
        super().__init__()
        self.lose_next_acknowledgement = True

    def append(self, bars):
        result = super().append(bars)
        if self.lose_next_acknowledgement:
            self.lose_next_acknowledgement = False
            raise TimeoutError("lost ClickHouse acknowledgement")
        return result


class ChartServiceTests(TestCase):
    def setUp(self):
        chart_service._RUNTIMES.clear()
        self.market = FootprintMarket.objects.get(exchange="binance", symbol="BTCUSDT")
        self.spec = self.market.instrument_specs.order_by("effective_from").first()
        self.anchor = chart_service.lookback_anchor(timezone.now())

    def tearDown(self):
        chart_service._RUNTIMES.clear()

    def execution(
        self,
        sequence,
        price_tick,
        *,
        epoch=1,
        buy=True,
        quantity=1,
    ):
        return ProjectorExecution(
            source_epoch=epoch,
            source_sequence=sequence,
            batch_index=0,
            timestamp_us=int(self.anchor.timestamp() * 1_000_000) + sequence * 1_000,
            trade_id=str(sequence),
            price_tick=price_tick,
            quantity_units=quantity,
            is_taker_buy=buy,
            instrument_spec_id=self.spec.pk,
        )

    def series(self, chart_type=ChartType.TICK, **overrides):
        values = {
            "market": self.market,
            "chart_type": chart_type,
            "parameter_a": 2 if chart_type == ChartType.TICK else 5,
            "parameter_b": 0 if chart_type == ChartType.TICK else 5,
            "status": ChartSeriesStatus.BUILDING,
            "anchor_at": self.anchor,
        }
        values.update(overrides)
        return ChartSeries.objects.create(**values)

    def test_tick_build_writes_closed_bars_and_checkpoints_only_active_state(self):
        series = self.series()
        executions = FakeExecutionStore(
            [
                self.execution(1, 100, quantity=2),
                self.execution(2, 102, buy=False, quantity=3),
                self.execution(3, 101, quantity=5),
            ]
        )
        bars = FakeBarStore()

        chart_service.build_series(
            series.pk,
            execution_store=executions,
            bar_store=bars,
        )

        series.refresh_from_db()
        self.assertEqual(series.status, ChartSeriesStatus.READY)
        stored = next(iter(bars.rows.values()))
        self.assertEqual(
            (stored.open_tick, stored.high_tick, stored.close_tick), (100, 102, 102)
        )
        self.assertEqual(
            (stored.buy_units, stored.sell_units, stored.trade_count), (2, 3, 2)
        )
        active = chart_service.active_bar_for_series(series)
        self.assertEqual(
            (active.open_tick, active.trade_count, active.buy_units), (101, 1, 5)
        )
        self.assertEqual(
            executions.calls[0]["limit"],
            CHART_BUILD_EXECUTION_CHUNK,
        )
        self.assertIsNone(executions.calls[0]["after_cursor"])

    def test_full_build_resumes_chunks_until_every_execution_is_processed(self):
        series = self.series(parameter_a=1)
        executions = FakeExecutionStore(
            [self.execution(index, 100 + index) for index in range(1, 4)]
        )
        bars = FakeBarStore()

        with patch.object(chart_service, "CHART_BUILD_EXECUTION_CHUNK", 2):
            chart_service.build_series(
                series.pk,
                execution_store=executions,
                bar_store=bars,
            )
            series.refresh_from_db()
            self.assertEqual(series.status, ChartSeriesStatus.BUILDING)
            self.assertEqual(
                series.checkpoint_state["cursor"],
                list(self.execution(2, 102).cursor),
            )

            chart_service.build_series(
                series.pk,
                execution_store=executions,
                bar_store=bars,
            )

        series.refresh_from_db()
        self.assertEqual(series.status, ChartSeriesStatus.READY)
        self.assertEqual(
            set(bars.rows),
            {(series.pk, 0), (series.pk, 1), (series.pk, 2)},
        )
        self.assertEqual(
            executions.calls[1]["after_cursor"],
            self.execution(2, 102).cursor,
        )

    def test_shared_build_advances_a_later_lookback_to_the_next_chunk(self):
        early = self.series(parameter_a=1, lookback_days=2)
        late = self.series(
            parameter_a=1,
            lookback_days=1,
            anchor_at=self.anchor + timedelta(milliseconds=3),
        )
        executions = FakeExecutionStore(
            [self.execution(index, 100 + index) for index in range(1, 4)]
        )
        bars = FakeBarStore()

        with patch.object(chart_service, "CHART_BUILD_EXECUTION_CHUNK", 2):
            chart_service.build_series_batch(
                (early.pk, late.pk),
                execution_store=executions,
                bar_store=bars,
            )
            late.refresh_from_db()
            self.assertEqual(late.status, ChartSeriesStatus.BUILDING)
            self.assertEqual(
                late.checkpoint_state["cursor"],
                list(self.execution(2, 102).cursor),
            )

            chart_service.build_series_batch(
                (early.pk, late.pk),
                execution_store=executions,
                bar_store=bars,
            )

        late.refresh_from_db()
        self.assertEqual(late.status, ChartSeriesStatus.READY)
        self.assertIn((late.pk, 0), bars.rows)

    def test_transient_build_is_requeued_and_can_replay_same_indices(self):
        series = self.series(status=ChartSeriesStatus.QUEUED)
        executions = FakeExecutionStore(
            [self.execution(1, 100), self.execution(2, 101)]
        )
        bars = CommitThenLoseAcknowledgementBarStore()

        with self.assertRaises(chart_service.TransientChartBuildError):
            chart_service.build_next_queued_series(
                execution_store=executions,
                bar_store=bars,
            )

        series.refresh_from_db()
        self.assertEqual(series.status, ChartSeriesStatus.QUEUED)
        self.assertEqual(set(bars.rows), {(series.pk, 0)})

        self.assertTrue(
            chart_service.build_next_queued_series(
                execution_store=executions,
                bar_store=bars,
            )
        )
        series.refresh_from_db()
        self.assertEqual(series.status, ChartSeriesStatus.READY)
        self.assertEqual(set(bars.rows), {(series.pk, 0)})

    def test_nontransient_build_failure_is_terminal(self):
        series = self.series(status=ChartSeriesStatus.QUEUED)
        executions = FakeExecutionStore([self.execution(1, 100)])
        bars = FakeBarStore()
        bars.append = Mock(side_effect=ValueError("invalid row"))

        with self.assertRaisesRegex(ValueError, "invalid row"):
            chart_service.build_next_queued_series(
                execution_store=executions,
                bar_store=bars,
            )

        series.refresh_from_db()
        self.assertEqual(series.status, ChartSeriesStatus.FAILED)
        self.assertEqual(series.error_code, "ValueError")

    def test_queued_configurations_for_one_market_share_one_replay(self):
        tick = self.series(
            ChartType.TICK,
            parameter_a=2,
            status=ChartSeriesStatus.QUEUED,
        )
        trev = self.series(
            ChartType.TREV,
            parameter_a=1,
            parameter_b=1,
            status=ChartSeriesStatus.QUEUED,
        )
        executions = FakeExecutionStore(
            [
                self.execution(1, 100),
                self.execution(2, 101),
                self.execution(3, 102),
            ]
        )
        bars = FakeBarStore()

        self.assertTrue(
            chart_service.build_next_queued_series(
                execution_store=executions,
                bar_store=bars,
            )
        )

        tick.refresh_from_db()
        trev.refresh_from_db()
        self.assertEqual((tick.status, trev.status), ("ready", "ready"))
        self.assertEqual(len(executions.calls), 1)
        self.assertEqual(executions.calls[0]["market_id"], self.market.pk)
        self.assertEqual(
            {series_id for series_id, _bar_index in bars.rows},
            {tick.pk, trev.pk},
        )

    @patch("footprints.chart_service.publish_chart_update")
    def test_live_tail_is_read_once_and_fanned_to_tick_and_trev(self, publish_update):
        build_rows = [self.execution(1, 100), self.execution(2, 101)]
        bar_store = FakeBarStore()
        series_rows = [
            self.series(ChartType.TICK, parameter_a=3),
            self.series(ChartType.TREV, parameter_a=5, parameter_b=5),
        ]
        for series in series_rows:
            chart_service.build_series(
                series.pk,
                execution_store=FakeExecutionStore(build_rows),
                bar_store=bar_store,
            )
        tail_store = FakeExecutionStore([*build_rows, self.execution(3, 105)])

        processed = chart_service.advance_ready_series(
            execution_store=tail_store,
            bar_store=bar_store,
        )

        self.assertEqual(processed, 1)
        self.assertEqual(len(tail_store.calls), 1)
        self.assertEqual(tail_store.calls[0]["after_cursor"], build_rows[-1].cursor)
        self.assertEqual(publish_update.call_count, 2)
        for call in publish_update.call_args_list:
            for bar in call.kwargs["bars"]:
                self.assertIn("levels", bar)

    def test_targeted_advance_only_loads_the_notified_market(self):
        target = self.series(
            status=ChartSeriesStatus.READY,
            checkpoint_state={"stub": True},
        )
        other_market = FootprintMarket.objects.get(exchange="bybit", symbol="BTCUSDT")
        self.series(
            market=other_market,
            status=ChartSeriesStatus.READY,
            checkpoint_state={"stub": True},
        )
        runtime = MagicMock()

        with (
            patch.object(chart_service, "_runtime_for", return_value=runtime) as load,
            patch.object(chart_service, "_advance_market", return_value=0) as advance,
        ):
            chart_service.advance_ready_series(
                execution_store=FakeExecutionStore([]),
                bar_store=FakeBarStore(),
                market_ids=[self.market.pk],
            )

        load.assert_called_once()
        self.assertEqual(load.call_args.args[0].pk, target.pk)
        advance.assert_called_once()
        self.assertEqual(advance.call_args.args[0], self.market.pk)

    @patch("footprints.chart_service.publish_chart_update")
    def test_active_only_update_is_checkpointed_before_publication(self, publish_update):
        first = self.execution(1, 100)
        second = self.execution(2, 101)
        series = self.series(ChartType.TICK, parameter_a=5)
        bars = FakeBarStore()
        chart_service.build_series(
            series.pk,
            execution_store=FakeExecutionStore([first]),
            bar_store=bars,
        )

        chart_service.advance_ready_series(
            execution_store=FakeExecutionStore([first, second]),
            bar_store=bars,
        )

        series.refresh_from_db()
        self.assertEqual(series.checkpoint_state["cursor"], list(second.cursor))
        self.assertEqual(publish_update.call_args.kwargs["bars"], [])
        self.assertEqual(
            publish_update.call_args.kwargs["active_bar"]["tradeCount"], 2
        )

    @patch("footprints.chart_service.publish_chart_update")
    def test_live_update_is_bounded_to_latest_snapshot_bars(self, publish_update):
        first = self.execution(1, 100)
        series = self.series(ChartType.TICK, parameter_a=2)
        bars = FakeBarStore()
        chart_service.build_series(
            series.pk,
            execution_store=FakeExecutionStore([first]),
            bar_store=bars,
        )
        tail = [first, *(self.execution(index, 100) for index in range(2, 1_004))]

        chart_service.advance_ready_series(
            execution_store=FakeExecutionStore(tail),
            bar_store=bars,
        )

        calls = publish_update.call_args_list
        published = [bar for call in calls for bar in call.kwargs["bars"]]
        self.assertEqual(len(calls), 10)
        self.assertEqual(len(published), 500)
        self.assertEqual((published[0]["barIndex"], published[-1]["barIndex"]), (1, 500))
        self.assertTrue(
            all(call.kwargs["active_bar"]["barIndex"] == 501 for call in calls)
        )
        self.assertEqual(len(bars.rows), 501)

    @patch("footprints.chart_service.publish_chart_error")
    @patch("footprints.chart_service.publish_chart_update")
    def test_trev_expansion_failure_marks_and_notifies_series(
        self,
        publish_update,
        publish_error,
    ):
        first = self.execution(1, 100)
        series = self.series(ChartType.TREV, parameter_a=1, parameter_b=1)
        bars = FakeBarStore()
        chart_service.build_series(
            series.pk,
            execution_store=FakeExecutionStore([first]),
            bar_store=bars,
        )

        chart_service.advance_ready_series(
            execution_store=FakeExecutionStore(
                [first, self.execution(2, 100_101)]
            ),
            bar_store=bars,
        )

        series.refresh_from_db()
        self.assertEqual(series.status, ChartSeriesStatus.FAILED)
        self.assertEqual(series.error_code, "TREV_EXPANSION_LIMIT")
        publish_error.assert_called_once_with(
            str(series.public_key),
            "TREV_EXPANSION_LIMIT",
        )
        publish_update.assert_not_called()

    @patch("footprints.chart_service.publish_chart_error")
    @patch("footprints.chart_service.publish_chart_update")
    def test_dense_native_active_bar_is_terminal(self, publish_update, publish_error):
        first = self.execution(1, 100)
        series = self.series(ChartType.TICK, parameter_a=5)
        bars = FakeBarStore()
        chart_service.build_series(
            series.pk,
            execution_store=FakeExecutionStore([first]),
            bar_store=bars,
        )

        with patch.object(chart_service, "CHART_MAX_LEVELS_PER_BAR", 1):
            chart_service.advance_ready_series(
                execution_store=FakeExecutionStore(
                    [first, self.execution(2, 101)]
                ),
                bar_store=bars,
            )

        series.refresh_from_db()
        self.assertEqual(series.status, ChartSeriesStatus.FAILED)
        self.assertEqual(series.error_code, "CHART_BAR_TOO_DENSE")
        publish_error.assert_called_once_with(
            str(series.public_key), "CHART_BAR_TOO_DENSE"
        )
        publish_update.assert_not_called()

    def test_recover_interrupted_builds_and_claims_the_waiting_market(self):
        interrupted = self.series(checkpoint_state={"saved": True})
        queued = self.series(parameter_a=3, status=ChartSeriesStatus.QUEUED)

        self.assertEqual(chart_service.recover_interrupted_builds(), 1)
        claimed = chart_service.claim_queued_market_series()

        self.assertEqual(set(claimed), {interrupted.pk, queued.pk})
        self.assertEqual(
            ChartSeries.objects.filter(status=ChartSeriesStatus.BUILDING).count(), 2
        )
        interrupted.refresh_from_db()
        self.assertEqual(interrupted.checkpoint_state, {"saved": True})

    def test_expired_series_are_not_claimed_or_advanced(self):
        expired = self.series(status=ChartSeriesStatus.QUEUED)
        live = self.series(parameter_a=3, status=ChartSeriesStatus.QUEUED)
        ChartSeries.objects.filter(pk=expired.pk).update(
            last_requested_at=timezone.now() - timedelta(minutes=31)
        )

        self.assertEqual(chart_service.claim_queued_market_series(), (live.pk,))

        live.status = ChartSeriesStatus.BUILDING
        live.save(update_fields=["status"])
        chart_service.build_series(
            live.pk,
            execution_store=FakeExecutionStore([self.execution(1, 100)]),
            bar_store=FakeBarStore(),
        )
        ChartSeries.objects.filter(pk=live.pk).update(
            last_requested_at=timezone.now() - timedelta(minutes=31)
        )
        execution_store = FakeExecutionStore([self.execution(2, 101)])
        processed = chart_service.advance_ready_series(
            execution_store=execution_store,
            bar_store=FakeBarStore(),
        )

        self.assertEqual(processed, 0)
        self.assertEqual(execution_store.calls, [])

    def test_expiry_deletes_idle_nonbuilding_series_only(self):
        ready = self.series(status=ChartSeriesStatus.READY, checkpoint_state={"x": 1})
        building = self.series(parameter_a=3, status=ChartSeriesStatus.BUILDING)
        expired_at = timezone.now() - timedelta(minutes=31)
        ChartSeries.objects.filter(pk__in=[ready.pk, building.pk]).update(
            last_requested_at=expired_at
        )

        self.assertEqual(
            chart_service.expire_stale_series(now=timezone.now()),
            1,
        )
        self.assertFalse(ChartSeries.objects.filter(pk=ready.pk).exists())
        self.assertTrue(ChartSeries.objects.filter(pk=building.pk).exists())

    def test_expired_building_series_still_occupies_market_capacity(self):
        building = self.series(status=ChartSeriesStatus.BUILDING)
        ChartSeries.objects.filter(pk=building.pk).update(
            last_requested_at=timezone.now() - timedelta(minutes=31)
        )
        for parameter in range(3, CHART_MAX_SERIES_PER_MARKET + 2):
            self.series(
                parameter_a=parameter,
                status=ChartSeriesStatus.READY,
                checkpoint_state={"state": parameter},
            )

        denied = chart_service.request_chart_series(
            market=self.market,
            chart_type=ChartType.TREV,
            parameter_a=20,
            parameter_b=30,
            lookback_days=2,
            now=timezone.now(),
        )

        self.assertIsNone(denied)
        renewed = chart_service.request_chart_series(
            market=self.market,
            chart_type=ChartType.TICK,
            parameter_a=building.parameter_a,
            parameter_b=0,
            lookback_days=2,
            now=timezone.now(),
        )
        self.assertEqual(renewed.pk, building.pk)
        self.assertEqual(ChartSeries.objects.count(), CHART_MAX_SERIES_PER_MARKET)

    def test_live_chunks_bound_bars_and_total_levels_with_active(self):
        level = [100, 1, 0]
        active = {"levels": [level, level]}
        bars = [
            {"levels": [level, level]},
            {"levels": [level, level]},
            {"levels": [level]},
        ]

        with patch.object(chart_service, "CHART_LIVE_MAX_LEVELS", 5):
            chunks = list(chart_service._live_update_chunks(bars, active))

        self.assertEqual([len(chunk) for chunk in chunks], [1, 2])
        self.assertTrue(
            all(
                len(active["levels"])
                + sum(len(bar["levels"]) for bar in chunk)
                <= 5
                for chunk in chunks
            )
        )


class MaintenanceSingletonTests(SimpleTestCase):
    @staticmethod
    def connection_with_row(row):
        connection = MagicMock()
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.fetchone.return_value = row
        return connection, cursor

    @patch("footprints.management.commands.footprint_maintenance.psycopg.connect")
    def test_acquires_and_verifies_dedicated_postgres_lock(self, connect):
        connection, cursor = self.connection_with_row((True,))
        connect.return_value = connection

        acquired = footprint_maintenance._acquire_singleton()
        cursor.fetchone.return_value = (1,)
        footprint_maintenance._verify_singleton(acquired)

        self.assertIs(acquired, connection)
        self.assertEqual(cursor.execute.call_count, 2)
        connection.close.assert_not_called()

    @patch("footprints.management.commands.footprint_maintenance.psycopg.connect")
    def test_rejects_a_second_maintenance_process(self, connect):
        connection, _cursor = self.connection_with_row((False,))
        connect.return_value = connection

        with self.assertRaisesRegex(CommandError, "another footprint maintenance"):
            footprint_maintenance._acquire_singleton()

        connection.close.assert_called_once_with()
