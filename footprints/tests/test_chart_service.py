from __future__ import annotations

from datetime import timedelta
from unittest.mock import MagicMock, Mock, patch

from django.core.management.base import CommandError
from django.test import SimpleTestCase, TestCase
from django.utils import timezone

from footprints import chart_service
from footprints.constants import (
    CHART_MAX_SERIES_PER_MARKET,
    CHART_SERIES_CACHE_SECONDS,
    CHART_WARM_TREV_CONTINUATION_TICKS,
    CHART_WARM_TREV_REVERSAL_TICKS,
)
from footprints.management.commands import footprint_maintenance
from footprints.models import (
    ChartSeries,
    ChartSeriesStatus,
    ChartType,
    FootprintMarket,
    InstrumentSpec,
)
from footprints.storage.charts import ProjectorExecution


class FakeExecutionStore:
    def __init__(self, executions):
        self.executions = list(executions)
        self.calls = []
        self.latest_calls = []

    def latest_cursor(self, **kwargs):
        self.latest_calls.append(kwargs)
        rows = [
            row
            for row in self.executions
            if row.timestamp_us >= kwargs["start_timestamp_us"]
        ]
        return max((row.cursor for row in rows), default=None)

    def iter_executions(self, **kwargs):
        self.calls.append(kwargs)
        after = kwargs.get("after_cursor")
        through = kwargs.get("through_cursor")
        rows = [
            row
            for row in self.executions
            if (after is None or row.cursor > after)
            and (through is None or row.cursor <= through)
        ]
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
            "lookback_days": None if chart_type == ChartType.TREV else 2,
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
        self.assertNotIn("limit", executions.calls[0])
        self.assertIsNone(executions.calls[0]["after_cursor"])
        self.assertEqual(
            executions.calls[0]["through_cursor"],
            self.execution(3, 101, quantity=5).cursor,
        )

    def test_full_build_streams_once_and_resumes_from_durable_checkpoints(self):
        series = self.series(parameter_a=1)
        executions = FakeExecutionStore(
            [self.execution(index, 100 + index) for index in range(1, 4)]
        )
        bars = FakeBarStore()

        with patch.object(chart_service, "CHART_BUILD_EXECUTION_CHUNK", 2):
            completed = chart_service.build_series_batch(
                (series.pk,),
                execution_store=executions,
                bar_store=bars,
                should_stop=lambda: True,
            )
            self.assertFalse(completed)
            series.refresh_from_db()
            self.assertEqual(series.status, ChartSeriesStatus.BUILDING)
            self.assertEqual(
                series.checkpoint_state["cursor"],
                list(self.execution(2, 102).cursor),
            )

            completed = chart_service.build_series_batch(
                (series.pk,),
                execution_store=executions,
                bar_store=bars,
            )
            self.assertTrue(completed)

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

    def test_different_lookbacks_are_claimed_as_independent_batches(self):
        early = self.series(
            parameter_a=1,
            lookback_days=2,
            status=ChartSeriesStatus.QUEUED,
        )
        late = self.series(
            parameter_a=1,
            lookback_days=1,
            anchor_at=self.anchor + timedelta(milliseconds=3),
            status=ChartSeriesStatus.QUEUED,
        )

        first_claim = chart_service.claim_queued_market_series()
        second_claim = chart_service.claim_queued_market_series()

        self.assertEqual(first_claim, (early.pk,))
        self.assertEqual(second_claim, (late.pk,))

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
        self.assertEqual((tick.status, trev.status), ("ready", "catching_up"))
        self.assertEqual(len(executions.calls), 1)
        self.assertEqual(len(executions.latest_calls), 1)
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
    def test_active_only_update_is_checkpointed_before_publication(
        self, publish_update
    ):
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
        self.assertEqual(publish_update.call_args.kwargs["active_bar"]["tradeCount"], 2)

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
        self.assertEqual(
            (published[0]["barIndex"], published[-1]["barIndex"]), (1, 500)
        )
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
            execution_store=FakeExecutionStore([first, self.execution(2, 100_101)]),
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
                execution_store=FakeExecutionStore([first, self.execution(2, 101)]),
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

    def test_completed_worker_runtime_is_discarded_in_the_coordinator(self):
        series = self.series()
        chart_service._RUNTIMES[series.pk] = MagicMock()

        chart_service.discard_series_runtimes((series.pk,))

        self.assertNotIn(series.pk, chart_service._RUNTIMES)

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

    def test_expiry_keeps_ready_cache_but_deletes_expired_queue_entries(self):
        ready = self.series(status=ChartSeriesStatus.READY, checkpoint_state={"x": 1})
        building = self.series(parameter_a=3, status=ChartSeriesStatus.BUILDING)
        queued = self.series(parameter_a=4, status=ChartSeriesStatus.QUEUED)
        failed = self.series(parameter_a=5, status=ChartSeriesStatus.FAILED)
        expired_at = timezone.now() - timedelta(minutes=31)
        ChartSeries.objects.filter(
            pk__in=[ready.pk, building.pk, queued.pk, failed.pk]
        ).update(last_requested_at=expired_at)

        self.assertEqual(
            chart_service.expire_stale_series(now=timezone.now()),
            2,
        )
        self.assertTrue(ChartSeries.objects.filter(pk=ready.pk).exists())
        self.assertTrue(ChartSeries.objects.filter(pk=building.pk).exists())
        self.assertFalse(ChartSeries.objects.filter(pk=queued.pk).exists())
        self.assertFalse(ChartSeries.objects.filter(pk=failed.pk).exists())

        ChartSeries.objects.filter(pk=ready.pk).update(
            last_requested_at=timezone.now()
            - timedelta(seconds=CHART_SERIES_CACHE_SECONDS + 1)
        )
        self.assertEqual(
            chart_service.expire_stale_series(now=timezone.now()),
            1,
        )
        self.assertFalse(ChartSeries.objects.filter(pk=ready.pk).exists())

    def test_dormant_ready_series_reuses_its_checkpoint(self):
        checkpoint = {"engine": {"saved": True}, "cursor": [1, 2, 0, 3, "4"]}
        ready = self.series(
            status=ChartSeriesStatus.READY,
            checkpoint_state=checkpoint,
        )
        ChartSeries.objects.filter(pk=ready.pk).update(
            last_requested_at=timezone.now() - timedelta(minutes=31)
        )

        reused = chart_service.request_chart_series(
            market=self.market,
            chart_type=ready.chart_type,
            parameter_a=ready.parameter_a,
            parameter_b=ready.parameter_b,
            lookback_days=ready.lookback_days,
            now=timezone.now(),
        )

        self.assertEqual(reused.pk, ready.pk)
        self.assertEqual(reused.status, ChartSeriesStatus.QUEUED)
        self.assertEqual(reused.checkpoint_state, checkpoint)

    def test_new_series_evicts_the_oldest_dormant_ready_cache_entry(self):
        now = timezone.now()
        oldest = None
        for parameter in range(1, CHART_MAX_SERIES_PER_MARKET + 1):
            cached = self.series(
                parameter_a=parameter,
                status=ChartSeriesStatus.READY,
                checkpoint_state={"state": parameter},
            )
            ChartSeries.objects.filter(pk=cached.pk).update(
                last_requested_at=now - timedelta(hours=parameter)
            )
            oldest = cached

        created = chart_service.request_chart_series(
            market=self.market,
            chart_type=ChartType.TREV,
            parameter_a=20,
            parameter_b=30,
            lookback_days=2,
            now=now,
        )

        self.assertIsNotNone(created)
        self.assertFalse(ChartSeries.objects.filter(pk=oldest.pk).exists())
        self.assertEqual(ChartSeries.objects.count(), CHART_MAX_SERIES_PER_MARKET)

    def test_warms_frontend_default_continuous_trev_series(self):
        enabled_markets = FootprintMarket.objects.filter(enabled=True).count()

        queued = chart_service.ensure_default_chart_series(now=timezone.now())

        self.assertEqual(queued, enabled_markets)
        warm = ChartSeries.objects.filter(
            chart_type=ChartType.TREV,
            parameter_a=CHART_WARM_TREV_CONTINUATION_TICKS,
            parameter_b=CHART_WARM_TREV_REVERSAL_TICKS,
        )
        self.assertEqual(warm.count(), queued)
        self.assertEqual(
            set(warm.values_list("lookback_days", flat=True)),
            {None},
        )

    def test_failed_warm_series_waits_for_operator_action(self):
        now = timezone.now()
        failed = self.series(
            ChartType.TREV,
            parameter_a=CHART_WARM_TREV_CONTINUATION_TICKS,
            parameter_b=CHART_WARM_TREV_REVERSAL_TICKS,
            lookback_days=None,
            status=ChartSeriesStatus.FAILED,
        )
        ChartSeries.objects.filter(pk=failed.pk).update(
            last_requested_at=now - timedelta(hours=1)
        )

        self.assertEqual(chart_service.expire_stale_series(now=now), 0)
        self.assertEqual(
            chart_service.ensure_default_chart_series(now=now),
            FootprintMarket.objects.filter(enabled=True).count() - 1,
        )
        failed.refresh_from_db()
        self.assertEqual(failed.status, ChartSeriesStatus.FAILED)
        self.assertEqual(
            ChartSeries.objects.filter(
                market=self.market,
                chart_type=ChartType.TREV,
                parameter_a=CHART_WARM_TREV_CONTINUATION_TICKS,
                parameter_b=CHART_WARM_TREV_REVERSAL_TICKS,
                lookback_days=None,
            ).count(),
            1,
        )

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

    def test_trev_lookbacks_share_a_fixed_thirty_day_origin(self):
        now = timezone.now()
        request = dict(
            market=self.market,
            chart_type=ChartType.TREV,
            parameter_a=3,
            parameter_b=2,
            now=now,
        )
        first = chart_service.request_chart_series(**request, lookback_days=1)
        second = chart_service.request_chart_series(**request, lookback_days=30)
        self.assertEqual(first.pk, second.pk)
        self.assertIsNone(first.lookback_days)
        self.assertEqual(first.anchor_at, chart_service.lookback_anchor(now, 30))

    @patch("footprints.chart_service.publish_chart_update")
    def test_warm_trev_survives_idle_expiry_and_advances_without_renewing_lease(
        self, publish
    ):
        series = self.series(ChartType.TREV)
        first = self.execution(1, 100)
        bars = FakeBarStore()
        chart_service.build_series(
            series.pk, execution_store=FakeExecutionStore([first]), bar_store=bars
        )
        chart_service.advance_ready_series(
            execution_store=FakeExecutionStore([first]), bar_store=bars
        )
        idle = timezone.now() - timedelta(days=2)
        ChartSeries.objects.filter(pk=series.pk).update(last_requested_at=idle)
        self.assertEqual(chart_service.expire_stale_series(now=timezone.now()), 0)
        tail = FakeExecutionStore([first, self.execution(2, 105)])
        self.assertEqual(
            chart_service.advance_ready_series(execution_store=tail, bar_store=bars), 1
        )
        series.refresh_from_db()
        self.assertEqual(series.status, ChartSeriesStatus.READY)
        self.assertEqual(series.last_requested_at, idle)
        self.assertEqual(tail.calls[0]["after_cursor"], first.cursor)

    @patch("footprints.chart_service.publish_chart_update")
    def test_reopen_waits_for_all_tail_batches_independently_of_cold_builds(
        self, publish
    ):
        series = self.series(ChartType.TREV)
        rows = [self.execution(i, 100 + i) for i in range(1, 8)]
        bars = FakeBarStore()
        chart_service.build_series(
            series.pk, execution_store=FakeExecutionStore(rows[:3]), bar_store=bars
        )
        chart_service.advance_ready_series(
            execution_store=FakeExecutionStore(rows[:3]), bar_store=bars
        )
        series.refresh_from_db()
        checkpoint = series.checkpoint_state
        request = dict(
            market=self.market,
            chart_type=ChartType.TREV,
            parameter_a=series.parameter_a,
            parameter_b=series.parameter_b,
            lookback_days=1,
            now=timezone.now(),
        )
        for _ in range(2):
            opened = chart_service.request_chart_series(**request)
            self.assertEqual(opened.pk, series.pk)
            self.assertEqual(opened.status, ChartSeriesStatus.CATCHING_UP)
            self.assertEqual(opened.checkpoint_state, checkpoint)
        cold = self.series(
            ChartType.TREV, parameter_a=20, status=ChartSeriesStatus.QUEUED
        )
        self.assertEqual(chart_service.claim_queued_market_series(), (cold.pk,))
        tail = FakeExecutionStore(rows)
        with patch.object(chart_service, "LIVE_EXECUTION_LIMIT", 2):
            for expected_status in (
                ChartSeriesStatus.CATCHING_UP,
                ChartSeriesStatus.READY,
            ):
                self.assertEqual(
                    chart_service.advance_ready_series(
                        execution_store=tail, bar_store=bars
                    ),
                    2,
                )
                series.refresh_from_db()
                self.assertEqual(series.status, expected_status)
        self.assertEqual(
            [call["after_cursor"] for call in tail.calls],
            [rows[2].cursor, rows[4].cursor],
        )
        self.assertFalse(any(key[0] == cold.pk for key in bars.rows))
        cold.refresh_from_db()
        self.assertEqual(cold.status, ChartSeriesStatus.BUILDING)

    @patch("footprints.chart_service.publish_chart_update")
    def test_trev_resume_matches_full_replay_at_every_trade_boundary(self, publish):
        series = self.series(ChartType.TREV, parameter_a=3, parameter_b=2)
        spec = InstrumentSpec.objects.create(
            market=self.market,
            effective_from=self.anchor + timedelta(seconds=1),
            tick_size="0.01",
            quantity_step="0.001",
        )
        rows = [
            self.execution(
                i, price, epoch=1 if i < 7 else 2, buy=i % 2 == 0, quantity=i
            )
            for i, price in enumerate(
                (100, 101, 109, 107, 100, 95, 93, 97, 100, 99), start=1
            )
        ]
        rows[4] = rows[4]._replace(timestamp_us=rows[2].timestamp_us)
        rows[8:] = [row._replace(instrument_spec_id=spec.pk) for row in rows[8:]]
        expected = FakeBarStore()
        chart_service.build_series(
            series.pk, execution_store=FakeExecutionStore(rows), bar_store=expected
        )
        series.refresh_from_db()
        expected_checkpoint = series.checkpoint_state

        for split in range(1, len(rows)):
            with self.subTest(split=split):
                ChartSeries.objects.filter(pk=series.pk).update(
                    status="building", checkpoint_state=None
                )
                bars = FakeBarStore()
                chart_service.build_series(
                    series.pk,
                    execution_store=FakeExecutionStore(rows[:split]),
                    bar_store=bars,
                )
                tail = FakeExecutionStore(rows)
                with patch.object(chart_service, "LIVE_EXECUTION_LIMIT", 2):
                    while True:
                        chart_service._RUNTIMES.clear()
                        chart_service.advance_ready_series(
                            execution_store=tail, bar_store=bars
                        )
                        series.refresh_from_db()
                        if series.status == ChartSeriesStatus.READY:
                            break
                self.assertEqual(tail.calls[0]["after_cursor"], rows[split - 1].cursor)
                self.assertEqual(bars.rows, expected.rows)
                self.assertEqual(series.checkpoint_state, expected_checkpoint)

    @patch("footprints.chart_service.publish_chart_update")
    def test_catchup_retries_from_durable_state_after_lost_bar_acknowledgement(
        self, publish
    ):
        series = self.series(ChartType.TREV, parameter_a=3, parameter_b=2)
        rows = [self.execution(1, 100), self.execution(2, 101), self.execution(3, 109)]
        initial = FakeBarStore()
        chart_service.build_series(
            series.pk, execution_store=FakeExecutionStore(rows[:2]), bar_store=initial
        )
        series.refresh_from_db()
        checkpoint = series.checkpoint_state
        bars = CommitThenLoseAcknowledgementBarStore()
        tail = FakeExecutionStore(rows)
        with self.assertRaises(TimeoutError):
            chart_service.advance_ready_series(execution_store=tail, bar_store=bars)
        committed = dict(bars.rows)
        self.assertTrue(committed)
        series.refresh_from_db()
        self.assertEqual(series.checkpoint_state, checkpoint)
        self.assertEqual(series.status, ChartSeriesStatus.CATCHING_UP)
        publish.assert_not_called()
        chart_service.advance_ready_series(execution_store=tail, bar_store=bars)
        self.assertEqual(bars.rows, committed)
        self.assertEqual(tail.calls[0]["after_cursor"], tail.calls[1]["after_cursor"])
        series.refresh_from_db()
        self.assertEqual(series.status, ChartSeriesStatus.READY)
        self.assertEqual(series.checkpoint_state["cursor"], list(rows[-1].cursor))

    @patch("footprints.chart_service.publish_chart_update")
    def test_open_during_live_pass_is_not_overwritten_by_that_pass(self, publish):
        series = self.series(ChartType.TREV)
        rows = [self.execution(1, 100), self.execution(2, 105)]
        bars = FakeBarStore()
        chart_service.build_series(
            series.pk, execution_store=FakeExecutionStore(rows[:1]), bar_store=bars
        )
        chart_service.advance_ready_series(
            execution_store=FakeExecutionStore(rows[:1]), bar_store=bars
        )
        original_append = bars.append

        def request_during_append(batch):
            bars.append = original_append
            chart_service.request_chart_series(
                market=self.market,
                chart_type=ChartType.TREV,
                parameter_a=5,
                parameter_b=5,
                lookback_days=5,
                now=timezone.now(),
            )
            return original_append(batch)

        bars.append = request_during_append
        chart_service.advance_ready_series(
            execution_store=FakeExecutionStore(rows), bar_store=bars
        )
        series.refresh_from_db()
        self.assertEqual(series.status, ChartSeriesStatus.CATCHING_UP)
        self.assertEqual(series.checkpoint_state["cursor"], list(rows[0].cursor))
        chart_service.advance_ready_series(
            execution_store=FakeExecutionStore(rows), bar_store=bars
        )
        series.refresh_from_db()
        self.assertEqual(series.status, ChartSeriesStatus.READY)

    @patch("footprints.chart_service.publish_chart_update")
    def test_arrivals_after_a_catchup_pass_do_not_prevent_readiness(self, publish):
        series = self.series(ChartType.TREV)
        rows = [self.execution(1, 100), self.execution(2, 105)]
        bars = FakeBarStore()
        chart_service.build_series(
            series.pk, execution_store=FakeExecutionStore(rows[:1]), bar_store=bars
        )
        tail = FakeExecutionStore(rows)
        latest = tail.latest_cursor

        def arriving_trade(**kwargs):
            cursor = latest(**kwargs)
            tail.executions.append(self.execution(3, 110))
            return cursor

        tail.latest_cursor = arriving_trade
        chart_service.advance_ready_series(execution_store=tail, bar_store=bars)
        series.refresh_from_db()
        self.assertEqual(series.status, ChartSeriesStatus.READY)
        self.assertEqual(series.checkpoint_state["cursor"], list(rows[-1].cursor))

    def test_invalid_trev_checkpoint_replaces_generation_before_rebuilding(self):
        series = self.series(
            ChartType.TREV,
            status=ChartSeriesStatus.CATCHING_UP,
            checkpoint_state={"invalid": True},
        )
        untouched = self.series(
            ChartType.TREV, parameter_a=6, status=ChartSeriesStatus.QUEUED
        )
        with self.assertLogs("footprints.chart_service", level="ERROR"):
            chart_service.advance_ready_series(
                execution_store=FakeExecutionStore([]), bar_store=FakeBarStore()
            )
        replacement = ChartSeries.objects.get(
            market=self.market, chart_type=ChartType.TREV, parameter_a=5
        )
        self.assertNotEqual(replacement.pk, series.pk)
        self.assertNotEqual(replacement.public_key, series.public_key)
        self.assertIsNone(replacement.checkpoint_state)
        self.assertEqual(
            replacement.anchor_at, chart_service.lookback_anchor(timezone.now(), 30)
        )
        self.assertEqual(replacement.status, ChartSeriesStatus.QUEUED)
        self.assertTrue(ChartSeries.objects.filter(pk=untouched.pk).exists())

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
                len(active["levels"]) + sum(len(bar["levels"]) for bar in chunk) <= 5
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

    @patch.object(footprint_maintenance, "requeue_claimed_series")
    @patch.object(footprint_maintenance, "open_chart_stores")
    def test_worker_retries_transient_store_setup(self, open_stores, requeue):
        open_stores.side_effect = TimeoutError("storage timeout")

        with self.assertRaises(footprint_maintenance.TransientChartBuildError):
            footprint_maintenance._run_claimed_build((11, 12))

        requeue.assert_called_once_with((11, 12))

    @patch.object(footprint_maintenance, "requeue_claimed_series")
    @patch.object(footprint_maintenance, "open_chart_stores")
    def test_worker_stops_on_fatal_store_setup(self, open_stores, requeue):
        open_stores.side_effect = ValueError("invalid instrument specification")

        with self.assertRaises(footprint_maintenance.ChartBuildWorkerSetupError):
            footprint_maintenance._run_claimed_build((11, 12))

        requeue.assert_called_once_with((11, 12))
