from __future__ import annotations

import gzip
import json
from dataclasses import replace
from datetime import timedelta
from unittest.mock import MagicMock, patch

from django.test import TestCase
from django.utils import timezone

from footprints import chart_service
from footprints.constants import (
    CHART_MAX_LEVELS_PER_BAR,
    CHART_MAX_SERIES_PER_MARKET,
    CHART_REQUEST_MAX_BYTES,
    CHART_SERIES_LEASE_SECONDS,
    CHART_SNAPSHOT_BARS,
    CHART_SNAPSHOT_MAX_LEVELS,
)
from footprints.models import ChartSeries, ChartSeriesStatus, ChartType, FootprintMarket
from footprints.storage.charts import ChartSnapshotBar
from footprints.tick import TickConfig, TickEngine, TickOrder, TickTrade
from footprints.tests.test_chart_service import FakeBarStore, FakeExecutionStore
from footprints.storage.charts import ProjectorExecution


class ChartApiTests(TestCase):
    def setUp(self):
        wakeup_patcher = patch("footprints.chart_api.publish_build_wakeup")
        self.publish_build_wakeup = wakeup_patcher.start()
        self.addCleanup(wakeup_patcher.stop)
        tail_patcher = patch("footprints.chart_api.publish_execution_wakeup")
        self.publish_execution_wakeup = tail_patcher.start()
        self.addCleanup(tail_patcher.stop)
        self.market = FootprintMarket.objects.get(exchange="binance", symbol="BTCUSDT")
        self.spec = self.market.instrument_specs.order_by("effective_from").first()
        self.anchor = chart_service.lookback_anchor(timezone.now())

    def post_series(self, payload):
        return self.client.post(
            "/api/v1/footprints/chart-series",
            data=json.dumps(
                {
                    "marketKey": self.market.market_key,
                    "lookbackDays": 2,
                    **payload,
                }
            ),
            content_type="application/json",
        )

    def test_market_catalog_has_the_minimal_contract(self):
        response = self.client.get("/api/v1/footprints/markets")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(set(payload), {"markets"})
        market = next(
            item
            for item in payload["markets"]
            if item["marketKey"] == self.market.market_key
        )
        self.assertEqual(
            set(market),
            {"marketKey", "exchange", "symbol", "baseAsset", "currentTickSize"},
        )

    def test_tick_and_trev_create_selected_lookback_series(self):
        tick = self.post_series({"chartType": "tick", "tradesPerBar": 500})
        trev = self.post_series(
            {
                "chartType": "trev",
                "continuationTicks": 20,
                "reversalTicks": 30,
            }
        )

        self.assertEqual((tick.status_code, trev.status_code), (202, 202))
        self.assertEqual(set(tick.json()), {"seriesKey", "status", "errorCode"})
        self.assertEqual(ChartSeries.objects.count(), 2)
        tick_row = ChartSeries.objects.get(chart_type=ChartType.TICK)
        trev_row = ChartSeries.objects.get(chart_type=ChartType.TREV)
        self.assertEqual(tick_row.anchor_at, self.anchor)
        self.assertEqual(tick_row.lookback_days, 2)
        self.assertEqual(
            trev_row.anchor_at, chart_service.lookback_anchor(timezone.now(), 30)
        )
        self.assertIsNone(trev_row.lookback_days)
        self.assertEqual(self.publish_build_wakeup.call_count, 2)

    def test_lookback_is_validated_and_part_of_the_series_identity(self):
        one_day = self.post_series(
            {"chartType": "tick", "lookbackDays": 1, "tradesPerBar": 500}
        )
        seven_days = self.post_series(
            {"chartType": "tick", "lookbackDays": 7, "tradesPerBar": 500}
        )
        invalid = self.post_series(
            {"chartType": "tick", "lookbackDays": 31, "tradesPerBar": 500}
        )

        self.assertNotEqual(one_day.json()["seriesKey"], seven_days.json()["seriesKey"])
        self.assertEqual(invalid.status_code, 400)
        self.assertEqual(invalid.json()["code"], "INVALID_LOOKBACK_DAYS")

    def test_same_configuration_reuses_the_series(self):
        first = self.post_series({"chartType": "tick", "tradesPerBar": 501})
        second = self.post_series({"chartType": "tick", "tradesPerBar": 501})

        self.assertEqual(second.json()["seriesKey"], first.json()["seriesKey"])
        self.assertEqual(ChartSeries.objects.count(), 1)

    def test_trev_open_catches_up_once_and_snapshot_retries_keep_the_same_series(self):
        payload = {"chartType": "trev", "continuationTicks": 3, "reversalTicks": 2}
        first = self.post_series({**payload, "lookbackDays": 1})
        second = self.post_series({**payload, "lookbackDays": 30})
        self.assertEqual(first.json()["seriesKey"], second.json()["seriesKey"])
        series = ChartSeries.objects.get(public_key=first.json()["seriesKey"])
        ChartSeries.objects.filter(pk=series.pk).update(
            status=ChartSeriesStatus.BUILDING
        )
        execution = ProjectorExecution(
            source_epoch=1,
            source_sequence=1,
            batch_index=0,
            timestamp_us=int(timezone.now().timestamp() * 1e6),
            trade_id="1",
            price_tick=100,
            quantity_units=2,
            is_taker_buy=True,
            instrument_spec_id=self.spec.pk,
        )
        executions = FakeExecutionStore([execution])
        bars = FakeBarStore()
        chart_service.build_series(
            series.pk, execution_store=executions, bar_store=bars
        )
        chart_service.advance_ready_series(execution_store=executions, bar_store=bars)
        series.refresh_from_db()
        checkpoint = series.checkpoint_state
        opened = self.post_series(payload)
        self.assertEqual(opened.status_code, 202)
        self.assertEqual(opened.json()["status"], "catching_up")
        self.publish_execution_wakeup.assert_called_once_with((self.market.pk,))
        for _ in range(2):
            pending = self.client.get(
                "/api/v1/footprints/chart-bars", {"seriesKey": series.public_key}
            )
            self.assertEqual(pending.status_code, 409)
            self.assertEqual(pending.json()["status"], "catching_up")
        series.refresh_from_db()
        self.assertEqual(series.checkpoint_state, checkpoint)
        chart_service.advance_ready_series(execution_store=executions, bar_store=bars)
        with patch("footprints.chart_api._chart_bar_store") as factory:
            factory.return_value.history_start_index.return_value = 0
            factory.return_value.read_snapshot.return_value = []
            ready = self.client.get(
                "/api/v1/footprints/chart-bars", {"seriesKey": series.public_key}
            )
        self.assertEqual(ready.status_code, 200)
        self.assertEqual(ready.json()["activeBar"]["tradeCount"], 1)
        chart_service._RUNTIMES.clear()

    @patch("footprints.chart_api._chart_bar_store")
    def test_history_pagination_remains_available_while_tip_is_catching_up(
        self, factory
    ):
        series = self._ready_tick_series()
        ChartSeries.objects.filter(pk=series.pk).update(
            status=ChartSeriesStatus.CATCHING_UP
        )
        factory.return_value.history_start_index.return_value = 1
        factory.return_value.read_snapshot.return_value = [self._bar(series, 1)]
        params = {"seriesKey": series.public_key}
        self.assertEqual(
            self.client.get("/api/v1/footprints/chart-bars", params).status_code, 409
        )
        history = self.client.get(
            "/api/v1/footprints/chart-bars", {**params, "beforeBarIndex": 2}
        )
        self.assertEqual(history.status_code, 200)
        self.assertIsNone(history.json()["activeBar"])
        self.assertFalse(history.json()["hasMore"])
        self.assertEqual(history.json()["historyStartBarIndex"], 1)
        self.assertIsNone(history.json()["nextBeforeBarIndex"])

    @patch("footprints.chart_api._chart_bar_store")
    def test_trev_history_lookback_filters_reads_without_reanchoring(self, factory):
        series = self._ready_tick_series()
        # Snapshot construction is engine-specific, but the stored-bar range is not.
        ChartSeries.objects.filter(pk=series.pk).update(
            chart_type=ChartType.TREV, parameter_b=2, lookback_days=None
        )
        with patch(
            "footprints.chart_api.snapshot_state_for_series", return_value=(None, 10)
        ):
            factory.return_value.history_start_index.return_value = 7
            factory.return_value.read_snapshot.return_value = [
                self._bar(series, 8),
                self._bar(series, 7),
            ]
            now = timezone.now()
            with patch("footprints.chart_api.timezone.now", return_value=now):
                response = self.client.get(
                    "/api/v1/footprints/chart-bars",
                    {"seriesKey": series.public_key, "lookbackDays": 1},
                )
        self.assertEqual(response.status_code, 200)
        self.assertFalse(response.json()["hasMore"])
        factory.return_value.history_start_index.assert_called_once_with(
            series_id=series.pk, before_bar_index=10, since=now - timedelta(days=1)
        )
        factory.return_value.read_snapshot.assert_called_once_with(
            series_id=series.pk,
            limit=500,
            max_levels=CHART_SNAPSHOT_MAX_LEVELS,
            before_bar_index=10,
            min_bar_index=7,
        )
        series.refresh_from_db()
        self.assertEqual(series.anchor_at, self.anchor)

    def test_snapshot_rejects_invalid_lookback(self):
        series = self._ready_tick_series()
        for value in ("bad", 0, 31):
            with self.subTest(value=value):
                response = self.client.get(
                    "/api/v1/footprints/chart-bars",
                    {"seriesKey": series.public_key, "lookbackDays": value},
                )
                self.assertEqual(response.status_code, 400)
                self.assertEqual(response.json()["code"], "INVALID_LOOKBACK_DAYS")

    def test_live_series_request_renews_its_lease(self):
        first = self.post_series({"chartType": "tick", "tradesPerBar": 501})
        series = ChartSeries.objects.get(public_key=first.json()["seriesKey"])
        previous = timezone.now() - timedelta(minutes=5)
        ChartSeries.objects.filter(pk=series.pk).update(last_requested_at=previous)

        second = self.post_series({"chartType": "tick", "tradesPerBar": 501})

        self.assertEqual(second.json()["seriesKey"], first.json()["seriesKey"])
        series.refresh_from_db()
        self.assertGreater(series.last_requested_at, previous)

    def test_dormant_ready_series_is_queued_from_its_saved_checkpoint(self):
        checkpoint = {"engine": {"saved": True}, "cursor": [1, 2, 0, 3, "4"]}
        series = ChartSeries.objects.create(
            market=self.market,
            chart_type=ChartType.TICK,
            parameter_a=502,
            status=ChartSeriesStatus.READY,
            anchor_at=self.anchor,
            checkpoint_state=checkpoint,
        )
        ChartSeries.objects.filter(pk=series.pk).update(
            last_requested_at=timezone.now()
            - timedelta(seconds=CHART_SERIES_LEASE_SECONDS + 1)
        )

        response = self.post_series(
            {"chartType": "tick", "tradesPerBar": series.parameter_a}
        )

        self.assertEqual(response.status_code, 202)
        self.assertEqual(response.json()["seriesKey"], str(series.public_key))
        series.refresh_from_db()
        self.assertEqual(series.status, ChartSeriesStatus.QUEUED)
        self.assertEqual(series.checkpoint_state, checkpoint)
        self.publish_build_wakeup.assert_called_once_with()

    def test_market_capacity_is_bounded_and_expired_rows_are_reclaimed(self):
        for trades_per_bar in range(1, CHART_MAX_SERIES_PER_MARKET + 1):
            response = self.post_series(
                {"chartType": "tick", "tradesPerBar": trades_per_bar}
            )
            self.assertEqual(response.status_code, 202)

        full = self.post_series(
            {
                "chartType": "trev",
                "continuationTicks": 20,
                "reversalTicks": 30,
            }
        )
        self.assertEqual(full.status_code, 429)
        self.assertEqual(full.json()["code"], "SERIES_CAPACITY_REACHED")

        oldest = ChartSeries.objects.order_by("pk").first()
        ChartSeries.objects.filter(pk=oldest.pk).update(
            last_requested_at=timezone.now()
            - timedelta(seconds=CHART_SERIES_LEASE_SECONDS + 1)
        )
        reclaimed = self.post_series(
            {
                "chartType": "trev",
                "continuationTicks": 20,
                "reversalTicks": 30,
            }
        )
        self.assertEqual(reclaimed.status_code, 202)
        self.assertEqual(ChartSeries.objects.count(), CHART_MAX_SERIES_PER_MARKET)

    def test_chart_series_request_body_is_bounded(self):
        response = self.client.post(
            "/api/v1/footprints/chart-series",
            data=b"{" + b" " * CHART_REQUEST_MAX_BYTES + b"}",
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 413)
        self.assertEqual(response.json()["code"], "REQUEST_TOO_LARGE")

        declared = self.client.post(
            "/api/v1/footprints/chart-series",
            data=b"{}",
            content_type="application/json",
            CONTENT_LENGTH=str(CHART_REQUEST_MAX_BYTES + 1),
        )
        self.assertEqual(declared.status_code, 413)
        self.assertEqual(declared.json()["code"], "REQUEST_TOO_LARGE")

        malformed = self.client.post(
            "/api/v1/footprints/chart-series",
            data=b"{}",
            content_type="application/json",
            CONTENT_LENGTH="invalid",
        )
        self.assertEqual(malformed.status_code, 400)
        self.assertEqual(malformed.json()["code"], "INVALID_CONTENT_LENGTH")

    def test_malformed_series_key_is_a_stable_not_found(self):
        response = self.client.get(
            "/api/v1/footprints/chart-bars",
            {"seriesKey": "not-a-uuid"},
        )

        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.json()["code"], "UNKNOWN_CHART_SERIES")

    def test_chart_history_range_is_bounded(self):
        series = self._ready_tick_series()

        invalid_cursor = self.client.get(
            "/api/v1/footprints/chart-bars",
            {"seriesKey": series.public_key, "beforeBarIndex": "invalid"},
        )
        oversized_page = self.client.get(
            "/api/v1/footprints/chart-bars",
            {"seriesKey": series.public_key, "limit": CHART_SNAPSHOT_BARS + 1},
        )

        self.assertEqual(invalid_cursor.status_code, 400)
        self.assertEqual(oversized_page.status_code, 400)
        self.assertEqual(invalid_cursor.json()["code"], "INVALID_CHART_RANGE")

    def test_failed_series_is_observed_without_an_automatic_retry(self):
        original_anchor = self.anchor - timedelta(days=5)
        series = ChartSeries.objects.create(
            market=self.market,
            chart_type=ChartType.TICK,
            parameter_a=504,
            status=ChartSeriesStatus.FAILED,
            anchor_at=original_anchor,
            error_code="TimeoutError",
        )

        response = self.post_series({"chartType": "tick", "tradesPerBar": 504})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["errorCode"], "TimeoutError")
        series.refresh_from_db()
        self.assertEqual(series.status, ChartSeriesStatus.FAILED)
        self.assertEqual(series.anchor_at, original_anchor)
        self.assertIsNone(series.checkpoint_state)

    @patch("footprints.chart_api._chart_bar_store")
    def test_chart_bars_always_return_latest_native_levels_and_active_bar(
        self, store_factory
    ):
        series = self._ready_tick_series()
        store = MagicMock()
        store.history_start_index.return_value = 0
        store.read_snapshot.return_value = [self._bar(series, 2), self._bar(series, 1)]
        store_factory.return_value = store

        response = self.client.get(
            "/api/v1/footprints/chart-bars",
            {"seriesKey": series.public_key},
        )

        self.assertEqual(response.status_code, 200, response.content)
        payload = response.json()
        self.assertEqual(
            set(payload),
            {
                "seriesKey",
                "bars",
                "activeBar",
                "hasMore",
                "nextBeforeBarIndex",
                "historyStartBarIndex",
            },
        )
        self.assertEqual([bar["barIndex"] for bar in payload["bars"]], [1, 2])
        self.assertEqual(payload["bars"][0]["levels"], [[100, 1, 2]])
        self.assertEqual(payload["activeBar"]["levels"], [[100, 2, 0]])
        store.read_snapshot.assert_called_once_with(
            series_id=series.pk,
            limit=CHART_SNAPSHOT_BARS,
            max_levels=CHART_SNAPSHOT_MAX_LEVELS - 1,
            before_bar_index=3,
            min_bar_index=0,
        )

    @patch("footprints.chart_api._chart_bar_store")
    def test_chart_bars_page_back_without_repeating_the_active_bar(self, store_factory):
        series = self._ready_tick_series()
        store = MagicMock()
        store.history_start_index.return_value = 0
        store.read_snapshot.return_value = [
            self._bar(series, 4),
            self._bar(series, 3),
        ]
        store_factory.return_value = store

        response = self.client.get(
            "/api/v1/footprints/chart-bars",
            {
                "seriesKey": series.public_key,
                "beforeBarIndex": 5,
                "limit": 2,
            },
        )

        self.assertEqual(response.status_code, 200, response.content)
        payload = response.json()
        self.assertEqual([bar["barIndex"] for bar in payload["bars"]], [3, 4])
        self.assertIsNone(payload["activeBar"])
        self.assertTrue(payload["hasMore"])
        self.assertEqual(payload["nextBeforeBarIndex"], 3)
        store.read_snapshot.assert_called_once_with(
            series_id=series.pk,
            limit=2,
            max_levels=CHART_SNAPSHOT_MAX_LEVELS,
            before_bar_index=3,
            min_bar_index=0,
        )

    @patch("footprints.chart_api._chart_bar_store")
    def test_chart_bars_compress_large_browser_responses(self, store_factory):
        series = self._ready_tick_series()
        store = MagicMock()
        store.history_start_index.return_value = 0
        store.read_snapshot.return_value = [
            self._bar(series, index) for index in range(20)
        ]
        store_factory.return_value = store

        response = self.client.get(
            "/api/v1/footprints/chart-bars",
            {"seriesKey": series.public_key},
            HTTP_ACCEPT_ENCODING="gzip",
        )

        self.assertEqual(response.status_code, 200, response.content)
        self.assertEqual(response.headers["Content-Encoding"], "gzip")
        payload = json.loads(gzip.decompress(response.content))
        self.assertEqual(len(payload["bars"]), 20)

    @patch("footprints.chart_api._chart_bar_store")
    def test_uncommitted_closed_rows_are_excluded_from_the_active_checkpoint(
        self, store_factory
    ):
        series = self._ready_tick_series()
        store = MagicMock()
        store.history_start_index.return_value = 0
        stored = [self._bar(series, 3), self._bar(series, 2)]
        store.read_snapshot.side_effect = lambda **query: [
            bar for bar in stored if bar.bar_index < query["before_bar_index"]
        ]
        store_factory.return_value = store

        response = self.client.get(
            "/api/v1/footprints/chart-bars",
            {"seriesKey": series.public_key},
        )

        self.assertEqual(response.status_code, 200, response.content)
        self.assertEqual(
            [bar["barIndex"] for bar in response.json()["bars"]],
            [2],
        )
        self.assertEqual(response.json()["activeBar"]["barIndex"], 3)
        self.assertEqual(store.read_snapshot.call_args.kwargs["before_bar_index"], 3)

    @patch("footprints.chart_api._chart_bar_store")
    def test_dense_cached_closed_bar_is_terminal(self, store_factory):
        series = self._ready_tick_series()
        dense = replace(
            self._bar(series, 2),
            level_ticks=tuple(range(1, CHART_MAX_LEVELS_PER_BAR + 2)),
            level_buy_units=(1,) * (CHART_MAX_LEVELS_PER_BAR + 1),
            level_sell_units=(0,) * (CHART_MAX_LEVELS_PER_BAR + 1),
        )
        store = MagicMock()
        store.history_start_index.return_value = 0
        store.read_snapshot.return_value = [dense]
        store_factory.return_value = store

        response = self.client.get(
            "/api/v1/footprints/chart-bars",
            {"seriesKey": series.public_key},
        )

        self.assertEqual(response.status_code, 422)
        self.assertEqual(response.json()["code"], "CHART_BAR_TOO_DENSE")
        series.refresh_from_db()
        self.assertEqual(series.status, ChartSeriesStatus.FAILED)

    def _ready_tick_series(self) -> ChartSeries:
        engine = TickEngine(TickConfig(2))
        anchor_us = int(self.anchor.timestamp() * 1_000_000)
        for rank in range(7):
            engine.push(
                TickTrade(
                    TickOrder(anchor_us + rank, rank),
                    100,
                    2,
                    True,
                    self.spec.pk,
                )
            )
        cursor = (1, 7, 0, anchor_us + 6, "7")
        checkpoint = chart_service._checkpoint_state(engine, cursor)
        return ChartSeries.objects.create(
            market=self.market,
            chart_type=ChartType.TICK,
            parameter_a=2,
            status=ChartSeriesStatus.READY,
            anchor_at=self.anchor,
            checkpoint_state=checkpoint,
        )

    def _bar(self, series: ChartSeries, index: int) -> ChartSnapshotBar:
        opened = self.anchor + timedelta(seconds=index)
        return ChartSnapshotBar(
            series_id=series.pk,
            bar_index=index,
            opened_at=opened,
            instrument_spec_id=self.spec.pk,
            open_tick=100,
            high_tick=102,
            low_tick=99,
            close_tick=101,
            buy_units=3,
            sell_units=2,
            trade_count=2,
            break_before=False,
            level_ticks=(100,),
            level_buy_units=(1,),
            level_sell_units=(2,),
        )
