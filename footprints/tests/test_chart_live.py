from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from asgiref.sync import async_to_sync
from channels.testing import WebsocketCommunicator
from django.test import SimpleTestCase, TestCase
from django.test.utils import override_settings
from django.utils import timezone

from footprints.chart_consumers import ChartConsumer, _series_status
from footprints.chart_live import publish_chart_error, publish_chart_update
from footprints.constants import CHART_SERIES_LEASE_SECONDS
from footprints.models import ChartSeries, ChartType, FootprintMarket


class ChartLiveContractTests(SimpleTestCase):
    def test_replacement_notice_unsubscribes_old_series(self):
        consumer = SimpleNamespace(
            subscriptions={"retired": "group"},
            send_json=AsyncMock(),
            _unsubscribe=AsyncMock(),
        )
        async_to_sync(ChartConsumer.chart_error)(
            consumer, {"series_key": "retired", "code": "CHART_SERIES_REPLACED"}
        )
        consumer._unsubscribe.assert_awaited_once_with("retired")
        consumer.send_json.assert_awaited_once_with(
            {"type": "error", "seriesKey": "retired", "code": "CHART_SERIES_REPLACED"}
        )

    def test_binary_and_oversized_frames_close_before_json_decoding(self):
        frames = (
            {"bytes_data": b"{}"},
            {"text_data": "x" * 2_049},
        )
        for frame in frames:
            with self.subTest(frame=next(iter(frame))):
                consumer = SimpleNamespace(close=AsyncMock())

                async_to_sync(ChartConsumer.receive)(consumer, **frame)

                consumer.close.assert_awaited_once_with(code=4400)

    @override_settings(ALLOWED_HOSTS=["frontend.example.test"])
    def test_asgi_accepts_an_allowed_websocket_origin(self):
        async def connect():
            from core.asgi import application

            communicator = WebsocketCommunicator(
                application,
                "/ws/v1/charts/",
                headers=[(b"origin", b"https://frontend.example.test")],
            )
            connected, _subprotocol = await communicator.connect()
            if connected:
                await communicator.disconnect()
            return connected

        self.assertTrue(async_to_sync(connect)())

    @override_settings(ALLOWED_HOSTS=["api.example.test"])
    def test_asgi_rejects_an_unknown_websocket_origin(self):
        async def connect():
            from core.asgi import application

            communicator = WebsocketCommunicator(
                application,
                "/ws/v1/charts/",
                headers=[(b"origin", b"https://evil.example.test")],
            )
            connected, _subprotocol = await communicator.connect()
            if connected:
                await communicator.disconnect()
            return connected

        self.assertFalse(async_to_sync(connect)())

    def test_unscoped_messages_are_closed_instead_of_emitting_invalid_errors(self):
        consumer = SimpleNamespace(close=AsyncMock())

        async_to_sync(ChartConsumer.receive_json)(consumer, [])
        async_to_sync(ChartConsumer.receive_json)(consumer, {"op": "subscribe"})

        self.assertEqual(
            [call.kwargs for call in consumer.close.await_args_list],
            [{"code": 4400}, {"code": 4400}],
        )

    def test_publisher_sends_minimal_series_scoped_update(self):
        layer = SimpleNamespace(group_send=AsyncMock())

        with patch("footprints.chart_live.get_channel_layer", return_value=layer):
            publish_chart_update(
                "series-1",
                bars=[{"barIndex": 3}],
                active_bar={"barIndex": 4},
            )

        layer.group_send.assert_awaited_once_with(
            "chart.series-1",
            {
                "type": "chart.update",
                "series_key": "series-1",
                "bars": [{"barIndex": 3}],
                "active_bar": {"barIndex": 4},
            },
        )

    def test_consumer_ack_and_update_use_the_minimal_contract(self):
        consumer = SimpleNamespace(send_json=AsyncMock())

        async_to_sync(ChartConsumer._ack)(consumer, "series-1")
        consumer.subscriptions = {"series-1": "chart.series-1"}
        async_to_sync(ChartConsumer.chart_update)(
            consumer,
            {
                "series_key": "series-1",
                "bars": [{"barIndex": 3}],
                "active_bar": None,
            },
        )

        self.assertEqual(
            [call.args[0] for call in consumer.send_json.await_args_list],
            [
                {"type": "subscribed", "seriesKey": "series-1"},
                {
                    "type": "update",
                    "seriesKey": "series-1",
                    "bars": [{"barIndex": 3}],
                    "activeBar": None,
                },
            ],
        )

    def test_error_is_scoped_to_the_failed_series(self):
        layer = SimpleNamespace(group_send=AsyncMock())
        with patch("footprints.chart_live.get_channel_layer", return_value=layer):
            publish_chart_error("series-1", "TREV_EXPANSION_LIMIT")

        layer.group_send.assert_awaited_once_with(
            "chart.series-1",
            {
                "type": "chart.error",
                "series_key": "series-1",
                "code": "TREV_EXPANSION_LIMIT",
            },
        )

        consumer = SimpleNamespace(
            send_json=AsyncMock(),
            subscriptions={"series-1": "chart.series-1"},
        )
        async_to_sync(ChartConsumer.chart_error)(
            consumer,
            {
                "series_key": "series-1",
                "code": "TREV_EXPANSION_LIMIT",
            },
        )
        consumer.send_json.assert_awaited_once_with(
            {
                "type": "error",
                "code": "TREV_EXPANSION_LIMIT",
                "seriesKey": "series-1",
            }
        )


class ChartLiveLeaseTests(TestCase):
    def setUp(self):
        self.market = FootprintMarket.objects.get(exchange="binance", symbol="BTCUSDT")

    def test_subscription_renews_only_a_live_series(self):
        series = ChartSeries.objects.create(
            market=self.market,
            chart_type=ChartType.TICK,
            parameter_a=500,
            anchor_at=timezone.now(),
        )
        previous = timezone.now() - timedelta(minutes=5)
        ChartSeries.objects.filter(pk=series.pk).update(last_requested_at=previous)

        self.assertEqual(_series_status(str(series.public_key)), "queued")
        series.refresh_from_db()
        self.assertGreater(series.last_requested_at, previous)

        expired_at = timezone.now() - timedelta(seconds=CHART_SERIES_LEASE_SECONDS + 1)
        ChartSeries.objects.filter(pk=series.pk).update(last_requested_at=expired_at)
        self.assertIsNone(_series_status(str(series.public_key)))
        series.refresh_from_db()
        self.assertEqual(series.last_requested_at, expired_at)

    def test_malformed_subscription_key_is_unknown(self):
        self.assertIsNone(_series_status("not-a-uuid"))
