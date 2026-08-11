import json
import threading
from unittest.mock import Mock, patch

from django.test import SimpleTestCase

from footprints.collectors import binance
from footprints.collectors.binance import normalize_trade as normalize_binance
from footprints.collectors.bybit import SequenceOrder, normalize_trade as normalize_bybit
from footprints.collectors.hyperliquid import normalize_trade as normalize_hyperliquid
from footprints.types import TakerSide


class CollectorWebSocketTests(SimpleTestCase):
    @patch("footprints.collectors.binance.run_websocket")
    @patch("footprints.collectors.binance.websocket.WebSocketApp")
    def test_adapter_error_closes_stream_for_a_new_epoch(
        self,
        websocket_app,
        run_websocket,
    ):
        socket = Mock()
        websocket_app.return_value = socket
        stop_event = threading.Event()

        binance.listen(
            {"BTCUSDT": "binance:perpetual:BTCUSDT"},
            Mock(),
            stop_event,
        )
        on_error = websocket_app.call_args.kwargs["on_error"]
        with self.assertLogs(
            "footprints.collectors.common",
            level="WARNING",
        ) as captured:
            on_error(socket, ValueError("malformed trade"))

        self.assertTrue(stop_event.is_set())
        socket.close.assert_called_once_with()
        self.assertEqual(
            captured.output,
            [
                "WARNING:footprints.collectors.common:"
                "Execution WebSocket stream error: malformed trade"
            ],
        )
        run_websocket.assert_called_once_with(
            socket,
            stop_event,
            ping_interval=20,
            ping_timeout=10,
        )

    @patch("footprints.collectors.binance.run_websocket")
    @patch("footprints.collectors.binance.websocket.WebSocketApp")
    def test_binance_ignores_only_zero_value_synthetic_trade(
        self,
        websocket_app,
        _run_websocket,
    ):
        on_trade = Mock()
        stop_event = threading.Event()

        binance.listen(
            {"BTCUSDT": "binance:perpetual:BTCUSDT"},
            on_trade,
            stop_event,
        )
        on_message = websocket_app.call_args.kwargs["on_message"]
        on_message(
            Mock(),
            json.dumps(
                {
                    "e": "trade",
                    "s": "BTCUSDT",
                    "p": "0",
                    "q": "0",
                    "X": "NA",
                    "st": 1,
                }
            ),
        )

        on_trade.assert_not_called()
        self.assertFalse(stop_event.is_set())

        on_message(
            Mock(),
            json.dumps(
                {
                    "e": "trade",
                    "s": "BTCUSDT",
                    "T": 1_723_000_000_000,
                    "t": 123,
                    "m": False,
                    "p": "68000.1",
                    "q": "0.3",
                    "X": "NA",
                    "st": 1,
                }
            ),
        )

        on_trade.assert_called_once()


class CollectorNormalizationTests(SimpleTestCase):
    def test_binance_individual_trade_side_and_order(self):
        trade = normalize_binance(
            {"T": 1_700_000_000_000, "t": 99, "m": True, "p": "68000.1", "q": "0.3"},
            "binance:perpetual:BTCUSDT",
        )
        self.assertEqual(trade.taker_side, TakerSide.SELL)
        self.assertEqual(trade.venue_sequence, 99)

    def test_live_normalizers_reject_unknown_side_tokens(self):
        with self.assertRaisesRegex(ValueError, "maker flag"):
            normalize_binance(
                {"T": 1_700_000_000_000, "t": 99, "m": "false", "p": "68000.1", "q": "0.3"},
                "binance:perpetual:BTCUSDT",
            )
        with self.assertRaisesRegex(ValueError, "taker side"):
            normalize_bybit(
                {"T": 1_700_000_000_000, "i": "abc", "S": "Unknown", "p": "68000.1", "v": "0.01"},
                "bybit:perpetual:BTCUSDT",
            )
        with self.assertRaisesRegex(ValueError, "taker side"):
            normalize_hyperliquid(
                {"time": 1_700_000_000_000, "coin": "BTC", "tid": 7, "side": "?", "px": "68000", "sz": "0.001"},
                "hyperliquid:perpetual:BTC",
            )

    def test_bybit_preserves_sequence_and_batch_order(self):
        trade = normalize_bybit(
            {"T": 1_700_000_000_000, "i": "abc", "seq": 12, "S": "Buy", "p": "68000.1", "v": "0.01"},
            "bybit:perpetual:BTCUSDT",
            batch_index=7,
        )
        self.assertEqual(trade.taker_side, TakerSide.BUY)
        self.assertEqual((trade.venue_sequence, trade.batch_index), (12, 7))

    def test_bybit_order_continues_across_messages_with_the_same_sequence(self):
        order = SequenceOrder()
        self.assertEqual(order.next_index("BTCUSDT", 12), 0)
        self.assertEqual(order.next_index("BTCUSDT", 12), 1)
        self.assertEqual(order.next_index("ETHUSDT", 12), 0)
        self.assertEqual(order.next_index("BTCUSDT", 13), 0)

    def test_hyperliquid_has_global_trade_identity_and_arrival_order(self):
        trade = normalize_hyperliquid(
            {"time": 1_700_000_000_000, "coin": "BTC", "tid": 7, "side": "A", "px": "68000", "sz": "0.001"},
            "hyperliquid:perpetual:BTC",
            venue_sequence=42,
        )
        self.assertEqual(trade.taker_side, TakerSide.SELL)
        self.assertEqual(trade.trade_id, "1700000000000:BTC:7")
        self.assertEqual(trade.venue_sequence, 42)
