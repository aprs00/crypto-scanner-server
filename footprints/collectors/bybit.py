from __future__ import annotations

import json
import threading
from decimal import Decimal

import websocket

from footprints.types import NormalizedTrade, TakerSide
from footprints.collectors.common import close_stream_on_error, run_websocket


class SequenceOrder:
    """Preserve delivery order when Bybit splits one sequence across messages."""

    def __init__(self) -> None:
        self._state: dict[str, tuple[int | None, int]] = {}

    def next_index(self, symbol: str, sequence: int | None) -> int:
        previous_sequence, next_index = self._state.get(symbol, (None, 0))
        index = next_index if sequence == previous_sequence else 0
        self._state[symbol] = (sequence, index + 1)
        return index


def normalize_trade(data: dict, market_key: str, batch_index: int = 0) -> NormalizedTrade:
    trade_id = str(data["i"])
    side = str(data["S"])
    if side not in {"Buy", "Sell"}:
        raise ValueError("Bybit trade has an invalid taker side")
    sequence = int(data["seq"])
    return NormalizedTrade(
        market_key=market_key,
        exchange_timestamp_us=int(data["T"]) * 1_000,
        trade_id=trade_id,
        venue_sequence=sequence,
        batch_index=batch_index,
        price=Decimal(str(data["p"])),
        quantity=Decimal(str(data["v"])),
        taker_side=TakerSide.BUY if side == "Buy" else TakerSide.SELL,
    )


def listen(markets: dict[str, str], on_trade, stop_event: threading.Event) -> None:
    url = "wss://stream.bybit.com/v5/public/linear"
    sequence_order = SequenceOrder()

    def handle_open(socket) -> None:
        socket.send(
            json.dumps(
                {
                    "op": "subscribe",
                    "args": [f"publicTrade.{symbol}" for symbol in markets],
                }
            )
        )

    def handle_message(_socket, message: str) -> None:
        payload = json.loads(message)
        topic = str(payload.get("topic", ""))
        if not topic.startswith("publicTrade."):
            return
        symbol = topic.removeprefix("publicTrade.")
        market_key = markets.get(symbol)
        if not market_key:
            return
        for data in payload.get("data", []):
            sequence = int(data["seq"])
            batch_index = sequence_order.next_index(symbol, sequence)
            on_trade(normalize_trade(data, market_key, batch_index))

    ws = websocket.WebSocketApp(
        url,
        on_open=handle_open,
        on_message=handle_message,
        on_error=close_stream_on_error(stop_event),
    )
    run_websocket(ws, stop_event, ping_interval=20, ping_timeout=10)
