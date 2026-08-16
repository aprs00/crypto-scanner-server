from __future__ import annotations

import json
import threading
from collections.abc import Callable
from decimal import Decimal

import websocket

from footprints.types import NormalizedTrade, TakerSide
from footprints.collectors.common import close_stream_on_error, run_websocket


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


def listen(
    markets: dict[str, str],
    on_trade,
    stop_event: threading.Event,
    *,
    on_ready: Callable[[], None] | None = None,
) -> None:
    url = "wss://stream.bybit.com/v5/public/linear"
    ready = False
    handle_error = close_stream_on_error(stop_event)

    def handle_open(socket) -> None:
        socket.send(
            json.dumps(
                {
                    "op": "subscribe",
                    "args": [f"publicTrade.{symbol}" for symbol in markets],
                }
            )
        )

    def handle_message(socket, message: str) -> None:
        nonlocal ready
        payload = json.loads(message)
        if payload.get("op") == "subscribe":
            if payload.get("success") is True:
                if not ready:
                    ready = True
                    if on_ready is not None:
                        on_ready()
            else:
                handle_error(
                    socket,
                    RuntimeError(
                        "Bybit execution WebSocket subscription was rejected: "
                        f"{payload.get('ret_msg', payload)}"
                    ),
                )
            return
        topic = str(payload.get("topic", ""))
        if not topic.startswith("publicTrade."):
            return
        symbol = topic.removeprefix("publicTrade.")
        market_key = markets.get(symbol)
        if not market_key:
            return
        for data in payload.get("data", []):
            on_trade(normalize_trade(data, market_key))

    ws = websocket.WebSocketApp(
        url,
        on_open=handle_open,
        on_message=handle_message,
        on_error=handle_error,
    )
    run_websocket(ws, stop_event, ping_interval=20, ping_timeout=10)
