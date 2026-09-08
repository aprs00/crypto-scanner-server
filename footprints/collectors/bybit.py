from __future__ import annotations

import json
import threading
from collections.abc import Callable
from decimal import Decimal, InvalidOperation

import websocket

from footprints.collectors.common import close_stream_on_error, run_websocket
from footprints.collectors.rejected import reject
from footprints.types import NormalizedTrade, TakerSide


def normalize_trade(
    data: dict, market_key: str, batch_index: int = 0
) -> NormalizedTrade:
    if not isinstance(data, dict) or not data.get("i"):
        raise ValueError("Bybit trade is missing its identity")
    trade_id = str(data["i"])
    side = str(data["S"])
    if side not in {"Buy", "Sell"}:
        raise ValueError("Bybit trade has an invalid taker side")
    sequence = int(data["seq"])
    price, quantity = Decimal(str(data["p"])), Decimal(str(data["v"]))
    if sequence < 0 or not price.is_finite() or not quantity.is_finite():
        raise ValueError("Bybit trade has invalid sequence or non-finite values")
    return NormalizedTrade(
        market_key=market_key,
        exchange_timestamp_us=int(data["T"]) * 1_000,
        trade_id=trade_id,
        venue_sequence=sequence,
        batch_index=batch_index,
        price=price,
        quantity=quantity,
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
        try:
            payload = json.loads(message)
            if not isinstance(payload, dict):
                raise ValueError("expected an object")
        except (ValueError, TypeError) as error:
            reject("bybit", message, error)
            return
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
        records = payload.get("data", [])
        if not isinstance(records, list):
            reject("bybit", records, ValueError("expected a trade list"))
            return
        for data in records:
            try:
                trade = normalize_trade(data, market_key)
            except (ValueError, TypeError, KeyError, InvalidOperation) as error:
                reject("bybit", data, error)
                continue
            on_trade(trade)

    ws = websocket.WebSocketApp(
        url,
        on_open=handle_open,
        on_message=handle_message,
        on_error=handle_error,
    )
    run_websocket(ws, stop_event, ping_interval=20, ping_timeout=10)
