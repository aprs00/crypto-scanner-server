from __future__ import annotations

import json
import threading
from decimal import Decimal

import websocket

from footprints.types import NormalizedTrade, TakerSide
from footprints.collectors.common import close_stream_on_error, run_websocket


def normalize_trade(
    data: dict,
    market_key: str,
    batch_index: int = 0,
    venue_sequence: int | None = None,
) -> NormalizedTrade:
    timestamp_ms = int(data["time"])
    coin = str(data["coin"])
    trade_id = str(data["tid"])
    side = str(data["side"])
    if side not in {"B", "A"}:
        raise ValueError("Hyperliquid trade has an invalid taker side")
    return NormalizedTrade(
        market_key=market_key,
        exchange_timestamp_us=timestamp_ms * 1_000,
        trade_id=f"{timestamp_ms}:{coin}:{trade_id}",
        venue_sequence=venue_sequence,
        batch_index=batch_index,
        price=Decimal(str(data["px"])),
        quantity=Decimal(str(data["sz"])),
        taker_side=TakerSide.BUY if side == "B" else TakerSide.SELL,
    )


def listen(markets: dict[str, str], on_trade, stop_event: threading.Event) -> None:
    url = "wss://api.hyperliquid.xyz/ws"
    def handle_open(socket) -> None:
        for symbol in markets:
            socket.send(
                json.dumps(
                    {
                        "method": "subscribe",
                        "subscription": {"type": "trades", "coin": symbol},
                    }
                )
            )

    def handle_message(_socket, message: str) -> None:
        payload = json.loads(message)
        if payload.get("channel") != "trades":
            return
        for data in payload.get("data", []):
            market_key = markets.get(str(data.get("coin", "")))
            if market_key:
                # The collector assigns one durable, process-restart-safe
                # arrival ordinal before committing the execution.
                on_trade(normalize_trade(data, market_key))

    ws = websocket.WebSocketApp(
        url,
        on_open=handle_open,
        on_message=handle_message,
        on_error=close_stream_on_error(stop_event),
    )
    run_websocket(ws, stop_event, ping_interval=30, ping_timeout=10)
