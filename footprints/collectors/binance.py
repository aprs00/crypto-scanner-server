from __future__ import annotations

import json
import threading
from decimal import Decimal, InvalidOperation

import websocket

from footprints.types import NormalizedTrade, TakerSide
from footprints.collectors.common import close_stream_on_error, run_websocket


def _is_synthetic_trade(data: dict) -> bool:
    if data.get("X") != "NA" or str(data.get("st")) != "1":
        return False
    try:
        return Decimal(str(data.get("p"))) == 0 and Decimal(str(data.get("q"))) == 0
    except (InvalidOperation, TypeError, ValueError):
        return False


def normalize_trade(data: dict, market_key: str) -> NormalizedTrade:
    trade_id = int(data["t"])
    maker_value = data["m"]
    if not isinstance(maker_value, bool):
        raise ValueError("Binance trade has an invalid maker flag")
    maker = maker_value
    return NormalizedTrade(
        market_key=market_key,
        exchange_timestamp_us=int(data["T"]) * 1_000,
        trade_id=str(trade_id),
        venue_sequence=trade_id,
        price=Decimal(str(data["p"])),
        quantity=Decimal(str(data["q"])),
        taker_side=TakerSide.SELL if maker else TakerSide.BUY,
    )


def listen(markets: dict[str, str], on_trade, stop_event: threading.Event) -> None:
    url = "wss://fstream.binance.com/ws"

    def handle_open(socket) -> None:
        socket.send(
            json.dumps(
                {
                    "method": "SUBSCRIBE",
                    "params": [f"{symbol.lower()}@trade" for symbol in markets],
                    "id": 1,
                }
            )
        )

    def handle_message(_socket, message: str) -> None:
        payload = json.loads(message)
        data = payload.get("data", payload)
        symbol = str(data.get("s", ""))
        market_key = markets.get(symbol)
        if market_key and data.get("e") == "trade":
            if _is_synthetic_trade(data):
                return
            on_trade(normalize_trade(data, market_key))

    ws = websocket.WebSocketApp(
        url,
        on_open=handle_open,
        on_message=handle_message,
        on_error=close_stream_on_error(stop_event),
    )
    run_websocket(ws, stop_event, ping_interval=20, ping_timeout=10)
