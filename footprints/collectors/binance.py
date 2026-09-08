from __future__ import annotations

import json
import threading
from collections.abc import Callable
from decimal import Decimal, InvalidOperation

import websocket

from footprints.collectors.common import close_stream_on_error, run_websocket
from footprints.collectors.rejected import reject
from footprints.types import NormalizedTrade, TakerSide


def _is_synthetic_trade(data: dict) -> bool:
    if data.get("X") != "NA" or str(data.get("st")) != "1":
        return False
    try:
        return Decimal(str(data.get("p"))) == 0 and Decimal(str(data.get("q"))) == 0
    except (InvalidOperation, TypeError, ValueError):
        return False


def normalize_trade(data: dict, market_key: str) -> NormalizedTrade:
    trade_id = int(data["t"])
    if trade_id < 0:
        raise ValueError("Binance trade ID cannot be negative")
    price, quantity = Decimal(str(data["p"])), Decimal(str(data["q"]))
    if not price.is_finite() or not quantity.is_finite():
        raise ValueError("Binance trade has non-finite values")
    maker_value = data["m"]
    if not isinstance(maker_value, bool):
        raise ValueError("Binance trade has an invalid maker flag")
    maker = maker_value
    return NormalizedTrade(
        market_key=market_key,
        exchange_timestamp_us=int(data["T"]) * 1_000,
        trade_id=str(trade_id),
        venue_sequence=trade_id,
        price=price,
        quantity=quantity,
        taker_side=TakerSide.SELL if maker else TakerSide.BUY,
    )


def listen(
    markets: dict[str, str],
    on_trade,
    stop_event: threading.Event,
    *,
    on_ready: Callable[[], None] | None = None,
) -> None:
    url = "wss://fstream.binance.com/ws"
    subscription_request_id = 1
    ready = False
    handle_error = close_stream_on_error(stop_event)

    def handle_open(socket) -> None:
        socket.send(
            json.dumps(
                {
                    "method": "SUBSCRIBE",
                    "params": [f"{symbol.lower()}@trade" for symbol in markets],
                    "id": subscription_request_id,
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
            reject("binance", message, error)
            return
        if payload.get("id") == subscription_request_id:
            if "result" in payload and payload["result"] is None:
                if not ready:
                    ready = True
                    if on_ready is not None:
                        on_ready()
            else:
                handle_error(
                    socket,
                    RuntimeError(
                        "Binance execution WebSocket subscription was rejected: "
                        f"{payload.get('msg', payload)}"
                    ),
                )
            return
        data = payload.get("data", payload)
        if not isinstance(data, dict):
            reject("binance", data, ValueError("expected a trade object"))
            return
        symbol = str(data.get("s", ""))
        market_key = markets.get(symbol)
        if market_key and data.get("e") == "trade":
            if _is_synthetic_trade(data):
                return
            try:
                trade = normalize_trade(data, market_key)
            except (ValueError, TypeError, KeyError, InvalidOperation) as error:
                reject("binance", data, error)
                return
            on_trade(trade)

    ws = websocket.WebSocketApp(
        url,
        on_open=handle_open,
        on_message=handle_message,
        on_error=handle_error,
    )
    run_websocket(ws, stop_event, ping_interval=20, ping_timeout=10)
