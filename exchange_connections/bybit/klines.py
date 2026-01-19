"""
Bybit Perpetual Futures Kline Collector

Features:
- Auto-reconnect in-process (never exits)
- Robust heartbeat monitoring (20s ping, 90s pong timeout)
- On reconnect: detect gaps (via BTC) and backfill all symbols
"""

import json
import time
import threading
import requests
import websocket
from decimal import Decimal
from typing import List, Set, Optional

from exchange_connections.base import BaseKlineCollector
from exchange_connections.candle_types import NormalizedCandle
from core.constants import Exchange

# Bybit V5 API endpoints
BYBIT_WS_URL = "wss://stream.bybit.com/v5/public/linear"
BYBIT_API_BASE = "https://api.bybit.com"
BYBIT_INSTRUMENTS_URL = f"{BYBIT_API_BASE}/v5/market/instruments-info"

# WebSocket configuration - disable websocket-client's ping, we use JSON ping
WS_PING_INTERVAL = 0
WS_PING_TIMEOUT = None

# Heartbeat configuration (Bybit recommends 20s ping interval)
HEARTBEAT_INTERVAL = 20
PONG_TIMEOUT_SECONDS = 90

# Subscription limits
MAX_ARGS_PER_SUBSCRIBE = 10
SUBSCRIBE_DELAY = 0.1


class BybitKlineCollector(BaseKlineCollector):
    """
    Collects 1-minute klines from Bybit linear perpetual futures.

    Uses Bybit V5 WebSocket API with:
    - JSON ping/pong heartbeat (20s interval)
    - Only processes confirmed (closed) candles
    """

    def __init__(self):
        super().__init__(exchange=Exchange.BYBIT, contract_type="perpetual")
        self.ws: Optional[websocket.WebSocketApp] = None
        self.ws_connected = False

        # Heartbeat tracking
        self.connection_start_time = 0
        self.last_pong_time = 0
        self.heartbeat_count = 0

    def fetch_perpetual_symbols(self) -> Set[str]:
        """Fetch all USDT linear perpetual symbols from Bybit API with pagination."""
        try:
            symbols = set()
            cursor = None

            while True:
                params = {
                    "category": "linear",
                    "limit": 1000,
                }
                if cursor:
                    params["cursor"] = cursor

                response = requests.get(
                    BYBIT_INSTRUMENTS_URL,
                    params=params,
                    timeout=30,
                )
                response.raise_for_status()
                data = response.json()

                if data.get("retCode") != 0:
                    print(f"[bybit] ERROR: API error: {data.get('retMsg')}")
                    break

                result = data.get("result", {})
                instruments = result.get("list", [])

                for instrument in instruments:
                    if (
                        instrument.get("contractType") == "LinearPerpetual"
                        and instrument.get("quoteCoin") == "USDT"
                        and instrument.get("status") == "Trading"
                    ):
                        symbols.add(instrument["symbol"])

                cursor = result.get("nextPageCursor")
                if not cursor:
                    break

                time.sleep(0.1)

            print(f"[bybit] Fetched {len(symbols)} linear perpetual symbols")
            return symbols

        except Exception as e:
            print(f"[bybit] ERROR: Failed to fetch symbols: {e}")
            return set()

    def normalize_candle(self, raw_data: dict) -> Optional[NormalizedCandle]:
        """Convert Bybit kline format to NormalizedCandle."""
        try:
            return NormalizedCandle(
                open_time_ms=int(raw_data["start"]),
                close_time_ms=int(raw_data["end"]),
                symbol=raw_data["s"],
                open=Decimal(str(raw_data["open"])),
                high=Decimal(str(raw_data["high"])),
                low=Decimal(str(raw_data["low"])),
                close=Decimal(str(raw_data["close"])),
                base_volume=Decimal(str(raw_data["volume"])),
                number_of_trades=0,
                quote_volume=Decimal(str(raw_data["turnover"])),
                taker_buy_base_volume=None,
                taker_buy_quote_volume=None,
            )
        except (KeyError, ValueError, TypeError) as e:
            print(f"[bybit] ERROR: Failed to normalize candle: {e}")
            return None

    def fetch_historical_klines(
        self, symbol: str, start_time_ms: int, end_time_ms: int
    ) -> List[NormalizedCandle]:
        """Fetch historical klines via Bybit REST API with retry logic."""
        max_retries = 4
        base_delay = 1

        for attempt in range(max_retries):
            try:
                response = requests.get(
                    f"{BYBIT_API_BASE}/v5/market/kline",
                    params={
                        "category": "linear",
                        "symbol": symbol,
                        "interval": "1",
                        "start": start_time_ms,
                        "end": end_time_ms,
                        "limit": 1,
                    },
                    timeout=10,
                )
                response.raise_for_status()
                data = response.json()

                ret_code = data.get("retCode")
                # Handle rate limit via retCode (10006 = rate limit)
                if ret_code == 10006:
                    if attempt < max_retries - 1:
                        wait_time = base_delay * (2**attempt)
                        print(
                            f"[bybit] Rate limit for {symbol}, waiting {wait_time}s (attempt {attempt + 1}/{max_retries})..."
                        )
                        time.sleep(wait_time)
                        continue
                    print(f"[bybit] ERROR: Rate limit exceeded for {symbol}")
                    return []

                if ret_code != 0:
                    print(
                        f"[bybit] ERROR: API error for {symbol}: {data.get('retMsg')}"
                    )
                    return []

                result = []
                for k in data.get("result", {}).get("list", []):
                    open_time_ms = int(k[0])
                    close_time_ms = open_time_ms + 60000 - 1

                    candle = NormalizedCandle(
                        open_time_ms=open_time_ms,
                        close_time_ms=close_time_ms,
                        symbol=symbol,
                        open=Decimal(str(k[1])),
                        high=Decimal(str(k[2])),
                        low=Decimal(str(k[3])),
                        close=Decimal(str(k[4])),
                        base_volume=Decimal(str(k[5])),
                        number_of_trades=0,
                        quote_volume=Decimal(str(k[6])),
                        taker_buy_base_volume=None,
                        taker_buy_quote_volume=None,
                    )
                    result.append(candle)

                return result

            except requests.exceptions.HTTPError as e:
                if e.response is not None and e.response.status_code in (429, 403):
                    if attempt < max_retries - 1:
                        wait_time = base_delay * (2**attempt)
                        print(
                            f"[bybit] Rate limit for {symbol}, waiting {wait_time}s (attempt {attempt + 1}/{max_retries})..."
                        )
                        time.sleep(wait_time)
                        continue
                print(
                    f"[bybit] ERROR: Failed to fetch historical klines for {symbol}: {e}"
                )
                return []
            except Exception as e:
                print(
                    f"[bybit] ERROR: Failed to fetch historical klines for {symbol}: {e}"
                )
                return []

        return []

    def _on_message(self, ws, message):
        """Handle incoming WebSocket message."""
        try:
            data = json.loads(message)

            if data.get("op") == "pong" or data.get("ret_msg") == "pong":
                self.last_pong_time = time.time()
                if self.heartbeat_count % 10 == 0:
                    elapsed = int(time.time() - self.connection_start_time)
                    print(f"[bybit] Pong received (uptime: {elapsed}s)")
                return

            if data.get("op") == "subscribe":
                if not data.get("success"):
                    print(f"[bybit] ERROR: Subscription failed: {data.get('ret_msg')}")
                return

            topic = data.get("topic", "")
            if topic.startswith("kline.1."):
                self._handle_kline(data)

        except Exception as e:
            print(f"[bybit] ERROR: Message error: {e}")

    def _handle_kline(self, data: dict):
        """Process incoming kline data."""
        try:
            topic = data.get("topic", "")
            parts = topic.split(".")
            if len(parts) != 3:
                return
            symbol = parts[2]

            kline_list = data.get("data", [])
            if not kline_list:
                return

            for kline_data in kline_list:
                if not kline_data.get("confirm", False):
                    continue

                kline_data["s"] = symbol
                candle = self.normalize_candle(kline_data)
                if candle:
                    self.save_kline(candle)
                    print(
                        f"[bybit] Stored kline: {candle.symbol} at {candle.open_time_ms}"
                    )

        except Exception as e:
            print(f"[bybit] ERROR: Error handling kline: {e}")

    def _on_error(self, ws, error):
        """Handle WebSocket errors."""
        print(f"[bybit] ERROR: WebSocket error: {error}")

    def _on_close(self, ws, code, msg):
        """Handle WebSocket close."""
        self.ws_connected = False
        duration = (
            int(time.time() - self.connection_start_time)
            if self.connection_start_time
            else 0
        )
        print(f"[bybit] WebSocket closed: code={code}, msg={msg}, duration={duration}s")

    def _on_open(self, ws):
        """Handle WebSocket open."""
        self.ws_connected = True
        self.connection_start_time = time.time()
        self.last_pong_time = time.time()
        self.heartbeat_count = 0
        print("[bybit] WebSocket connected")

        threading.Thread(target=self._setup, daemon=True).start()
        threading.Thread(target=self._heartbeat_loop, daemon=True).start()

    def _setup(self):
        """Subscribe to symbols."""
        print(f"[bybit] Subscribing to {len(self.symbols)} symbols...")

        symbol_list = list(self.symbols)
        for i in range(0, len(symbol_list), MAX_ARGS_PER_SUBSCRIBE):
            if not self.ws_connected or not self.ws:
                break

            batch = symbol_list[i : i + MAX_ARGS_PER_SUBSCRIBE]
            args = [f"kline.1.{symbol}" for symbol in batch]

            try:
                self.ws.send(json.dumps({"op": "subscribe", "args": args}))
                time.sleep(SUBSCRIBE_DELAY)
            except Exception as e:
                print(f"[bybit] ERROR: Subscribe batch failed: {e}")

        print(f"[bybit] Subscribed to {len(self.symbols)} symbols")

    def _heartbeat_loop(self):
        """Send JSON ping every 20s and monitor pong responses."""
        while self.ws_connected:
            time.sleep(HEARTBEAT_INTERVAL)

            if not self.ws_connected or not self.ws:
                break

            time_since_pong = time.time() - self.last_pong_time
            if time_since_pong > PONG_TIMEOUT_SECONDS:
                print(f"[bybit] ERROR: No pong in {time_since_pong:.0f}s, closing")
                self.ws.close()
                break

            try:
                self.ws.send(json.dumps({"op": "ping"}))
                self.heartbeat_count += 1

                if self.heartbeat_count % 20 == 0:
                    elapsed = int(time.time() - self.connection_start_time)
                    print(
                        f"[bybit] Heartbeat #{self.heartbeat_count} (uptime: {elapsed}s)"
                    )
            except Exception as e:
                print(f"[bybit] ERROR: Heartbeat failed: {e}")

    def connect_websocket(self) -> Optional[threading.Thread]:
        """Create and connect WebSocket."""
        if not self.symbols:
            print("[bybit] No symbols to connect to")
            return None

        print("[bybit] Connecting to WebSocket...")
        self.ws = websocket.WebSocketApp(
            BYBIT_WS_URL,
            on_open=self._on_open,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close,
        )

        ws = self.ws
        thread = threading.Thread(
            target=lambda: ws.run_forever(
                ping_interval=WS_PING_INTERVAL,
                ping_timeout=WS_PING_TIMEOUT,
            ),
            daemon=True,
        )
        thread.start()
        return thread

    def close_websocket(self):
        """Close the WebSocket connection."""
        if self.ws:
            self.ws.close()
            self.ws = None


def main():
    """Entry point for the kline collector."""
    collector = BybitKlineCollector()
    try:
        collector.run()
    except KeyboardInterrupt:
        print("Shutting down...")
        collector.stop()


if __name__ == "__main__":
    main()
