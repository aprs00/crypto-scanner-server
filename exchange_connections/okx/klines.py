"""
OKX Perpetual Futures Kline Collector

Features:
- Auto-reconnect in-process (never exits)
- Heartbeat via plain string "ping" (25s interval, OKX disconnects after 30s)
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

# OKX V5 API endpoints
# Candle channels live on the "business" WebSocket endpoint, not "public".
OKX_WS_URL = "wss://ws.okx.com:8443/ws/v5/business"
OKX_API_BASE = "https://www.okx.com"
OKX_INSTRUMENTS_URL = f"{OKX_API_BASE}/api/v5/public/instruments"
OKX_HISTORY_CANDLES_URL = f"{OKX_API_BASE}/api/v5/market/history-candles"
OKX_MAX_KLINES_PER_REQUEST = 100

# WebSocket configuration - disable websocket-client's ping, we use plain string ping
WS_PING_INTERVAL = 0
WS_PING_TIMEOUT = None

# Heartbeat configuration (OKX disconnects after 30s without ping)
HEARTBEAT_INTERVAL = 25
PONG_TIMEOUT_SECONDS = 60

# Subscription limits (OKX allows up to 128 args per subscribe; 100 is safe)
MAX_ARGS_PER_SUBSCRIBE = 100
SUBSCRIBE_DELAY = 0.05


class OkxKlineCollector(BaseKlineCollector):
    """
    Collects 1-minute klines from OKX linear perpetual swaps.

    Uses OKX V5 WebSocket API with:
    - Plain string "ping"/"pong" heartbeat (25s interval)
    - Only processes confirmed (closed) candles (data[i][8] == "1")
    """

    def __init__(self):
        super().__init__(exchange=Exchange.OKX, contract_type="perpetual")
        self.ws: Optional[websocket.WebSocketApp] = None
        self.ws_connected = False

        # Heartbeat tracking
        self.connection_start_time = 0
        self.heartbeat_count = 0
        self.last_pong_time = 0.0

    def get_backfill_chunk_minutes(self) -> int:
        return OKX_MAX_KLINES_PER_REQUEST

    def fetch_perpetual_symbols(self) -> Set[str]:
        """Fetch all USDT linear SWAP symbols from OKX API."""
        try:
            response = requests.get(
                OKX_INSTRUMENTS_URL,
                params={"instType": "SWAP"},
                timeout=30,
            )
            response.raise_for_status()
            data = response.json()

            if data.get("code") != "0":
                print(f"[okx] ERROR: API error: {data.get('msg')}")
                return set()

            symbols = set()
            for instrument in data.get("data", []):
                if (
                    instrument.get("state") == "live"
                    and instrument.get("ctType") == "linear"
                    and instrument.get("settleCcy") == "USDT"
                ):
                    symbols.add(instrument["instId"])

            print(f"[okx] Fetched {len(symbols)} linear perpetual symbols")
            return symbols

        except Exception as e:
            print(f"[okx] ERROR: Failed to fetch symbols: {e}")
            return set()

    def normalize_candle(self, raw_data: dict) -> Optional[NormalizedCandle]:
        """Convert OKX kline array to NormalizedCandle.

        Expected `raw_data` shape: {"instId": symbol, "data": arr} where
        arr = [ts_ms, o, h, l, c, vol, volCcy, volCcyQuote, confirm]
        """
        try:
            arr = raw_data["data"]
            open_time_ms = int(arr[0])
            return NormalizedCandle(
                open_time_ms=open_time_ms,
                close_time_ms=open_time_ms + 59999,
                symbol=raw_data["instId"],
                open=Decimal(str(arr[1])),
                high=Decimal(str(arr[2])),
                low=Decimal(str(arr[3])),
                close=Decimal(str(arr[4])),
                base_volume=Decimal(str(arr[5])),
                number_of_trades=0,
                quote_volume=Decimal(str(arr[7])),
                taker_buy_base_volume=None,
                taker_buy_quote_volume=None,
            )
        except (KeyError, IndexError, ValueError, TypeError) as e:
            print(f"[okx] ERROR: Failed to normalize candle: {e}")
            return None

    def fetch_historical_klines(
        self, symbol: str, start_time_ms: int, end_time_ms: int
    ) -> List[NormalizedCandle]:
        """Fetch historical klines via OKX REST API with retry logic.

        OKX `after` is exclusive — it returns candles strictly OLDER than the
        provided timestamp. To include the candle at `end_time_ms - 60000` we
        pass `end_time_ms` itself.
        """
        max_retries = 4
        base_delay = 1
        requested_minutes = max(1, (end_time_ms - start_time_ms) // 60000)
        limit = min(requested_minutes, OKX_MAX_KLINES_PER_REQUEST)

        for attempt in range(max_retries):
            try:
                response = requests.get(
                    OKX_HISTORY_CANDLES_URL,
                    params={
                        "instId": symbol,
                        "bar": "1m",
                        "after": end_time_ms,
                        "limit": limit,
                    },
                    timeout=10,
                )

                if response.status_code in (429, 503):
                    if attempt < max_retries - 1:
                        wait_time = base_delay * (2**attempt)
                        print(
                            f"[okx] Rate limit for {symbol}, waiting {wait_time}s "
                            f"(attempt {attempt + 1}/{max_retries})..."
                        )
                        time.sleep(wait_time)
                        continue
                    print(f"[okx] ERROR: Rate limit exceeded for {symbol}")
                    return []

                response.raise_for_status()
                data = response.json()

                if data.get("code") != "0":
                    print(f"[okx] ERROR: API error for {symbol}: {data.get('msg')}")
                    return []

                result = []
                for item in data.get("data", []):
                    open_time_ms = int(item[0])
                    if open_time_ms < start_time_ms or open_time_ms >= end_time_ms:
                        continue

                    candle = NormalizedCandle(
                        open_time_ms=open_time_ms,
                        close_time_ms=open_time_ms + 59999,
                        symbol=symbol,
                        open=Decimal(str(item[1])),
                        high=Decimal(str(item[2])),
                        low=Decimal(str(item[3])),
                        close=Decimal(str(item[4])),
                        base_volume=Decimal(str(item[5])),
                        number_of_trades=0,
                        quote_volume=Decimal(str(item[7])),
                        taker_buy_base_volume=None,
                        taker_buy_quote_volume=None,
                    )
                    result.append(candle)

                result.sort(key=lambda c: c.open_time_ms)
                return result

            except requests.exceptions.HTTPError as e:
                if e.response is not None and e.response.status_code in (429, 503):
                    if attempt < max_retries - 1:
                        wait_time = base_delay * (2**attempt)
                        print(
                            f"[okx] Rate limit for {symbol}, waiting {wait_time}s "
                            f"(attempt {attempt + 1}/{max_retries})..."
                        )
                        time.sleep(wait_time)
                        continue
                print(
                    f"[okx] ERROR: Failed to fetch historical klines for {symbol}: {e}"
                )
                return []
            except Exception as e:
                print(
                    f"[okx] ERROR: Failed to fetch historical klines for {symbol}: {e}"
                )
                return []

        return []

    def _on_message(self, ws, message):
        """Handle incoming WebSocket message."""
        try:
            if message == "pong":
                self.last_pong_time = time.time()
                if self.heartbeat_count % 10 == 0:
                    elapsed = int(time.time() - self.connection_start_time)
                    print(f"[okx] Pong received (uptime: {elapsed}s)")
                return

            data = json.loads(message)

            if data.get("event") == "subscribe":
                return
            if data.get("event") == "error":
                print(
                    f"[okx] ERROR: Subscription error: "
                    f"code={data.get('code')} msg={data.get('msg')}"
                )
                return

            arg = data.get("arg") or {}
            if arg.get("channel") == "candle1m":
                self._handle_kline(data)

        except Exception as e:
            print(f"[okx] ERROR: Message error: {e}")

    def _handle_kline(self, data: dict):
        """Process incoming kline data — only save confirmed candles."""
        try:
            symbol = data.get("arg", {}).get("instId")
            if not symbol:
                return

            for arr in data.get("data", []):
                if len(arr) < 9 or arr[8] != "1":
                    continue

                raw_data = {"instId": symbol, "data": arr}
                candle = self.normalize_candle(raw_data)
                if candle:
                    self.save_kline(candle, source="live")

        except Exception as e:
            print(f"[okx] ERROR: Error handling kline: {e}")

    def _on_error(self, ws, error):
        """Handle WebSocket errors."""
        print(f"[okx] ERROR: WebSocket error: {error}")

    def _on_close(self, ws, code, msg):
        """Handle WebSocket close."""
        self.ws_connected = False
        duration = (
            int(time.time() - self.connection_start_time)
            if self.connection_start_time
            else 0
        )
        print(f"[okx] WebSocket closed: code={code}, msg={msg}, duration={duration}s")

    def _on_open(self, ws):
        """Handle WebSocket open."""
        self.ws_connected = True
        self.connection_start_time = time.time()
        self.last_pong_time = time.time()
        self.heartbeat_count = 0
        print("[okx] WebSocket connected")

        threading.Thread(target=self._setup, daemon=True).start()
        threading.Thread(target=self._heartbeat_loop, daemon=True).start()

    def _setup(self):
        """Subscribe to symbols in batches."""
        print(f"[okx] Subscribing to {len(self.symbols)} symbols...")

        symbol_list = list(self.symbols)
        for i in range(0, len(symbol_list), MAX_ARGS_PER_SUBSCRIBE):
            if not self.ws_connected or not self.ws:
                break

            batch = symbol_list[i : i + MAX_ARGS_PER_SUBSCRIBE]
            args = [{"channel": "candle1m", "instId": symbol} for symbol in batch]

            try:
                self.ws.send(json.dumps({"op": "subscribe", "args": args}))
                time.sleep(SUBSCRIBE_DELAY)
            except Exception as e:
                print(f"[okx] ERROR: Subscribe batch failed: {e}")

        print(f"[okx] Subscribed to {len(self.symbols)} symbols")

    def _heartbeat_loop(self):
        """Send plain string 'ping' every 25s to keep connection alive."""
        while self.ws_connected:
            time.sleep(HEARTBEAT_INTERVAL)

            if not self.ws_connected or not self.ws:
                break

            if (
                self.last_pong_time
                and time.time() - self.last_pong_time > PONG_TIMEOUT_SECONDS
            ):
                print(
                    f"[okx] ERROR: No pong for {PONG_TIMEOUT_SECONDS}s, "
                    f"closing WebSocket"
                )
                try:
                    self.ws.close()
                except Exception:
                    pass
                break

            try:
                self.ws.send("ping")
                self.heartbeat_count += 1

                if self.heartbeat_count % 20 == 0:
                    elapsed = int(time.time() - self.connection_start_time)
                    print(
                        f"[okx] Heartbeat #{self.heartbeat_count} (uptime: {elapsed}s)"
                    )
            except Exception as e:
                print(f"[okx] ERROR: Heartbeat failed: {e}")

    def connect_websocket(self) -> Optional[threading.Thread]:
        """Create and connect WebSocket."""
        if not self.symbols:
            print("[okx] No symbols to connect to")
            return None

        print("[okx] Connecting to WebSocket...")
        self.ws = websocket.WebSocketApp(
            OKX_WS_URL,
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
    collector = OkxKlineCollector()
    try:
        collector.run()
    except KeyboardInterrupt:
        print("Shutting down...")
        collector.stop()


if __name__ == "__main__":
    main()
