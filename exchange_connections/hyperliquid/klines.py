"""
Hyperliquid Perpetual Futures Kline Collector

Features:
- Auto-reconnect in-process (never exits)
- Robust heartbeat monitoring (50s ping, 90s pong timeout)
- On reconnect: detect gaps (via BTC) and backfill all symbols
- Accumulates candle updates and saves when minute changes
- Generates synthetic candles for symbols with no trades
"""

import json
import time
import threading
import requests
import websocket
from decimal import Decimal
from typing import Dict, List, Set, Optional

from exchange_connections.base import BaseKlineCollector
from exchange_connections.candle_types import NormalizedCandle
from core.constants import Exchange

HYPERLIQUID_WS_URL = "wss://api.hyperliquid.xyz/ws"
HYPERLIQUID_INFO_URL = "https://api.hyperliquid.xyz/info"
WS_PING_INTERVAL = 0
WS_PING_TIMEOUT = None
HEARTBEAT_INTERVAL = 50
PONG_TIMEOUT_SECONDS = 90


class HyperliquidKlineCollector(BaseKlineCollector):
    """
    Collects 1-minute klines from Hyperliquid perpetual futures.

    Unlike Binance, Hyperliquid streams updates on every trade.
    We accumulate updates and save when the minute changes.
    Synthetic candles are generated for symbols with no trades.
    """

    def __init__(self):
        super().__init__(exchange=Exchange.HYPERLIQUID, contract_type="perpetual")
        self.ws: Optional[websocket.WebSocketApp] = None
        self.ws_connected = False
        self.connection_start_time = 0
        self.last_pong_time = 0
        self.heartbeat_count = 0
        self.backfill_rate_limit = 0.1

        # Pending candle accumulation
        self.pending_candles: Dict[str, dict] = {}
        self.current_minute: int = 0

        # Price tracking for synthetic candles
        self.last_prices: Dict[str, Decimal] = {}

    def fetch_perpetual_symbols(self) -> Set[str]:
        """Fetch all perpetual symbols from Hyperliquid API."""
        try:
            response = requests.post(
                HYPERLIQUID_INFO_URL,
                headers={"Content-Type": "application/json"},
                json={"type": "meta"},
                timeout=30,
            )
            response.raise_for_status()
            symbols = {
                asset["name"]
                for asset in response.json().get("universe", [])
                if asset.get("name") and not asset.get("isDelisted")
            }
            print(f"[hyperliquid] Fetched {len(symbols)} perpetual symbols")
            return symbols
        except Exception as e:
            print(f"[hyperliquid] ERROR: Failed to fetch symbols: {e}")
            return set()

    def _normalize_minute_ts_ms(self, raw_ts: int | str) -> int:
        """Normalize timestamps to minute-aligned milliseconds."""
        ts = int(raw_ts)
        if ts < 1_000_000_000_000:
            ts *= 1000
        return (ts // 60000) * 60000

    def normalize_candle(self, raw_data: dict) -> Optional[NormalizedCandle]:
        """Convert Hyperliquid kline format to NormalizedCandle."""
        try:
            open_time_ms = self._normalize_minute_ts_ms(raw_data["t"])
            close_time_ms = open_time_ms + 60000 - 1
            return NormalizedCandle(
                open_time_ms=open_time_ms,
                close_time_ms=close_time_ms,
                symbol=raw_data["s"],
                open=Decimal(str(raw_data["o"])),
                high=Decimal(str(raw_data["h"])),
                low=Decimal(str(raw_data["l"])),
                close=Decimal(str(raw_data["c"])),
                base_volume=Decimal(str(raw_data["v"])),
                number_of_trades=int(raw_data["n"]),
                quote_volume=None,
                taker_buy_base_volume=None,
                taker_buy_quote_volume=None,
            )
        except (KeyError, ValueError, TypeError) as e:
            print(f"[hyperliquid] ERROR: Failed to normalize candle: {e}")
            return None

    def fetch_historical_klines(
        self, symbol: str, start_time_ms: int, end_time_ms: int
    ) -> List[NormalizedCandle]:
        """Fetch historical klines via Hyperliquid REST API with retry logic."""
        max_retries = 4
        base_delay = 1

        for attempt in range(max_retries):
            try:
                response = requests.post(
                    HYPERLIQUID_INFO_URL,
                    headers={"Content-Type": "application/json"},
                    json={
                        "type": "candleSnapshot",
                        "req": {
                            "coin": symbol,
                            "interval": "1m",
                            "startTime": start_time_ms,
                            "endTime": end_time_ms,
                        },
                    },
                    timeout=10,
                )
                response.raise_for_status()
                candles = response.json()

                if not candles:
                    return []

                result = []
                for raw_candle in candles:
                    candle = self.normalize_candle(raw_candle)
                    if candle:
                        result.append(candle)

                return result

            except requests.exceptions.HTTPError as e:
                if e.response is not None and e.response.status_code == 429:
                    if attempt < max_retries - 1:
                        wait_time = base_delay * (2**attempt)
                        print(
                            f"[hyperliquid] Rate limit for {symbol}, waiting {wait_time}s (attempt {attempt + 1}/{max_retries})..."
                        )
                        time.sleep(wait_time)
                        continue
                print(f"[hyperliquid] ERROR: Failed to fetch klines for {symbol}: {e}")
                return []
            except Exception as e:
                print(f"[hyperliquid] ERROR: Failed to fetch klines for {symbol}: {e}")
                return []

        return []

    def _fetch_initial_prices(self):
        """Fetch initial prices for all symbols using allMids endpoint (single request)."""
        print("[hyperliquid] Fetching initial prices...")
        try:
            response = requests.post(
                HYPERLIQUID_INFO_URL,
                headers={"Content-Type": "application/json"},
                json={"type": "allMids"},
                timeout=10,
            )
            response.raise_for_status()
            mids = response.json()  # Dict mapping symbol -> mid price string

            count = 0
            for symbol in self.symbols:
                if symbol in mids:
                    self.last_prices[symbol] = Decimal(mids[symbol])
                    count += 1

            print(f"[hyperliquid] Initialized {count}/{len(self.symbols)} prices")
        except Exception as e:
            print(f"[hyperliquid] ERROR: Failed to fetch initial prices: {e}")

    def _create_synthetic_candle(
        self, symbol: str, timestamp_ms: int, price: Decimal
    ) -> NormalizedCandle:
        """Create a synthetic (flat) candle for a symbol with no trades."""
        return NormalizedCandle(
            open_time_ms=timestamp_ms,
            close_time_ms=timestamp_ms + 60000 - 1,
            symbol=symbol,
            open=price,
            high=price,
            low=price,
            close=price,
            base_volume=Decimal("0"),
            number_of_trades=0,
            quote_volume=None,
            taker_buy_base_volume=None,
            taker_buy_quote_volume=None,
        )

    def _flush_pending_candles(self):
        """Save all pending candles and generate synthetics for missing symbols."""
        if self.current_minute == 0:
            return

        real_count = 0
        synthetic_count = 0

        # Save real candles and update last_prices
        for symbol, raw in self.pending_candles.items():
            candle = self.normalize_candle(raw)
            if candle:
                self.save_kline(candle)
                self.last_prices[symbol] = candle.close
                real_count += 1

        # Generate synthetic candles for symbols not in pending_candles
        missing_symbols = self.symbols - set(self.pending_candles.keys())
        for symbol in missing_symbols:
            if symbol in self.last_prices:
                candle = self._create_synthetic_candle(
                    symbol, self.current_minute, self.last_prices[symbol]
                )
                self.save_kline(candle)
                synthetic_count += 1

        print(
            f"[hyperliquid] Saved {real_count} real + {synthetic_count} synthetic "
            f"candles for minute {self.current_minute}"
        )
        self.pending_candles.clear()

    def _on_message(self, ws, message):
        """Handle incoming WebSocket message."""
        try:
            data = json.loads(message)
            if data.get("channel") == "pong":
                self.last_pong_time = time.time()
                if self.heartbeat_count % 10 == 0:
                    elapsed = int(time.time() - self.connection_start_time)
                    print(f"[hyperliquid] Pong received (uptime: {elapsed}s)")
            elif data.get("channel") == "candle" and data.get("data"):
                self._handle_candle(data["data"])
        except Exception as e:
            print(f"[hyperliquid] ERROR: Message error: {e}")

    def _handle_candle(self, candle_data: dict):
        """Accumulate candle updates, save to DB when minute changes."""
        symbol = candle_data.get("s")
        if not symbol:
            return

        raw_t = candle_data.get("t")
        if raw_t is None:
            return

        candle_minute = self._normalize_minute_ts_ms(raw_t)

        if self.current_minute == 0:
            self.current_minute = candle_minute

        if candle_minute > self.current_minute:
            self._flush_pending_candles()
            self.current_minute = candle_minute

        if candle_minute < self.current_minute:
            return

        candle_data["t"] = candle_minute
        self.pending_candles[symbol] = candle_data

    def _on_error(self, ws, error):
        """Handle WebSocket errors."""
        print(f"[hyperliquid] ERROR: WebSocket error: {error}")

    def _on_close(self, ws, code, msg):
        """Handle WebSocket close."""
        self._flush_pending_candles()

        self.ws_connected = False
        duration = (
            int(time.time() - self.connection_start_time)
            if self.connection_start_time
            else 0
        )
        print(
            f"[hyperliquid] WebSocket closed: code={code}, msg={msg}, duration={duration}s"
        )

    def _on_open(self, ws):
        """Handle WebSocket open."""
        self.ws_connected = True
        self.connection_start_time = time.time()
        self.last_pong_time = time.time()
        self.heartbeat_count = 0
        self.current_minute = 0
        self.pending_candles.clear()
        print("[hyperliquid] WebSocket connected")

        threading.Thread(target=self._setup, daemon=True).start()
        threading.Thread(target=self._heartbeat_loop, daemon=True).start()

    def _setup(self):
        """Subscribe to symbols."""
        print(f"[hyperliquid] Subscribing to {len(self.symbols)} symbols...")

        for symbol in self.symbols:
            if not self.ws_connected or not self.ws:
                break
            try:
                self.ws.send(
                    json.dumps(
                        {
                            "method": "subscribe",
                            "subscription": {
                                "type": "candle",
                                "coin": symbol,
                                "interval": "1m",
                            },
                        }
                    )
                )
                time.sleep(0.05)
            except Exception as e:
                print(f"[hyperliquid] ERROR: Subscribe failed for {symbol}: {e}")

        print(f"[hyperliquid] Subscribed to {len(self.symbols)} symbols")

    def _heartbeat_loop(self):
        """Send JSON heartbeat every 50s to prevent 60s server timeout."""
        while self.ws_connected:
            time.sleep(HEARTBEAT_INTERVAL)

            if not self.ws_connected or not self.ws:
                break

            time_since_pong = time.time() - self.last_pong_time
            if time_since_pong > PONG_TIMEOUT_SECONDS:
                print(
                    f"[hyperliquid] ERROR: No pong in {time_since_pong:.0f}s, closing"
                )
                self.ws.close()
                break

            try:
                self.ws.send(json.dumps({"method": "ping"}))
                self.heartbeat_count += 1

                if self.heartbeat_count % 20 == 0:
                    elapsed = int(time.time() - self.connection_start_time)
                    print(
                        f"[hyperliquid] Heartbeat #{self.heartbeat_count} (uptime: {elapsed}s)"
                    )
            except Exception as e:
                print(f"[hyperliquid] ERROR: Heartbeat failed: {e}")

    def connect_websocket(self) -> Optional[threading.Thread]:
        """Create and connect WebSocket."""
        if not self.symbols:
            print("[hyperliquid] No symbols to connect to")
            return None

        print("[hyperliquid] Connecting to WebSocket...")
        self.ws = websocket.WebSocketApp(
            HYPERLIQUID_WS_URL,
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

    def run(self):
        """Main loop - fetch initial prices before starting WebSocket."""
        print(f"Starting {self.exchange.title()} Kline Collector...")

        # Fetch initial prices once at startup (before first WebSocket connection)
        self.update_symbols()
        if self.symbols and not self.last_prices:
            self._fetch_initial_prices()

        # Continue with normal run loop
        super().run()


def main():
    """Entry point for the kline collector."""
    collector = HyperliquidKlineCollector()
    try:
        collector.run()
    except KeyboardInterrupt:
        print("Shutting down...")
        collector.stop()


if __name__ == "__main__":
    main()
