import json
import time
import threading
import requests
import websocket
from decimal import Decimal
from typing import Set, Optional

from exchange_connections.base import BaseKlineCollector
from exchange_connections.candle_types import NormalizedCandle

HYPERLIQUID_WS_URL = "wss://api.hyperliquid.xyz/ws"
HYPERLIQUID_INFO_URL = "https://api.hyperliquid.xyz/info"
WS_PING_INTERVAL = 30
WS_PING_TIMEOUT = 10


class HyperliquidKlineCollector(BaseKlineCollector):
    """
    Collects 1-minute kline data for all Hyperliquid perpetual futures symbols.

    Key differences from Binance:
    - Uses per-symbol subscriptions (not combined stream URL)
    - Symbol naming: "BTC" instead of "BTCUSDT"
    - Missing quote_volume and taker volume fields
    """

    def __init__(self):
        super().__init__(exchange="hyperliquid", contract_type="perpetual")
        self.ws: Optional[websocket.WebSocketApp] = None
        self.ws_connected = False
        self.subscribed_symbols: Set[str] = set()
        self.generate_synthetic_candles = True

    def fetch_perpetual_symbols(self) -> Set[str]:
        """Fetch all active perpetual symbols from Hyperliquid API.

        Filters out delisted symbols which have isDelisted=True in the meta response.
        These symbols have no trading activity and return empty candle data.
        """
        try:
            response = requests.post(
                HYPERLIQUID_INFO_URL,
                headers={"Content-Type": "application/json"},
                json={"type": "meta"},
                timeout=30,
            )
            response.raise_for_status()
            data = response.json()

            symbols = set()
            delisted_count = 0
            universe = data.get("universe", [])
            for asset in universe:
                name = asset.get("name")
                if not name:
                    continue
                # Skip delisted symbols - they have no trading activity
                if asset.get("isDelisted"):
                    delisted_count += 1
                    continue
                symbols.add(name)

            print(f"[hyperliquid] Fetched {len(symbols)} active perpetual symbols (skipped {delisted_count} delisted)")
            return symbols

        except Exception as e:
            self.log_error(f"Failed to fetch symbols: {e}")
            return set()

    def normalize_candle(self, raw_data: dict) -> Optional[NormalizedCandle]:
        """Convert Hyperliquid candle format to NormalizedCandle."""
        try:
            return NormalizedCandle(
                open_time_ms=int(raw_data["t"]),
                close_time_ms=int(raw_data["T"]),
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
            self.log_error(f"Failed to normalize candle: {e}, data: {raw_data}")
            return None

    def map_coingecko_symbol(self, coingecko_symbol: str) -> Optional[str]:
        """Map CoinGecko symbol to Hyperliquid format (same format)."""
        return coingecko_symbol

    def _subscribe_to_symbol(self, symbol: str):
        """Subscribe to candle updates for a single symbol."""
        if not self.ws or not self.ws_connected:
            return

        try:
            sub_msg = {
                "method": "subscribe",
                "subscription": {
                    "type": "candle",
                    "coin": symbol,
                    "interval": "1m",
                },
            }
            self.ws.send(json.dumps(sub_msg))
            self.subscribed_symbols.add(symbol)
        except Exception as e:
            self.log_error(f"Failed to subscribe to {symbol}: {e}")

    def _subscribe_all_symbols(self):
        """Subscribe to all tracked symbols."""
        print(f"[hyperliquid] Subscribing to {len(self.symbols)} symbols...")
        for symbol in self.symbols:
            self._subscribe_to_symbol(symbol)
            time.sleep(0.05)
        print(f"[hyperliquid] Subscribed to {len(self.subscribed_symbols)} symbols")

    def fetch_initial_prices(self):
        """Fetch latest candle for each symbol to initialize last_prices.

        Uses 1-hour lookback to handle low-volume symbols with sporadic trading.
        """
        print(f"[hyperliquid] Fetching initial prices for {len(self.symbols)} symbols...")

        current_time_ms = int(time.time() * 1000)
        # 1 hour lookback to capture low-volume symbols with sporadic trading
        start_time_ms = current_time_ms - (60 * 60 * 1000)

        fetched_count = 0
        for symbol in self.symbols:
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
                            "endTime": current_time_ms,
                        },
                    },
                    timeout=10,
                )
                response.raise_for_status()
                candles = response.json()

                if candles:
                    # Get the most recent candle (last in list)
                    latest = candles[-1]
                    close_price = Decimal(str(latest["c"]))
                    self.last_prices[symbol] = close_price
                    fetched_count += 1

                # Small delay to avoid rate limiting
                time.sleep(0.05)

            except Exception as e:
                self.log_error(f"Failed to fetch initial price for {symbol}: {e}")

        print(
            f"[hyperliquid] Initialized prices for {fetched_count}/{len(self.symbols)} symbols"
        )

    def _on_message(self, _ws, message):
        """Handle incoming WebSocket message."""
        try:
            data = json.loads(message)

            if data.get("channel") == "subscriptionResponse":
                return

            if data.get("channel") == "candle":
                candle_data = data.get("data")
                if candle_data:
                    self._process_candle_update(candle_data)

        except json.JSONDecodeError:
            pass
        except Exception as e:
            self.log_error(f"Error handling message: {e}")

    def _process_candle_update(self, candle_data: dict):
        """Process a candle update, only process closed candles."""
        try:
            close_time_ms = int(candle_data.get("T", 0))
            current_time_ms = int(time.time() * 1000)

            if current_time_ms >= close_time_ms:
                candle = self.normalize_candle(candle_data)
                if candle:
                    self.process_kline(candle)

        except Exception as e:
            self.log_error(f"Error processing candle update: {e}")

    def _on_error(self, _ws, error):
        """Handle WebSocket errors."""
        self.log_error(f"WebSocket error: {error}")

    def _on_close(self, _ws, close_status_code, close_msg):
        """Handle WebSocket close."""
        self.ws_connected = False
        self.subscribed_symbols.clear()
        print(
            f"[hyperliquid] WebSocket closed: code={close_status_code}, msg={close_msg}"
        )

    def _on_open(self, _ws):
        """Handle WebSocket open."""
        self.ws_connected = True
        print("[hyperliquid] WebSocket connected")

        def setup():
            self._subscribe_all_symbols()
            self.fetch_initial_prices()

        threading.Thread(target=setup, daemon=True).start()

    def connect_websocket(self) -> Optional[threading.Thread]:
        """Create and connect WebSocket."""
        if not self.symbols:
            print("[hyperliquid] No symbols to connect to")
            return None

        print(f"[hyperliquid] Connecting to WebSocket...")

        self.ws = websocket.WebSocketApp(
            HYPERLIQUID_WS_URL,
            on_open=self._on_open,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close,
        )

        ws_thread = threading.Thread(
            target=lambda: self.ws.run_forever(
                ping_interval=WS_PING_INTERVAL,
                ping_timeout=WS_PING_TIMEOUT,
            ),
            daemon=True,
        )
        ws_thread.start()

        return ws_thread

    def on_symbols_changed(self):
        """Called when symbols change - close WebSocket to reconnect with new symbols."""
        if self.ws:
            self.ws.close()


def main():
    """Entry point for the Hyperliquid kline collector."""
    collector = HyperliquidKlineCollector()
    try:
        collector.run()
    except KeyboardInterrupt:
        print("Shutting down...")
        collector.stop()


if __name__ == "__main__":
    main()
