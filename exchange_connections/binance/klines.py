import json
import time
import threading
import requests
import websocket
from typing import Set, Optional
from decimal import Decimal

from exchange_connections.base import BaseKlineCollector
from exchange_connections.candle_types import NormalizedCandle
from exchange_connections.constants import BinanceContractStatus


BINANCE_FUTURES_EXCHANGE_INFO_URL = "https://fapi.binance.com/fapi/v1/exchangeInfo"
BINANCE_FUTURES_WS_URL = "wss://fstream.binance.com/stream"

WS_PING_INTERVAL = 60
WS_PING_TIMEOUT = 30
MAX_STREAMS_PER_CONNECTION = 1024


class BinanceKlineCollector(BaseKlineCollector):
    """
    Collects 1-minute kline data for all Binance perpetual futures symbols.

    Handles:
    - Symbol discovery and change detection
    - WebSocket connection with automatic reconnection (24h limit)
    - Kline batching by timestamp for complete minute collection
    - Bulk database inserts
    - Redis pubsub for correlation updates
    - Market cap ranking from CoinGecko (via base class)
    """

    def __init__(self):
        super().__init__(exchange="binance", contract_type="perpetual")
        self.ws: Optional[websocket.WebSocketApp] = None
        self.ws_connected = False
        self.connection_start_time: Optional[float] = None

    def fetch_perpetual_symbols(self) -> Set[str]:
        """Fetch all perpetual futures symbols from Binance API."""
        try:
            response = requests.get(BINANCE_FUTURES_EXCHANGE_INFO_URL, timeout=30)
            response.raise_for_status()
            data = response.json()

            symbols = set()
            for symbol_info in data.get("symbols", []):
                if (
                    symbol_info.get("contractType") == "PERPETUAL"
                    and symbol_info.get("quoteAsset") == "USDT"
                    and symbol_info.get("status") == BinanceContractStatus.TRADING.value
                ):
                    symbols.add(symbol_info["symbol"])

            print(f"[binance] Fetched {len(symbols)} perpetual futures symbols")
            return symbols
        except Exception as e:
            self.log_error(f"Failed to fetch symbols: {e}")
            return set()

    def normalize_candle(self, raw_data: dict) -> Optional[NormalizedCandle]:
        """Convert Binance kline format to NormalizedCandle."""
        try:
            return NormalizedCandle(
                open_time_ms=int(raw_data["t"]),
                close_time_ms=int(raw_data["T"]),
                symbol=raw_data["s"],
                open=Decimal(raw_data["o"]),
                high=Decimal(raw_data["h"]),
                low=Decimal(raw_data["l"]),
                close=Decimal(raw_data["c"]),
                base_volume=Decimal(raw_data["v"]),
                number_of_trades=int(raw_data["n"]),
                quote_volume=Decimal(raw_data["q"]),
                taker_buy_base_volume=Decimal(raw_data["V"]),
                taker_buy_quote_volume=Decimal(raw_data["Q"]),
            )
        except (KeyError, ValueError, TypeError) as e:
            self.log_error(f"Failed to normalize candle: {e}, data: {raw_data}")
            return None

    def map_coingecko_symbol(self, coingecko_symbol: str) -> Optional[str]:
        """Map CoinGecko symbol to Binance format (add USDT suffix)."""
        return f"{coingecko_symbol}USDT"

    def build_ws_url(self) -> str:
        """Build WebSocket URL with all kline streams."""
        streams = [f"{symbol.lower()}@kline_1m" for symbol in self.symbols]

        if len(streams) > MAX_STREAMS_PER_CONNECTION:
            self.log_error(
                f"Warning: {len(streams)} streams exceed limit of {MAX_STREAMS_PER_CONNECTION}"
            )
            streams = streams[:MAX_STREAMS_PER_CONNECTION]

        streams_param = "/".join(streams)
        return f"{BINANCE_FUTURES_WS_URL}?streams={streams_param}"

    def on_message(self, _ws, message):
        """Handle incoming WebSocket message."""
        try:
            data = json.loads(message)

            if "stream" in data and "data" in data:
                stream = data["stream"]
                if "@kline_1m" in stream:
                    kline_data = data["data"].get("k", {})
                    is_closed = kline_data.get("x", False)

                    if is_closed:
                        candle = self.normalize_candle(kline_data)
                        if candle:
                            self.process_kline(candle)

        except Exception as e:
            self.log_error(f"Error handling WebSocket message: {e}")

    def on_error(self, _ws, error):
        """Handle WebSocket errors."""
        error_msg = str(error)
        self.log_error(f"WebSocket error: {error_msg}")

    def on_close(self, _ws, close_status_code, close_msg):
        """Handle WebSocket close."""
        self.ws_connected = False
        print(f"[binance] WebSocket closed: code={close_status_code}, msg={close_msg}")

        if self.connection_start_time:
            duration = time.time() - self.connection_start_time
            hours = duration / 3600
            if hours >= 23.5:
                print(
                    f"[binance] WebSocket closed after {hours:.2f} hours (expected 24h disconnect)"
                )

    def on_open(self, _ws):
        """Handle WebSocket open."""
        self.ws_connected = True
        self.connection_start_time = time.time()
        print(
            f"[binance] WebSocket connected, subscribed to {len(self.symbols)} streams"
        )

    def connect_websocket(self) -> Optional[threading.Thread]:
        """Create and connect WebSocket."""
        if not self.symbols:
            print("[binance] No symbols to connect to")
            return None

        url = self.build_ws_url()
        print(f"[binance] Connecting to WebSocket with {len(self.symbols)} streams...")

        self.ws = websocket.WebSocketApp(
            url,
            on_open=self.on_open,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
        )

        ws = self.ws
        ws_thread = threading.Thread(
            target=lambda: ws.run_forever(
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

    def stop(self):
        """Stop the collector."""
        super().stop()
        if self.ws:
            self.ws.close()


def main():
    """Entry point for the kline collector."""
    collector = BinanceKlineCollector()
    try:
        collector.run()
    except KeyboardInterrupt:
        print("Shutting down...")
        collector.stop()


if __name__ == "__main__":
    main()
