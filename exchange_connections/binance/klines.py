import json
import time
import threading
import requests
import websocket
from typing import List, Set, Optional
from decimal import Decimal

from exchange_connections.base import BaseKlineCollector
from exchange_connections.candle_types import NormalizedCandle
from exchange_connections.constants import BinanceContractStatus
from core.constants import Exchange


BINANCE_FUTURES_EXCHANGE_INFO_URL = "https://fapi.binance.com/fapi/v1/exchangeInfo"
BINANCE_FUTURES_WS_URL = "wss://fstream.binance.com/ws"
BINANCE_FUTURES_KLINES_URL = "https://fapi.binance.com/fapi/v1/klines"

WS_PING_INTERVAL = 180  # 3 minutes
WS_PING_TIMEOUT = 10  # 10 seconds
MAX_STREAMS_PER_CONNECTION = 1024
SUBSCRIBE_DELAY = 0.2  # seconds between subscribe batches


class BinanceKlineCollector(BaseKlineCollector):
    """
    Collects 1-minute kline data for all Binance perpetual futures symbols.

    Features:
    - Auto-reconnect in-process (never exits)
    - On reconnect: detect gaps (via BTC) and backfill all symbols
    - Immediate DB save for each kline (no batching)
    """

    def __init__(self):
        super().__init__(exchange=Exchange.BINANCE, contract_type="perpetual")
        self.ws: Optional[websocket.WebSocketApp] = None

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
            print(f"[binance] ERROR: Failed to fetch symbols: {e}")
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
            print(f"[binance] ERROR: Failed to normalize candle: {e}, data: {raw_data}")
            return None

    def fetch_historical_klines(
        self, symbol: str, start_time_ms: int, end_time_ms: int
    ) -> List[NormalizedCandle]:
        """Fetch historical klines via Binance REST API with retry logic."""
        max_retries = 4
        base_delay = 1

        for attempt in range(max_retries):
            try:
                response = requests.get(
                    BINANCE_FUTURES_KLINES_URL,
                    params={
                        "symbol": symbol,
                        "interval": "1m",
                        "startTime": start_time_ms,
                        "endTime": end_time_ms,
                        "limit": 1,
                    },
                    timeout=10,
                )
                response.raise_for_status()
                klines = response.json()

                result = []
                for k in klines:
                    candle = NormalizedCandle(
                        open_time_ms=int(k[0]),
                        close_time_ms=int(k[6]),
                        symbol=symbol,
                        open=Decimal(str(k[1])),
                        high=Decimal(str(k[2])),
                        low=Decimal(str(k[3])),
                        close=Decimal(str(k[4])),
                        base_volume=Decimal(str(k[5])),
                        number_of_trades=int(k[8]),
                        quote_volume=Decimal(str(k[7])),
                        taker_buy_base_volume=Decimal(str(k[9])),
                        taker_buy_quote_volume=Decimal(str(k[10])),
                    )
                    result.append(candle)

                return result

            except requests.exceptions.HTTPError as e:
                if e.response is not None and e.response.status_code in (429, 418):
                    if attempt < max_retries - 1:
                        # Check for Retry-After header
                        retry_after = e.response.headers.get("Retry-After")
                        if retry_after:
                            wait_time = int(retry_after)
                        else:
                            wait_time = base_delay * (2**attempt)
                        print(
                            f"[binance] Rate limit for {symbol}, waiting {wait_time}s (attempt {attempt + 1}/{max_retries})..."
                        )
                        time.sleep(wait_time)
                        continue
                print(
                    f"[binance] ERROR: Failed to fetch historical klines for {symbol}: {e}"
                )
                return []
            except Exception as e:
                print(
                    f"[binance] ERROR: Failed to fetch historical klines for {symbol}: {e}"
                )
                return []

        return []

    def build_stream_list(self) -> List[str]:
        """Build list of kline streams to subscribe to."""
        streams = [f"{symbol.lower()}@kline_1m" for symbol in self.symbols]
        if len(streams) > MAX_STREAMS_PER_CONNECTION:
            print(
                f"[binance] WARNING: {len(streams)} streams exceed limit of {MAX_STREAMS_PER_CONNECTION}"
            )
            streams = streams[:MAX_STREAMS_PER_CONNECTION]
        return streams

    def on_message(self, _ws, message):
        """Handle incoming WebSocket message."""
        try:
            data = json.loads(message)

            if data.get("e") == "kline":
                kline_data = data.get("k", {})
                is_closed = kline_data.get("x", False)

                if is_closed:
                    candle = self.normalize_candle(kline_data)
                    if candle:
                        self.save_kline(candle, source="live")

        except Exception as e:
            print(f"[binance] ERROR: Error handling WebSocket message: {e}")

    def on_error(self, _ws, error):
        """Handle WebSocket errors."""
        print(f"[binance] ERROR: WebSocket error: {error}")

    def on_close(self, _ws, close_status_code, close_msg):
        """Handle WebSocket close."""
        print(f"[binance] WebSocket closed: code={close_status_code}, msg={close_msg}")

    def on_open(self, _ws, streams: Optional[List[str]] = None):
        """Handle WebSocket open - subscribe to all streams in batches."""
        if streams is None:
            streams = self.build_stream_list()

        batch_size = 100
        for i in range(0, len(streams), batch_size):
            batch = streams[i : i + batch_size]
            subscribe_msg = {
                "method": "SUBSCRIBE",
                "params": batch,
                "id": i // batch_size + 1,
            }
            _ws.send(json.dumps(subscribe_msg))
            time.sleep(SUBSCRIBE_DELAY)

        print(f"[binance] WebSocket connected, subscribed to {len(streams)} streams")

    def connect_websocket(self) -> Optional[threading.Thread]:
        """Create and connect WebSocket."""
        if not self.symbols:
            print("[binance] No symbols to connect to")
            return None

        streams = self.build_stream_list()

        self.ws = websocket.WebSocketApp(
            BINANCE_FUTURES_WS_URL,
            on_open=lambda ws: self.on_open(ws, streams),
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
        )

        thread = threading.Thread(
            target=lambda w=self.ws: w.run_forever(
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
    collector = BinanceKlineCollector()
    try:
        collector.run()
    except KeyboardInterrupt:
        print("Shutting down...")
        collector.stop()


if __name__ == "__main__":
    main()
