import json
import time
import threading
import requests
import websocket
from datetime import datetime, timezone as dt_timezone
from typing import Dict, Set, Optional
from decimal import Decimal

from exchange_connections.constants import BinanceContractStatus
from core.constants import RedisPubMessages
from exchange_connections.services.klines_ingest import (
    build_model_from_ws,
    bulk_insert_klines,
)
from core.redis_config import get_redis_connection


BINANCE_FUTURES_EXCHANGE_INFO_URL = "https://fapi.binance.com/fapi/v1/exchangeInfo"
BINANCE_FUTURES_WS_URL = "wss://fstream.binance.com/stream"
COINGECKO_MARKET_CAP_URL = "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=250&page=1&sparkline=false"
MARKET_CAP_ZSET_KEY = "market_cap:binance:perpetual"
SYMBOLS_REDIS_KEY = "symbols:binance:perpetual"
ERROR_LOG_KEY = "error_log"

SYMBOL_REFRESH_INTERVAL = 1800
MARKET_CAP_REFRESH_INTERVAL = 3600
WS_RECONNECT_DELAY = 5
WS_PING_INTERVAL = 60
WS_PING_TIMEOUT = 30
MAX_STREAMS_PER_CONNECTION = 1024


class BinanceKlineCollector:
    """
    Collects 1-minute kline data for all Binance perpetual futures symbols.

    Handles:
    - Symbol discovery and change detection
    - WebSocket connection with automatic reconnection (24h limit)
    - Kline batching by timestamp for complete minute collection
    - Bulk database inserts
    - Redis pubsub for correlation updates
    - Market cap ranking from CoinGecko
    """

    def __init__(self):
        self.redis = get_redis_connection()
        self.symbols: Set[str] = set()
        self.ws: Optional[websocket.WebSocketApp] = None
        self.ws_connected = False
        self.should_run = True
        self.connection_start_time: Optional[float] = None

        self.kline_batch: Dict[int, Dict[str, dict]] = (
            {}
        )  # timestamp_ms -> {symbol: kline_data}
        self.batch_lock = (
            threading.RLock()
        )  # RLock allows reentrant locking from same thread
        self.expected_symbols: Set[str] = set()

        self.last_symbol_refresh = 0
        self.last_market_cap_refresh = 0

    def log_error(self, error_msg: str):
        """Store error message to Redis for monitoring."""
        timestamp = datetime.now(dt_timezone.utc).isoformat()
        full_msg = f"[{timestamp}] {error_msg}"
        print(f"ERROR: {full_msg}")
        try:
            self.redis.lpush(ERROR_LOG_KEY, full_msg)
            self.redis.ltrim(ERROR_LOG_KEY, 0, 999)
        except Exception as e:
            print(f"Failed to log error to Redis: {e}")

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

            print(f"Fetched {len(symbols)} perpetual futures symbols from Binance")
            return symbols
        except Exception as e:
            self.log_error(f"Failed to fetch symbols from Binance: {e}")
            return set()

    def update_symbols(self):
        """Update symbols in Redis and detect changes."""
        new_symbols = self.fetch_perpetual_symbols()
        if not new_symbols:
            print("No symbols fetched, keeping existing symbols")
            return

        try:
            current_bytes = self.redis.smembers(SYMBOLS_REDIS_KEY)
            current_symbols = {s.decode("utf-8") for s in current_bytes}  # type: ignore[union-attr]
        except Exception as e:
            self.log_error(f"Failed to get current symbols from Redis: {e}")
            current_symbols = set()

        added = new_symbols - current_symbols
        removed = current_symbols - new_symbols

        timestamp = int(time.time() * 1000)

        for symbol in added:
            print(f"New symbol listed: {symbol}")
            try:
                self.redis.sadd(SYMBOLS_REDIS_KEY, symbol)
                self.redis.publish(
                    RedisPubMessages.SYMBOL_ADDED.value, f"{symbol}:{timestamp}"
                )
            except Exception as e:
                self.log_error(f"Failed to add symbol {symbol}: {e}")

        for symbol in removed:
            print(f"Symbol delisted: {symbol}")
            try:
                self.redis.srem(SYMBOLS_REDIS_KEY, symbol)
                self.redis.publish(
                    RedisPubMessages.SYMBOL_DELISTED.value, f"{symbol}:{timestamp}"
                )
            except Exception as e:
                self.log_error(f"Failed to remove symbol {symbol}: {e}")

        self.symbols = new_symbols
        self.expected_symbols = new_symbols.copy()
        self.last_symbol_refresh = time.time()

        print(
            f"Symbol update complete: {len(added)} added, {len(removed)} removed, {len(self.symbols)} total"
        )

    def fetch_market_cap_ranking(self):
        """Fetch top coins by market cap from CoinGecko and store matching symbols."""
        try:
            response = requests.get(COINGECKO_MARKET_CAP_URL, timeout=30)
            response.raise_for_status()
            coins = response.json()

            coin_ranks = {}
            for rank, coin in enumerate(coins, 1):
                coin_symbol = coin.get("symbol", "").upper()
                if coin_symbol:
                    coin_ranks[coin_symbol] = rank

            matched = []
            for symbol in self.symbols:
                base = symbol.replace("USDT", "")
                if base in coin_ranks:
                    matched.append((symbol, coin_ranks[base]))

            matched.sort(key=lambda x: x[1])
            top_100 = matched[:100]

            pipe = self.redis.pipeline()
            pipe.delete(MARKET_CAP_ZSET_KEY)
            for symbol, rank in top_100:
                pipe.zadd(MARKET_CAP_ZSET_KEY, {symbol: -rank})
            pipe.execute()

            print(f"Updated market cap ranking: {len(top_100)} symbols")
            self.last_market_cap_refresh = time.time()

        except Exception as e:
            self.log_error(f"Failed to fetch market cap ranking: {e}")

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

    def process_kline(self, data: dict):
        """Process a kline message from WebSocket."""
        try:
            kline = data.get("k", {})
            symbol = kline.get("s")
            is_closed = kline.get("x", False)

            if not is_closed:
                return

            timestamp_ms = kline.get("t")
            if not timestamp_ms or not symbol:
                return

            with self.batch_lock:
                if timestamp_ms not in self.kline_batch:
                    self.kline_batch[timestamp_ms] = {}

                if self.kline_batch:
                    latest_ts = max(self.kline_batch.keys())
                    if timestamp_ms < latest_ts:
                        print(
                            f"Discarding stale kline for {symbol} ts={timestamp_ms} (latest={latest_ts})"
                        )
                        return
                    elif timestamp_ms > latest_ts:
                        old_batches = [
                            ts for ts in self.kline_batch.keys() if ts < timestamp_ms
                        ]
                        for old_ts in old_batches:
                            self._process_batch(old_ts)

                self.kline_batch[timestamp_ms][symbol] = kline

                received = set(self.kline_batch[timestamp_ms].keys())
                expected = self.expected_symbols

                if len(received) == len(expected):
                    self._process_batch(timestamp_ms)

        except Exception as e:
            self.log_error(f"Error processing kline: {e}")

    def _process_batch(self, timestamp_ms: int):
        """Process a complete batch of klines for a timestamp."""
        with self.batch_lock:
            if timestamp_ms not in self.kline_batch:
                return

            batch = self.kline_batch.pop(timestamp_ms)

        print(f"Processing batch: ts={timestamp_ms}, symbols={len(batch)}")

        try:
            kline_models = []
            newest_values = {}

            for symbol, kline_data in batch.items():
                try:
                    model = build_model_from_ws(
                        kline_dict=kline_data,
                        exchange="binance",
                        contract_type="perpetual",
                    )
                    kline_models.append(model)

                    newest_values[symbol] = {
                        "price": float(Decimal(kline_data["c"])),
                        "volume": float(Decimal(kline_data["v"])),
                        "trades": float(kline_data["n"]),
                    }
                except Exception as e:
                    self.log_error(f"Error building model for {symbol}: {e}")

            if kline_models:
                inserted = bulk_insert_klines(kline_models)
                print(f"Inserted {inserted} klines to database")

            if newest_values:
                self.redis.publish(
                    RedisPubMessages.KLINE_SAVED_TO_DB.value,
                    json.dumps(
                        {
                            "newest_values": newest_values,
                            "timestamp": timestamp_ms,
                        }
                    ),
                )
                print(f"Published KLINE_SAVED_TO_DB with {len(newest_values)} symbols")

        except Exception as e:
            self.log_error(f"Error processing batch: {e}")

    def on_message(self, _ws, message):
        """Handle incoming WebSocket message."""
        try:
            data = json.loads(message)

            if "stream" in data and "data" in data:
                stream = data["stream"]
                if "@kline_1m" in stream:
                    self.process_kline(data["data"])

        except Exception as e:
            self.log_error(f"Error handling WebSocket message: {e}")

    def on_error(self, _ws, error):
        """Handle WebSocket errors."""
        error_msg = str(error)
        self.log_error(f"WebSocket error: {error_msg}")

        if "Reconnection signal" in error_msg or "close" in error_msg.lower():
            for _ in range(10):
                print("WebSocket error indicates need to reconnect")
            print(error_msg)

    def on_close(self, _ws, close_status_code, close_msg):
        """Handle WebSocket close."""
        self.ws_connected = False
        print(f"WebSocket closed: code={close_status_code}, msg={close_msg}")

        if self.connection_start_time:
            duration = time.time() - self.connection_start_time
            hours = duration / 3600
            if hours >= 23.5:
                print(
                    f"WebSocket closed after {hours:.2f} hours (expected 24h disconnect)"
                )

    def on_open(self, _ws):
        """Handle WebSocket open."""
        self.ws_connected = True
        self.connection_start_time = time.time()
        print(f"WebSocket connected, subscribed to {len(self.symbols)} streams")

    def on_ping(self, _ws, _message):
        """Handle ping from server."""

    def connect_websocket(self) -> Optional[threading.Thread]:
        """Create and connect WebSocket."""
        if not self.symbols:
            print("No symbols to connect to")
            return None

        url = self.build_ws_url()
        print(f"Connecting to WebSocket with {len(self.symbols)} streams...")

        self.ws = websocket.WebSocketApp(
            url,
            on_open=self.on_open,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
            on_ping=self.on_ping,
        )

        ws_thread = threading.Thread(
            target=lambda: self.ws.run_forever(  # type: ignore[union-attr]
                ping_interval=WS_PING_INTERVAL,
                ping_timeout=WS_PING_TIMEOUT,
            ),
            daemon=True,
        )
        ws_thread.start()

        return ws_thread

    def check_pending_batches(self):
        """Check for stale batches that should be processed."""
        current_time = int(time.time() * 1000)

        with self.batch_lock:
            stale_timestamps = []
            for timestamp_ms in self.kline_batch:
                if current_time - timestamp_ms > 90000:
                    stale_timestamps.append(timestamp_ms)

            for ts in stale_timestamps:
                batch = self.kline_batch.get(ts, {})
                if batch:
                    print(
                        f"Processing stale batch: ts={ts}, symbols={len(batch)}/{len(self.expected_symbols)}"
                    )

        for ts in stale_timestamps:
            self._process_batch(ts)

    def run(self):
        """Main run loop."""
        print("Starting Binance Kline Collector...")

        self.update_symbols()
        if not self.symbols:
            self.log_error("No symbols available, exiting")
            return

        self.fetch_market_cap_ranking()

        while self.should_run:
            try:
                ws_thread = self.connect_websocket()
                if ws_thread is None:
                    time.sleep(WS_RECONNECT_DELAY)
                    continue

                while self.should_run and ws_thread.is_alive():
                    time.sleep(10)

                    self.check_pending_batches()

                    if time.time() - self.last_symbol_refresh > SYMBOL_REFRESH_INTERVAL:
                        old_symbols = self.symbols.copy()
                        self.update_symbols()

                        if self.symbols != old_symbols:
                            print("Symbols changed, reconnecting WebSocket...")
                            if self.ws:
                                self.ws.close()
                            break
                    if (
                        time.time() - self.last_market_cap_refresh
                        > MARKET_CAP_REFRESH_INTERVAL
                    ):
                        self.fetch_market_cap_ranking()

                    if self.connection_start_time:
                        duration = time.time() - self.connection_start_time
                        if duration > 23.5 * 3600:
                            print(
                                "Approaching 24-hour limit, proactively reconnecting..."
                            )
                            if self.ws:
                                self.ws.close()
                            break
                if self.should_run:
                    print(
                        f"WebSocket disconnected, reconnecting in {WS_RECONNECT_DELAY}s..."
                    )
                    time.sleep(WS_RECONNECT_DELAY)

            except Exception as e:
                self.log_error(f"Error in main loop: {e}")
                time.sleep(WS_RECONNECT_DELAY)

    def stop(self):
        """Stop the collector."""
        self.should_run = False
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
