import json
import time
import threading
from abc import ABC, abstractmethod
from datetime import datetime, timezone as dt_timezone
from decimal import Decimal
from typing import Dict, Set, Optional, cast

import requests

from exchange_connections.candle_types import NormalizedCandle
from exchange_connections.services.klines_ingest import (
    bulk_insert_klines,
    build_model_from_ws,
)
from exchange_connections.selectors import get_symbol_kline_data_multi_hours
from core.constants import Exchange, RedisStreamKeys, TIMEFRAME_HOURS
from core.redis_config import get_redis_connection
from core.redis_streams import StreamPublisher


ERROR_LOG_KEY = "error_log"
WS_RECONNECT_DELAY = 5
COINGECKO_MARKET_CAP_URL = "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=250&page=1&sparkline=false"
MARKET_CAP_REFRESH_INTERVAL = 14400


class BaseKlineCollector(ABC):
    """
    Base class for collecting 1-minute kline data from any exchange.

    Handles:
    - Symbol discovery and change detection
    - Kline batching by timestamp for complete minute collection
    - Bulk database inserts
    - Redis pubsub for correlation updates

    Subclasses must implement:
    - fetch_perpetual_symbols(): Get available symbols from exchange API
    - connect_websocket(): Establish WebSocket connection
    - normalize_candle(): Convert exchange-specific candle format to NormalizedCandle
    """

    def __init__(self, exchange: Exchange, contract_type: str = "perpetual"):
        self.exchange = exchange
        self.contract_type = contract_type
        self.redis = get_redis_connection()
        self.stream_publisher = StreamPublisher(self.redis)
        self.symbols: Set[str] = set()
        self.should_run = True

        self.kline_batch: Dict[int, Dict[str, NormalizedCandle]] = {}
        self.batch_lock = threading.RLock()
        self.expected_symbols: Set[str] = set()

        self.last_prices: Dict[str, Decimal] = {}
        self.generate_synthetic_candles = False
        self.last_processed_timestamp: int = 0

        self.last_symbol_refresh = 0
        self.symbol_refresh_interval = 1800
        self.last_market_cap_refresh = 0

    @property
    def symbols_redis_key(self) -> str:
        return f"symbols:{self.exchange}:{self.contract_type}"

    @property
    def market_cap_redis_key(self) -> str:
        return f"market_cap:{self.exchange}:{self.contract_type}"

    def log_error(self, error_msg: str):
        """Store error message to Redis for monitoring."""
        timestamp = datetime.now(dt_timezone.utc).isoformat()
        full_msg = f"[{self.exchange.upper()}][{timestamp}] {error_msg}"
        print(f"ERROR: {full_msg}")
        try:
            self.redis.lpush(ERROR_LOG_KEY, full_msg)
            self.redis.ltrim(ERROR_LOG_KEY, 0, 999)
        except Exception as e:
            print(f"Failed to log error to Redis: {e}")

    @abstractmethod
    def fetch_perpetual_symbols(self) -> Set[str]:
        """Fetch all perpetual symbols from exchange API."""
        pass

    @abstractmethod
    def connect_websocket(self) -> Optional[threading.Thread]:
        """Create and connect WebSocket, return the thread running it."""
        pass

    @abstractmethod
    def normalize_candle(self, raw_data: dict) -> Optional[NormalizedCandle]:
        """Convert exchange-specific candle data to NormalizedCandle."""
        pass

    @abstractmethod
    def map_coingecko_symbol(self, coingecko_symbol: str) -> Optional[str]:
        """Map a CoinGecko symbol (e.g., 'BTC') to exchange symbol format.

        Returns None if the symbol doesn't exist on this exchange.
        Examples:
            - Binance: 'BTC' -> 'BTCUSDT'
            - Hyperliquid: 'BTC' -> 'BTC'
        """
        pass

    def update_symbols(self):
        """Update symbols in Redis and detect changes."""
        new_symbols = self.fetch_perpetual_symbols()
        if not new_symbols:
            print(f"[{self.exchange}] No symbols fetched, keeping existing symbols")
            return

        try:
            current_bytes = cast(
                Set[bytes], self.redis.smembers(self.symbols_redis_key)
            )
            current_symbols = {s.decode("utf-8") for s in current_bytes}
        except Exception as e:
            self.log_error(f"Failed to get current symbols from Redis: {e}")
            current_symbols = set()

        added = new_symbols - current_symbols
        removed = current_symbols - new_symbols

        timestamp = int(time.time() * 1000)

        for symbol in added:
            print(f"[{self.exchange}] New symbol listed: {symbol}")
            try:
                self.redis.sadd(self.symbols_redis_key, symbol)
                # Publish to stream
                stream_key = RedisStreamKeys.symbols(self.exchange)
                msg_id = self.stream_publisher.publish(
                    stream_key,
                    {
                        "exchange": self.exchange,
                        "symbol": symbol,
                        "event": "ADDED",
                        "timestamp": timestamp,
                    },
                    maxlen=100,
                )
                if msg_id:
                    print(
                        f"[{self.exchange}] Published SYMBOL_ADDED to stream: {symbol}"
                    )
            except Exception as e:
                self.log_error(f"Failed to add symbol {symbol}: {e}")

        for symbol in removed:
            print(f"[{self.exchange}] Symbol delisted: {symbol}")
            try:
                self.redis.srem(self.symbols_redis_key, symbol)
                # Publish to stream
                stream_key = RedisStreamKeys.symbols(self.exchange)
                msg_id = self.stream_publisher.publish(
                    stream_key,
                    {
                        "exchange": self.exchange,
                        "symbol": symbol,
                        "event": "DELISTED",
                        "timestamp": timestamp,
                    },
                    maxlen=100,
                )
                if msg_id:
                    print(
                        f"[{self.exchange}] Published SYMBOL_DELISTED to stream: {symbol}"
                    )
            except Exception as e:
                self.log_error(f"Failed to remove symbol {symbol}: {e}")

        self.symbols = new_symbols
        self.expected_symbols = new_symbols.copy()
        self.last_symbol_refresh = time.time()

        print(
            f"[{self.exchange}] Symbol update complete: {len(added)} added, "
            f"{len(removed)} removed, {len(self.symbols)} total"
        )

    def _get_coingecko_rankings(self) -> dict:
        """Fetch CoinGecko market cap data, using Redis cache if available.

        Returns dict mapping symbol (e.g., 'BTC') to rank (1 = highest market cap).
        Cache is shared across all exchanges with 4-hour TTL.
        """
        cache_key = "coingecko:market_cap_rankings"

        try:
            cached = cast(Optional[bytes], self.redis.get(cache_key))
            if cached:
                return json.loads(cached)
        except Exception as e:
            self.log_error(f"Failed to read CoinGecko cache: {e}")

        try:
            response = requests.get(COINGECKO_MARKET_CAP_URL, timeout=30)
            response.raise_for_status()
            coins = response.json()

            coin_ranks = {}
            for rank, coin in enumerate(coins, 1):
                coin_symbol = coin.get("symbol", "").upper()
                if coin_symbol:
                    coin_ranks[coin_symbol] = rank

            try:
                self.redis.setex(
                    cache_key, MARKET_CAP_REFRESH_INTERVAL, json.dumps(coin_ranks)
                )
            except Exception as e:
                self.log_error(f"Failed to cache CoinGecko data: {e}")

            print(
                f"[{self.exchange}] Fetched fresh CoinGecko market cap data: {len(coin_ranks)} coins"
            )
            return coin_ranks

        except Exception as e:
            self.log_error(f"Failed to fetch CoinGecko market cap: {e}")
            return {}

    def fetch_market_cap_ranking(self):
        """Update market cap ranking for this exchange using CoinGecko data."""
        coin_ranks = self._get_coingecko_rankings()
        if not coin_ranks:
            return

        matched = []
        for coingecko_symbol, rank in coin_ranks.items():
            exchange_symbol = self.map_coingecko_symbol(coingecko_symbol)
            if exchange_symbol and exchange_symbol in self.symbols:
                matched.append((exchange_symbol, rank))

        matched.sort(key=lambda x: x[1])
        top_100 = matched[:100]

        try:
            pipe = self.redis.pipeline()
            pipe.delete(self.market_cap_redis_key)
            for symbol, rank in top_100:
                pipe.zadd(self.market_cap_redis_key, {symbol: -rank})
            pipe.execute()

            print(
                f"[{self.exchange}] Updated market cap ranking: {len(top_100)} symbols"
            )
            self.last_market_cap_refresh = time.time()

        except Exception as e:
            self.log_error(f"Failed to update market cap ranking: {e}")

    def process_kline(self, candle: NormalizedCandle):
        """Process a normalized kline from WebSocket."""
        try:
            timestamp_ms = candle.open_time_ms
            symbol = candle.symbol

            self.last_prices[symbol] = candle.close

            batches_to_process = []

            with self.batch_lock:
                if timestamp_ms <= self.last_processed_timestamp:
                    return

                existing_timestamps = set(self.kline_batch.keys())

                if existing_timestamps:
                    current_latest = max(existing_timestamps)

                    if timestamp_ms < current_latest:
                        if timestamp_ms not in self.kline_batch:
                            print(
                                f"[{self.exchange}] Discarding stale kline for {symbol} "
                                f"ts={timestamp_ms} (latest={current_latest})"
                            )
                            return
                        # Late arrival for existing batch - continue to add it

                    elif timestamp_ms > current_latest:
                        # New minute arrived - process all older batches
                        for ts in list(existing_timestamps):
                            batch = self.kline_batch.pop(ts)
                            batches_to_process.append((ts, batch))

                if timestamp_ms not in self.kline_batch:
                    self.kline_batch[timestamp_ms] = {}
                self.kline_batch[timestamp_ms][symbol] = candle

                if len(self.kline_batch[timestamp_ms]) == len(self.expected_symbols):
                    batch = self.kline_batch.pop(timestamp_ms)
                    batches_to_process.append((timestamp_ms, batch))

            # Process batches OUTSIDE the lock
            for ts, batch in batches_to_process:
                self._do_batch_processing(ts, batch)

        except Exception as e:
            self.log_error(f"Error processing kline: {e}")

    def _create_synthetic_candle(
        self, symbol: str, timestamp_ms: int, last_price: Decimal
    ) -> NormalizedCandle:
        """Create a synthetic candle for a symbol with no trades."""
        close_time_ms = timestamp_ms + 60000 - 1
        return NormalizedCandle(
            open_time_ms=timestamp_ms,
            close_time_ms=close_time_ms,
            symbol=symbol,
            open=last_price,
            high=last_price,
            low=last_price,
            close=last_price,
            base_volume=Decimal("0"),
            number_of_trades=0,
            quote_volume=None,
            taker_buy_base_volume=None,
            taker_buy_quote_volume=None,
        )

    def _do_batch_processing(
        self, timestamp_ms: int, batch: Dict[str, NormalizedCandle]
    ):
        """Process a batch - add synthetics if needed, save to DB, publish to Redis.

        This method does the heavy work and should be called OUTSIDE any locks.
        """
        with self.batch_lock:
            if timestamp_ms > self.last_processed_timestamp:
                self.last_processed_timestamp = timestamp_ms

        if self.generate_synthetic_candles:
            missing_symbols = self.expected_symbols - set(batch.keys())
            synthetic_count = 0
            for symbol in missing_symbols:
                if symbol in self.last_prices:
                    batch[symbol] = self._create_synthetic_candle(
                        symbol, timestamp_ms, self.last_prices[symbol]
                    )
                    synthetic_count += 1

            if synthetic_count > 0:
                print(
                    f"[{self.exchange}] Generated {synthetic_count} synthetic candles for ts={timestamp_ms}"
                )

        print(
            f"[{self.exchange}] Processing batch: ts={timestamp_ms}, symbols={len(batch)}"
        )

        try:
            kline_models = []
            newest_values = {}

            for symbol, candle in batch.items():
                try:
                    model = build_model_from_ws(
                        kline_dict=candle.to_dict(),
                        exchange=self.exchange,
                        contract_type=self.contract_type,
                    )
                    kline_models.append(model)

                    newest_values[symbol] = {
                        "price": float(candle.close),
                        "volume": float(candle.base_volume),
                        "trades": float(candle.number_of_trades),
                    }
                except Exception as e:
                    self.log_error(f"Error building model for {symbol}: {e}")

            if kline_models:
                inserted = bulk_insert_klines(kline_models)
                print(f"[{self.exchange}] Inserted {inserted} klines to database")

            if newest_values:
                oldest_values = get_symbol_kline_data_multi_hours(
                    symbols=list(batch.keys()),
                    exchange=self.exchange,
                    contract_type=self.contract_type,
                    hours_list=TIMEFRAME_HOURS,
                    kline_timestamp_ms=timestamp_ms,
                )

                # Publish to stream with automatic retries
                stream_key = RedisStreamKeys.klines(self.exchange)
                msg_id = self.stream_publisher.publish(
                    stream_key,
                    {
                        "exchange": self.exchange,
                        "contract_type": self.contract_type,
                        "newest_values": newest_values,
                        "oldest_values": oldest_values,
                        "timestamp": timestamp_ms,
                    },
                    maxlen=1000,  # Keep last ~7 days at 1min intervals
                )
                if msg_id:
                    print(
                        f"[{self.exchange}] Published to stream {stream_key} with {len(newest_values)} symbols (ID: {msg_id})"
                    )
                else:
                    self.log_error(
                        f"Failed to publish kline update to stream for timestamp {timestamp_ms}"
                    )

        except Exception as e:
            self.log_error(f"Error processing batch: {e}")

    def _process_batch(self, timestamp_ms: int):
        """Pop batch from kline_batch and process it.

        Used by check_pending_batches() and subclasses (e.g., Hyperliquid).
        """
        with self.batch_lock:
            if timestamp_ms not in self.kline_batch:
                return
            batch = self.kline_batch.pop(timestamp_ms)

        self._do_batch_processing(timestamp_ms, batch)

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
                        f"[{self.exchange}] Processing stale batch: ts={ts}, "
                        f"symbols={len(batch)}/{len(self.expected_symbols)}"
                    )

        for ts in stale_timestamps:
            self._process_batch(ts)

    def run(self):
        """Main run loop."""
        print(f"Starting {self.exchange.title()} Kline Collector...")

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

                    if (
                        time.time() - self.last_symbol_refresh
                        > self.symbol_refresh_interval
                    ):
                        old_symbols = self.symbols.copy()
                        self.update_symbols()

                        if self.symbols != old_symbols:
                            print(
                                f"[{self.exchange}] Symbols changed, reconnecting WebSocket..."
                            )
                            self.on_symbols_changed()
                            break

                    if (
                        time.time() - self.last_market_cap_refresh
                        > MARKET_CAP_REFRESH_INTERVAL
                    ):
                        self.fetch_market_cap_ranking()

                if self.should_run:
                    print(
                        f"[{self.exchange}] WebSocket disconnected, "
                        f"reconnecting in {WS_RECONNECT_DELAY}s..."
                    )
                    time.sleep(WS_RECONNECT_DELAY)

            except Exception as e:
                self.log_error(f"Error in main loop: {e}")
                time.sleep(WS_RECONNECT_DELAY)

    def on_symbols_changed(self):
        """Called when symbols change and need to reconnect. Override in subclass if needed."""
        pass

    def stop(self):
        """Stop the collector."""
        self.should_run = False
