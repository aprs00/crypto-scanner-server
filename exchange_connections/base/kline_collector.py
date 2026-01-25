import os
import time
import threading
from collections import deque
from abc import ABC, abstractmethod
from datetime import datetime, timezone as dt_timezone
from typing import Dict, List, Set, Optional

from django.db import connection

from exchange_connections.candle_types import NormalizedCandle
from exchange_connections.services.klines_ingest import (
    bulk_insert_klines,
    build_model_from_ws,
)
from core.constants import Exchange, EXCHANGE_CONFIG
from exchange_connections.constants import get_btc_symbol
from exchange_connections.selectors import (
    get_symbol_kline_data_at_timestamp,
    get_symbol_kline_data_multi_hours,
)
from core.redis_config import get_redis_connection
from core.redis_streams import publish_market_event


WS_RECONNECT_DELAY = 5
BACKFILL_MINUTES = 30
BACKFILL_RATE_LIMIT = 0.001
SYMBOL_CHECK_INTERVAL = 900  # 15 minutes


class BaseKlineCollector(ABC):
    """
    Base class for collecting 1-minute kline data from any exchange.

    Features:
    - Auto-reconnect in-process (never exits)
    - On reconnect: detect gaps (via BTC) and backfill all symbols
    - Auto-subscribe to new symbols (reconnect when symbols change)
    - Batched DB saves: buffers klines by timestamp, saves all klines for a
      minute together when the next minute arrives, then publishes Redis event

    Subclasses must implement:
    - fetch_perpetual_symbols(): Get available symbols from exchange API
    - connect_websocket(): Establish WebSocket connection
    - close_websocket(): Close WebSocket connection
    - normalize_candle(): Convert exchange-specific candle format to NormalizedCandle
    - fetch_historical_klines(): Fetch historical klines via REST API
    """

    def __init__(self, exchange: Exchange, contract_type: str = "perpetual"):
        self.exchange = exchange
        self.contract_type = contract_type
        self.symbols: Set[str] = set()
        self.should_run = True
        self.ws_thread: Optional[threading.Thread] = None
        self.backfill_rate_limit: float = BACKFILL_RATE_LIMIT
        self.redis = get_redis_connection()
        self._backfill_in_progress = False
        self._pending_timestamps: Set[int] = set()
        self._recent_timestamps: deque[int] = deque()
        self._recent_timestamp_set: Set[int] = set()
        self._recent_limit = 500
        self._primary_symbol = get_btc_symbol(self.exchange)
        # Buffer for batching klines by timestamp before saving
        self._kline_buffer: Dict[int, List[NormalizedCandle]] = {}
        # Combined hours options for correlation and zscore services
        config = EXCHANGE_CONFIG.get(self.exchange, {})
        hours_opts = config.get("hours_options", {})
        self._all_hours_options: List[int] = sorted(
            set(hours_opts.get("correlation", {}).values())
            | set(hours_opts.get("zscore", {}).values())
        )

    @abstractmethod
    def fetch_perpetual_symbols(self) -> Set[str]:
        """Fetch all perpetual symbols from exchange API."""
        pass

    @abstractmethod
    def connect_websocket(self) -> Optional[threading.Thread]:
        """Create and connect WebSocket, return the thread running it."""
        pass

    @abstractmethod
    def close_websocket(self):
        """Close the WebSocket connection."""
        pass

    @abstractmethod
    def normalize_candle(self, raw_data: dict) -> Optional[NormalizedCandle]:
        """Convert exchange-specific candle data to NormalizedCandle."""
        pass

    @abstractmethod
    def fetch_historical_klines(
        self, symbol: str, start_time_ms: int, end_time_ms: int
    ) -> List[NormalizedCandle]:
        """Fetch historical klines via REST API for backfill."""
        pass

    def update_symbols(self) -> bool:
        """Update symbols from exchange API. Returns True if symbols changed."""
        new_symbols = self.fetch_perpetual_symbols()
        if not new_symbols:
            print(f"[{self.exchange}] No symbols fetched, keeping existing symbols")
            return False

        if new_symbols != self.symbols:
            added = new_symbols - self.symbols
            removed = self.symbols - new_symbols

            for symbol in added:
                print(f"[{self.exchange}] New symbol: {symbol}")
            for symbol in removed:
                print(f"[{self.exchange}] Removed symbol: {symbol}")

            old_count = len(self.symbols)
            self.symbols = new_symbols
            print(
                f"[{self.exchange}] Symbols updated: {old_count} -> {len(self.symbols)}"
            )
            try:
                key = f"symbols:{self.exchange}:{self.contract_type}"
                pipe = self.redis.pipeline()
                pipe.delete(key)
                if self.symbols:
                    pipe.sadd(key, *self.symbols)
                pipe.execute()
            except Exception as e:
                print(f"[{self.exchange}] ERROR: Failed to sync symbols to redis: {e}")
            try:
                publish_market_event(
                    exchange=self.exchange,
                    contract_type=self.contract_type,
                    event_type="symbol_update",
                    payload={
                        "added": sorted(list(added)),
                        "removed": sorted(list(removed)),
                    },
                    redis_client=self.redis,
                )
            except Exception as e:
                print(f"[{self.exchange}] ERROR: Failed to publish symbol update: {e}")
            return old_count > 0  # Only trigger reconnect if we had symbols before

        return False

    def _remember_timestamp(self, timestamp_ms: int):
        if timestamp_ms in self._recent_timestamp_set:
            return
        self._recent_timestamps.append(timestamp_ms)
        self._recent_timestamp_set.add(timestamp_ms)
        while len(self._recent_timestamps) > self._recent_limit:
            oldest = self._recent_timestamps.popleft()
            self._recent_timestamp_set.discard(oldest)

    def _publish_kline_timestamp(
        self,
        timestamp_ms: int,
        source: str,
        newest_values: Optional[Dict] = None,
        oldest_values: Optional[Dict] = None,
    ):
        if timestamp_ms in self._recent_timestamp_set:
            return
        self._remember_timestamp(timestamp_ms)
        payload = {
            "timestamp_ms": timestamp_ms,
            "source": source,
        }
        if newest_values is not None:
            payload["newest_values"] = newest_values
        if oldest_values is not None:
            payload["oldest_values"] = oldest_values
        publish_market_event(
            exchange=self.exchange,
            contract_type=self.contract_type,
            event_type="kline",
            payload=payload,
            redis_client=self.redis,
        )

    def _buffer_live_timestamp(self, timestamp_ms: int):
        self._pending_timestamps.add(timestamp_ms)

    def _flush_pending_timestamps(self):
        if not self._pending_timestamps:
            return
        for timestamp_ms in sorted(self._pending_timestamps):
            newest, oldest = self._query_kline_data_for_timestamp(timestamp_ms)
            self._publish_kline_timestamp(
                timestamp_ms, source="live", newest_values=newest, oldest_values=oldest
            )
        self._pending_timestamps.clear()

    def detect_btc_gaps(self) -> List[int]:
        """Detect missing BTC kline timestamps in the last N minutes.

        Returns list of missing timestamp_ms values that need backfilling.
        """
        current_time_ms = int(time.time() * 1000)
        current_minute_ms = (current_time_ms // 60000) * 60000
        start_time_ms = current_minute_ms - (BACKFILL_MINUTES * 60000)

        expected_timestamps = set()
        ts = start_time_ms
        while ts < current_minute_ms:
            expected_timestamps.add(ts)
            ts += 60000

        if not expected_timestamps:
            return []

        start_dt = datetime.fromtimestamp(start_time_ms / 1000, tz=dt_timezone.utc)
        end_dt = datetime.fromtimestamp(current_minute_ms / 1000, tz=dt_timezone.utc)
        btc_symbol = get_btc_symbol(self.exchange)

        query = """
            SELECT EXTRACT(EPOCH FROM k.start_time)::bigint * 1000 AS ts_ms
            FROM cs_klines_1m k
            JOIN cs_exchanges e ON k.exchange_id = e.id
            JOIN cs_symbols s ON k.symbol_id = s.id
            WHERE e.name = %s
              AND s.name = %s
              AND k.start_time >= %s
              AND k.start_time < %s
        """

        existing_timestamps = set()
        try:
            with connection.cursor() as cursor:
                cursor.execute(query, [self.exchange, btc_symbol, start_dt, end_dt])
                for row in cursor.fetchall():
                    existing_timestamps.add(int(row[0]))
        except Exception as e:
            print(f"[{self.exchange}] ERROR: Failed to query existing timestamps: {e}")
            return []
        finally:
            connection.close()

        missing = sorted(expected_timestamps - existing_timestamps)

        if missing:
            print(
                f"[{self.exchange}] Gap detection: {len(missing)} missing BTC minutes "
                f"in last {BACKFILL_MINUTES} minutes"
            )

        return missing

    def _backfill_symbol(self, symbol: str, timestamp_ms: int) -> int:
        """Fetch and save a single symbol's kline for a timestamp. Returns count inserted."""
        try:
            candles = self.fetch_historical_klines(
                symbol=symbol,
                start_time_ms=timestamp_ms,
                end_time_ms=timestamp_ms + 60000,
            )
            count = 0
            for candle in candles:
                self.save_kline(candle, source="backfill")
                count += 1
            return count
        except Exception as e:
            print(
                f"[{self.exchange}] ERROR: Backfill failed for {symbol} "
                f"at ts={timestamp_ms}: {e}"
            )
            return 0
        finally:
            connection.close()

    def backfill_gaps(self):
        """Detect BTC gaps and backfill ALL symbols for those timestamps sequentially."""
        missing_timestamps = self.detect_btc_gaps()
        if not missing_timestamps:
            return

        self._backfill_in_progress = True
        symbols_list = list(self.symbols)
        total_requests = len(missing_timestamps) * len(symbols_list)
        print(
            f"[{self.exchange}] Backfilling {len(missing_timestamps)} gaps "
            f"for {len(symbols_list)} symbols ({total_requests} requests)..."
        )

        total_inserted = 0
        for timestamp_ms in missing_timestamps:
            if not self.should_run:
                break

            for symbol in symbols_list:
                if not self.should_run:
                    break
                count = self._backfill_symbol(symbol, timestamp_ms)
                total_inserted += count

            try:
                newest, oldest = self._query_kline_data_for_timestamp(timestamp_ms)
                self._publish_kline_timestamp(
                    timestamp_ms,
                    source="backfill",
                    newest_values=newest,
                    oldest_values=oldest,
                )
            except Exception as e:
                print(f"[{self.exchange}] ERROR: Failed to publish backfill event: {e}")

        print(f"[{self.exchange}] Backfill complete: {total_inserted} klines inserted")
        self._backfill_in_progress = False
        self._flush_pending_timestamps()

    def save_kline(self, candle: NormalizedCandle, source: str = "live"):
        """Save kline - buffers live klines, saves backfill immediately."""
        timestamp_ms = candle.open_time_ms

        if source == "live":
            # Buffer live klines for batch saving
            self._kline_buffer.setdefault(timestamp_ms, []).append(candle)
            self._flush_completed_minutes(timestamp_ms)
        else:
            # Backfill: save immediately (already complete data)
            self._save_klines_batch([candle])

    def _flush_completed_minutes(self, current_timestamp_ms: int):
        """Flush all buffered minutes older than current timestamp."""
        completed = [ts for ts in self._kline_buffer if ts < current_timestamp_ms]

        for ts in sorted(completed):
            candles = self._kline_buffer.pop(ts)
            self._save_klines_batch(candles)

            if self._backfill_in_progress:
                self._buffer_live_timestamp(ts)
            else:
                # Query for newest and oldest values to include in the stream
                newest, oldest = self._query_kline_data_for_timestamp(ts)
                self._publish_kline_timestamp(
                    ts, source="live", newest_values=newest, oldest_values=oldest
                )

    def _query_kline_data_for_timestamp(
        self, timestamp_ms: int
    ) -> tuple[Optional[Dict], Optional[Dict]]:
        """Query newest and oldest kline data for the given timestamp."""
        if not self.symbols or not self._all_hours_options:
            return None, None

        try:
            symbols_list = list(self.symbols)
            newest = get_symbol_kline_data_at_timestamp(
                symbols=symbols_list,
                exchange=self.exchange,
                contract_type=self.contract_type,
                kline_timestamp_ms=timestamp_ms,
            )
            oldest = get_symbol_kline_data_multi_hours(
                symbols=symbols_list,
                exchange=self.exchange,
                contract_type=self.contract_type,
                hours_list=self._all_hours_options,
                kline_timestamp_ms=timestamp_ms,
            )
            return newest, oldest
        except Exception as e:
            print(
                f"[{self.exchange}] ERROR: Failed to query kline data for stream: {e}"
            )
            return None, None
        finally:
            connection.close()

    def _flush_all_buffered_klines(self):
        """Flush all remaining buffered klines (called on disconnect)."""
        if not self._kline_buffer:
            return

        for ts in sorted(self._kline_buffer.keys()):
            candles = self._kline_buffer.pop(ts)
            self._save_klines_batch(candles)
            print(
                f"[{self.exchange}] Flushed {len(candles)} buffered klines for timestamp {ts}"
            )
            if self._backfill_in_progress:
                self._buffer_live_timestamp(ts)
            else:
                newest, oldest = self._query_kline_data_for_timestamp(ts)
                self._publish_kline_timestamp(
                    ts, source="live", newest_values=newest, oldest_values=oldest
                )

    def _save_klines_batch(self, candles: List[NormalizedCandle]):
        """Batch save multiple klines to database."""
        if not candles:
            return
        try:
            models = [
                build_model_from_ws(
                    kline_dict=c.to_dict(),
                    exchange=self.exchange,
                    contract_type=self.contract_type,
                )
                for c in candles
            ]
            bulk_insert_klines(models)
        except Exception as e:
            print(
                f"[{self.exchange}] ERROR: Failed to batch save {len(candles)} klines: {e}"
            )

    def run(self):
        """Main loop - runs forever with auto-reconnect."""
        print(f"Starting {self.exchange.title()} Kline Collector...")

        while self.should_run:
            # Update symbols from exchange
            self.update_symbols()

            if not self.symbols:
                print(f"[{self.exchange}] No symbols available, waiting 60s...")
                time.sleep(60)
                continue

            # Connect WebSocket
            self.ws_thread = self.connect_websocket()
            if not self.ws_thread:
                print(
                    f"[{self.exchange}] Failed to connect, retrying in {WS_RECONNECT_DELAY}s..."
                )
                time.sleep(WS_RECONNECT_DELAY)
                continue

            # On connect: detect gaps (via BTC) and backfill all symbols
            if os.environ.get("DISABLE_BACKFILL", "").lower() not in (
                "1",
                "true",
                "yes",
            ):
                self.backfill_gaps()
            else:
                print(
                    f"[{self.exchange}] Backfill disabled via DISABLE_BACKFILL env var"
                )

            # Wait for disconnect or symbol change
            last_symbol_check = time.time()
            poll_interval = 5
            while self.should_run:
                if not self.ws_thread or not self.ws_thread.is_alive():
                    break

                time.sleep(poll_interval)

                # Check for symbol changes periodically
                if time.time() - last_symbol_check >= SYMBOL_CHECK_INTERVAL:
                    last_symbol_check = time.time()
                    old_symbols = self.symbols.copy()
                    self.update_symbols()

                    if self.symbols != old_symbols:
                        print(f"[{self.exchange}] Symbols changed, reconnecting...")
                        self.close_websocket()
                        break

            # Flush any buffered klines before reconnecting
            self._flush_all_buffered_klines()
            print(f"[{self.exchange}] Reconnecting in {WS_RECONNECT_DELAY}s...")
            time.sleep(WS_RECONNECT_DELAY)

    def stop(self):
        """Stop the collector."""
        self.should_run = False
        self._flush_all_buffered_klines()
        self.close_websocket()
