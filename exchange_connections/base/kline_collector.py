import json
import os
import time
import threading
import urllib.request
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
# Detect recent sparse gaps even when no disconnect happened.
RECENT_GAP_LOOKBACK_MINUTES = 30
# Reconnect sockets that stay open but stop delivering closed candles.
LIVE_STALE_RECONNECT_SECONDS = 180
# Safety cap for reconnect catch-up to avoid unbounded backfill workloads.
MAX_BACKFILL_LOOKBACK_MINUTES = 7 * 24 * 60
# Default per-request backfill chunk size (subclasses can override).
DEFAULT_BACKFILL_CHUNK_MINUTES = 1000
# Avoid publishing backfill events for very recent minutes; live stream should
# own the frontier to prevent backfill/live duplicate timestamp processing.
BACKFILL_PUBLISH_LAG_MINUTES = 2
SYMBOL_CHECK_INTERVAL = 900  # 15 minutes
COINGECKO_MARKET_CAP_URL = (
    "https://api.coingecko.com/api/v3/coins/markets"
    "?vs_currency=usd&order=market_cap_desc&per_page=250&page=1&sparkline=false"
)
MARKET_CAP_REFRESH_INTERVAL = 14400
MARKET_CAP_MAX_SYMBOLS = 100


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
        self.redis = get_redis_connection()
        self._backfill_in_progress = False
        self._pending_timestamps: Set[int] = set()
        self._recent_timestamps: deque[int] = deque()
        self._recent_timestamp_set: Set[int] = set()
        self._recent_limit = 500
        self._primary_symbol = get_btc_symbol(self.exchange)
        self._last_market_cap_refresh = 0
        self._last_published_timestamp_ms: Optional[int] = None
        self._last_live_candle_wall_time: Optional[float] = None
        self._published_counts: Dict[str, int] = {"live": 0, "backfill": 0}
        self._dropped_duplicate_publishes = 0
        self._throttled_log_at: Dict[str, float] = {}
        # Buffer for batching klines by timestamp before saving
        self._kline_buffer: Dict[int, List[NormalizedCandle]] = {}
        # Combined hours options for correlation and zscore services
        config = EXCHANGE_CONFIG.get(self.exchange, {})
        hours_opts = config.get("hours_options", {})
        self._all_hours_options: List[int] = sorted(
            set(hours_opts.get("correlation", {}).values())
            | set(hours_opts.get("zscore", {}).values())
        )

    def _log_throttled(self, key: str, every_seconds: int, message: str):
        now = time.time()
        last = self._throttled_log_at.get(key, 0.0)
        if now - last >= every_seconds:
            print(message)
            self._throttled_log_at[key] = now

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

    def build_backfill_synthetic_candles(
        self, timestamp_ms: int, missing_symbols: List[str]
    ) -> List[NormalizedCandle]:
        """Optionally provide synthetic candles for missing backfill symbols."""
        return []

    def get_recent_gap_lookback_minutes(self) -> int:
        """Minutes to scan for sparse recent gaps on reconnect."""
        value = os.environ.get("BACKFILL_RECENT_LOOKBACK_MINUTES")
        if value is None:
            return RECENT_GAP_LOOKBACK_MINUTES
        try:
            parsed = int(value)
            return parsed if parsed > 0 else RECENT_GAP_LOOKBACK_MINUTES
        except ValueError:
            return RECENT_GAP_LOOKBACK_MINUTES

    def get_max_backfill_minutes(self) -> int:
        """Maximum reconnect catch-up window in minutes."""
        value = os.environ.get("MAX_BACKFILL_MINUTES")
        if value is None:
            return MAX_BACKFILL_LOOKBACK_MINUTES
        try:
            parsed = int(value)
            return parsed if parsed > 0 else MAX_BACKFILL_LOOKBACK_MINUTES
        except ValueError:
            return MAX_BACKFILL_LOOKBACK_MINUTES

    def get_backfill_chunk_minutes(self) -> int:
        """Max candles requested per backfill REST call."""
        return DEFAULT_BACKFILL_CHUNK_MINUTES

    def get_backfill_publish_lag_minutes(self) -> int:
        """Only publish backfill events older than this many minutes from now."""
        value = os.environ.get("BACKFILL_PUBLISH_LAG_MINUTES")
        if value is None:
            return BACKFILL_PUBLISH_LAG_MINUTES
        try:
            parsed = int(value)
            return parsed if parsed > 0 else BACKFILL_PUBLISH_LAG_MINUTES
        except ValueError:
            return BACKFILL_PUBLISH_LAG_MINUTES

    def _should_disable_backfill(self) -> bool:
        disable = os.environ.get("DISABLE_BACKFILL", "").lower() in ("1", "true", "yes")
        if not disable:
            return False

        allow_disable = os.environ.get("ALLOW_DISABLE_BACKFILL", "").lower() in (
            "1",
            "true",
            "yes",
        )
        if allow_disable:
            print(
                f"[{self.exchange}] WARNING: Backfill disabled "
                f"(DISABLE_BACKFILL + ALLOW_DISABLE_BACKFILL)"
            )
            return True

        print(
            f"[{self.exchange}] WARNING: Ignoring DISABLE_BACKFILL; "
            f"set ALLOW_DISABLE_BACKFILL=1 to disable backfill explicitly"
        )
        return False

    @staticmethod
    def _build_contiguous_ranges(timestamps: List[int]) -> List[tuple[int, int]]:
        """Convert sorted minute timestamps into contiguous [start, end) ranges."""
        if not timestamps:
            return []

        ordered = sorted(set(timestamps))
        ranges: List[tuple[int, int]] = []
        start = ordered[0]
        prev = ordered[0]

        for ts in ordered[1:]:
            if ts - prev != 60000:
                ranges.append((start, prev + 60000))
                start = ts
            prev = ts

        ranges.append((start, prev + 60000))
        return ranges

    def _chunk_ranges(self, ranges: List[tuple[int, int]]) -> List[tuple[int, int]]:
        chunk_minutes = max(1, self.get_backfill_chunk_minutes())
        chunk_ms = chunk_minutes * 60000
        chunks: List[tuple[int, int]] = []

        for start_ms, end_ms in ranges:
            cursor = start_ms
            while cursor < end_ms:
                chunk_end = min(cursor + chunk_ms, end_ms)
                chunks.append((cursor, chunk_end))
                cursor = chunk_end

        return chunks

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

    def _market_cap_redis_key(self) -> str:
        return f"market_cap:{self.exchange}:{self.contract_type}"

    def _map_market_cap_symbol(self, coingecko_symbol: str) -> Optional[str]:
        symbol = coingecko_symbol.upper()
        if self.exchange == Exchange.HYPERLIQUID:
            exchange_symbol = symbol
        elif self.exchange == Exchange.OKX:
            exchange_symbol = f"{symbol}-USDT-SWAP"
        else:
            exchange_symbol = f"{symbol}USDT"
        return exchange_symbol if exchange_symbol in self.symbols else None

    def _get_coingecko_rankings(self) -> Dict[str, int]:
        """Fetch CoinGecko market cap data, using Redis cache if available."""
        cache_key = "coingecko:market_cap_rankings"

        try:
            cached = self.redis.get(cache_key)
            if cached:
                cached_text = (
                    cached.decode("utf-8")
                    if isinstance(cached, (bytes, bytearray))
                    else cached
                )
                if isinstance(cached_text, str):
                    return json.loads(cached_text)
        except Exception as e:
            print(f"[{self.exchange}] ERROR: Failed to read CoinGecko cache: {e}")

        try:
            request = urllib.request.Request(
                COINGECKO_MARKET_CAP_URL,
                headers={"User-Agent": "crypto-scanner-server/1.0"},
            )
            with urllib.request.urlopen(request, timeout=30) as response:
                coins = json.loads(response.read().decode("utf-8"))

            coin_ranks: Dict[str, int] = {}
            for rank, coin in enumerate(coins, 1):
                coin_symbol = coin.get("symbol", "").upper()
                if coin_symbol:
                    coin_ranks[coin_symbol] = rank

            try:
                self.redis.setex(
                    cache_key, MARKET_CAP_REFRESH_INTERVAL, json.dumps(coin_ranks)
                )
            except Exception as e:
                print(f"[{self.exchange}] ERROR: Failed to cache CoinGecko data: {e}")

            print(
                f"[{self.exchange}] Fetched fresh CoinGecko market cap data: {len(coin_ranks)} coins"
            )
            return coin_ranks
        except Exception as e:
            print(f"[{self.exchange}] ERROR: Failed to fetch CoinGecko market cap: {e}")
            return {}

    def fetch_market_cap_ranking(self):
        """Update market cap ranking for this exchange using CoinGecko data."""
        if not self.symbols:
            return

        coin_ranks = self._get_coingecko_rankings()
        if not coin_ranks:
            return

        matched: List[tuple[str, int]] = []
        for coingecko_symbol, rank in coin_ranks.items():
            exchange_symbol = self._map_market_cap_symbol(coingecko_symbol)
            if exchange_symbol:
                matched.append((exchange_symbol, rank))

        matched.sort(key=lambda x: x[1])
        top = matched[:MARKET_CAP_MAX_SYMBOLS]

        try:
            pipe = self.redis.pipeline()
            pipe.delete(self._market_cap_redis_key())
            if top:
                pipe.zadd(
                    self._market_cap_redis_key(),
                    {symbol: -rank for symbol, rank in top},
                )
            pipe.execute()
            self._last_market_cap_refresh = time.time()
            print(f"[{self.exchange}] Updated market cap ranking: {len(top)} symbols")
        except Exception as e:
            print(f"[{self.exchange}] ERROR: Failed to update market cap ranking: {e}")

    def _maybe_refresh_market_cap(self, force: bool = False):
        if not self.symbols:
            return
        if force or (
            time.time() - self._last_market_cap_refresh > MARKET_CAP_REFRESH_INTERVAL
        ):
            self.fetch_market_cap_ranking()

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
            self._dropped_duplicate_publishes += 1
            self._log_throttled(
                "duplicate_publish_dropped",
                15,
                f"[{self.exchange}][WARN] Dropping duplicate kline publish ts={timestamp_ms} source={source} "
                f"(total_dropped={self._dropped_duplicate_publishes})",
            )
            return
        if (
            self._last_published_timestamp_ms is not None
            and timestamp_ms <= self._last_published_timestamp_ms
        ):
            self._log_throttled(
                "publish_out_of_order",
                10,
                f"[{self.exchange}][WARN] Out-of-order stream publish ts={timestamp_ms}, "
                f"prev_published_ts={self._last_published_timestamp_ms}, source={source}",
            )

        expected = len(self.symbols)
        newest_count = len(newest_values or {})
        if expected and newest_values is not None and newest_count < expected:
            missing = [s for s in sorted(self.symbols) if s not in newest_values][:5]
            self._log_throttled(
                "publish_incomplete_newest",
                20,
                f"[{self.exchange}][WARN] Publishing incomplete newest_values at ts={timestamp_ms} source={source}: "
                f"{newest_count}/{expected} symbols, sample missing={missing}",
            )
        if expected and oldest_values is not None:
            for hours in self._all_hours_options:
                hourly = oldest_values.get(hours) or oldest_values.get(str(hours), {})
                if len(hourly) < expected:
                    self._log_throttled(
                        f"publish_incomplete_oldest_{hours}",
                        20,
                        f"[{self.exchange}][WARN] Publishing incomplete oldest_values at ts={timestamp_ms} source={source}: "
                        f"hours={hours} count={len(hourly)}/{expected}",
                    )

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
        self._last_published_timestamp_ms = max(
            timestamp_ms,
            self._last_published_timestamp_ms or timestamp_ms,
        )
        self._published_counts[source] = self._published_counts.get(source, 0) + 1
        self._log_throttled(
            "publish_progress",
            60,
            f"[{self.exchange}] Stream publish progress: last_ts={self._last_published_timestamp_ms} "
            f"live={self._published_counts.get('live', 0)} backfill={self._published_counts.get('backfill', 0)} "
            f"duplicate_dropped={self._dropped_duplicate_publishes}",
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
        """Detect missing BTC minute timestamps in reconnect catch-up window."""
        current_time_ms = int(time.time() * 1000)
        current_minute_ms = (current_time_ms // 60000) * 60000
        recent_lookback_minutes = self.get_recent_gap_lookback_minutes()
        max_backfill_minutes = self.get_max_backfill_minutes()
        recent_window_start_ms = current_minute_ms - (recent_lookback_minutes * 60000)
        max_window_start_ms = current_minute_ms - (max_backfill_minutes * 60000)
        btc_symbol = self._primary_symbol

        latest_btc_query = """
            SELECT MAX(k.start_time) AS latest_start_time
            FROM cs_klines_1m k
            JOIN cs_exchanges e ON k.exchange_id = e.id
            JOIN cs_symbols s ON k.symbol_id = s.id
            WHERE e.name = %s
              AND s.name = %s
        """

        latest_btc_dt = None
        try:
            with connection.cursor() as cursor:
                cursor.execute(latest_btc_query, [self.exchange, btc_symbol])
                row = cursor.fetchone()
                latest_btc_dt = row[0] if row and row[0] else None
        except Exception as e:
            print(f"[{self.exchange}] ERROR: Failed to query latest BTC timestamp: {e}")
            return []
        finally:
            connection.close()

        if latest_btc_dt is None:
            start_time_ms = recent_window_start_ms
        else:
            latest_btc_ms = int(latest_btc_dt.timestamp() * 1000)
            # Cover both reconnect-tail outage and sparse recent gaps.
            start_time_ms = min(recent_window_start_ms, latest_btc_ms + 60000)

        if start_time_ms < max_window_start_ms:
            start_time_ms = max_window_start_ms

        expected_timestamps = set(range(start_time_ms, current_minute_ms, 60000))

        if not expected_timestamps:
            return []

        start_dt = datetime.fromtimestamp(start_time_ms / 1000, tz=dt_timezone.utc)
        end_dt = datetime.fromtimestamp(current_minute_ms / 1000, tz=dt_timezone.utc)

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
            scanned_minutes = (current_minute_ms - start_time_ms) // 60000
            print(
                f"[{self.exchange}] Gap detection: {len(missing)} missing BTC minutes "
                f"in last {scanned_minutes} minutes "
                f"(oldest={missing[0]}, newest={missing[-1]})"
            )

        return missing

    def _backfill_symbol_range(self, symbol: str, start_ms: int, end_ms: int) -> int:
        """Fetch and save a symbol's klines for [start_ms, end_ms)."""
        try:
            candles = self.fetch_historical_klines(
                symbol=symbol,
                start_time_ms=start_ms,
                end_time_ms=end_ms,
            )
            if candles:
                filtered = [c for c in candles if start_ms <= c.open_time_ms < end_ms]
                if filtered:
                    self._save_klines_batch(filtered)
                return len(filtered)
            return 0
        except Exception as e:
            print(
                f"[{self.exchange}] ERROR: Backfill failed for {symbol} "
                f"in [{start_ms}, {end_ms}): {e}"
            )
            return 0
        finally:
            connection.close()

    def backfill_gaps(self):
        """Detect BTC gaps and backfill ALL symbols for those timestamps sequentially."""
        missing_timestamps = self.detect_btc_gaps()
        if not missing_timestamps:
            return

        ranges = self._build_contiguous_ranges(missing_timestamps)
        chunks = self._chunk_ranges(ranges)
        symbols_list = list(self.symbols)
        total_requests = len(chunks) * len(symbols_list)

        self._backfill_in_progress = True
        try:
            print(
                f"[{self.exchange}] Backfilling {len(missing_timestamps)} gaps "
                f"across {len(chunks)} chunk(s) for {len(symbols_list)} symbols "
                f"({total_requests} requests)..."
            )

            total_inserted = 0
            for chunk_start_ms, chunk_end_ms in chunks:
                if not self.should_run:
                    break

                for symbol in symbols_list:
                    if not self.should_run:
                        break
                    count = self._backfill_symbol_range(
                        symbol=symbol,
                        start_ms=chunk_start_ms,
                        end_ms=chunk_end_ms,
                    )
                    total_inserted += count

                for timestamp_ms in range(chunk_start_ms, chunk_end_ms, 60000):
                    try:
                        current_minute_ms = (int(time.time() * 1000) // 60000) * 60000
                        publish_cutoff_ms = current_minute_ms - (
                            self.get_backfill_publish_lag_minutes() * 60000
                        )
                        if timestamp_ms >= publish_cutoff_ms:
                            continue

                        newest, oldest = self._query_kline_data_for_timestamp(
                            timestamp_ms
                        )
                        newest = newest or {}
                        if symbols_list and len(newest) < len(symbols_list):
                            missing_symbols = [
                                s for s in symbols_list if s not in newest
                            ]
                            synthetic_candles = self.build_backfill_synthetic_candles(
                                timestamp_ms=timestamp_ms,
                                missing_symbols=missing_symbols,
                            )
                            if synthetic_candles:
                                self._save_klines_batch(synthetic_candles)
                                newest = get_symbol_kline_data_at_timestamp(
                                    symbols=symbols_list,
                                    exchange=self.exchange,
                                    contract_type=self.contract_type,
                                    kline_timestamp_ms=timestamp_ms,
                                )

                            if len(newest) < len(symbols_list):
                                print(
                                    f"[{self.exchange}] WARN: Skipping incomplete backfill publish at {timestamp_ms}: "
                                    f"newest symbols {len(newest)}/{len(symbols_list)}"
                                )
                                continue

                        self._publish_kline_timestamp(
                            timestamp_ms,
                            source="backfill",
                            newest_values=newest,
                            oldest_values=oldest,
                        )
                    except Exception as e:
                        print(
                            f"[{self.exchange}] ERROR: Failed to publish backfill event: {e}"
                        )

            print(
                f"[{self.exchange}] Backfill complete: {total_inserted} klines inserted"
            )
        finally:
            self._backfill_in_progress = False
            self._flush_pending_timestamps()

    def save_kline(self, candle: NormalizedCandle, source: str = "live"):
        """Save kline - buffers live klines, saves backfill immediately."""
        timestamp_ms = candle.open_time_ms

        if source == "live":
            self._last_live_candle_wall_time = time.time()
            # Buffer live klines for batch saving
            self._kline_buffer.setdefault(timestamp_ms, []).append(candle)

            expected_count = len(self.symbols) if self.symbols else None
            # Flush older minutes even if a symbol never sent a candle. Downstream
            # services consume exact-timestamp payloads and treat missing symbols as NaN.
            self._flush_completed_minutes(
                timestamp_ms,
                expected_count=expected_count,
                force=True,
            )
            # Flush the current minute immediately only when it is complete.
            self._flush_completed_minutes(
                timestamp_ms + 1, expected_count=expected_count
            )
        else:
            # Backfill: save immediately (already complete data)
            self._save_klines_batch([candle])

    def _flush_completed_minutes(
        self,
        cutoff_timestamp_ms: int,
        expected_count: Optional[int] = None,
        force: bool = False,
    ):
        """Flush buffered minutes older than cutoff timestamp.

        If expected_count is provided, only flush minutes that have all symbols.
        When force=True, flush and publish even if the minute is incomplete.
        """
        completed = [ts for ts in self._kline_buffer if ts < cutoff_timestamp_ms]
        if not completed:
            return

        for ts in sorted(completed):
            candles = self._kline_buffer.get(ts)
            if not candles:
                continue

            symbol_count = len({c.symbol for c in candles})
            is_complete = expected_count is None or symbol_count >= expected_count
            if not is_complete and not force:
                continue

            candles = self._kline_buffer.pop(ts)
            self._save_klines_batch(candles)

            if not is_complete:
                self._log_throttled(
                    "flush_incomplete_live",
                    20,
                    f"[{self.exchange}][WARN] Flushing incomplete kline minute ts={ts}: "
                    f"{symbol_count}/{expected_count} symbols",
                )

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

        expected_count = len(self.symbols) if self.symbols else None
        max_ts = max(self._kline_buffer.keys())
        self._flush_completed_minutes(
            max_ts + 1,
            expected_count=expected_count,
            force=True,
        )

    def _is_live_stream_stale(self, ws_session_started: float) -> bool:
        if self._backfill_in_progress:
            return False

        last_live = self._last_live_candle_wall_time
        activity_time = (
            last_live
            if last_live is not None and last_live >= ws_session_started
            else ws_session_started
        )
        return time.time() - activity_time > LIVE_STALE_RECONNECT_SECONDS

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
            self._maybe_refresh_market_cap(force=True)

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
            ws_session_started = time.time()

            backfill_enabled = not self._should_disable_backfill()
            backfill_check_minute_ms = (int(time.time() * 1000) // 60000) * 60000
            post_rollover_gap_check_done = not backfill_enabled

            # On connect: detect gaps (via BTC) and backfill all symbols
            if backfill_enabled:
                self.backfill_gaps()

            # Wait for disconnect or symbol change
            last_symbol_check = time.time()
            poll_interval = 5
            while self.should_run:
                if not self.ws_thread or not self.ws_thread.is_alive():
                    break

                time.sleep(poll_interval)

                if backfill_enabled and not post_rollover_gap_check_done:
                    current_minute_ms = (int(time.time() * 1000) // 60000) * 60000
                    if current_minute_ms > backfill_check_minute_ms:
                        self.backfill_gaps()
                        post_rollover_gap_check_done = True

                if self._is_live_stream_stale(ws_session_started):
                    print(
                        f"[{self.exchange}][WARN] No live candles received for "
                        f"{LIVE_STALE_RECONNECT_SECONDS}s; reconnecting WebSocket"
                    )
                    self.close_websocket()
                    break

                # Check for symbol changes periodically
                if time.time() - last_symbol_check >= SYMBOL_CHECK_INTERVAL:
                    last_symbol_check = time.time()
                    old_symbols = self.symbols.copy()
                    self.update_symbols()
                    self._maybe_refresh_market_cap(force=True)

                    if self.symbols != old_symbols:
                        print(f"[{self.exchange}] Symbols changed, reconnecting...")
                        self.close_websocket()
                        break

            # Flush any buffered klines before reconnecting
            self._flush_all_buffered_klines()
            session_seconds = time.time() - ws_session_started
            print(
                f"[{self.exchange}] WebSocket session ended after {session_seconds:.1f}s "
                f"(last_published_ts={self._last_published_timestamp_ms}, "
                f"live_publishes={self._published_counts.get('live', 0)}, "
                f"backfill_publishes={self._published_counts.get('backfill', 0)}, "
                f"duplicate_dropped={self._dropped_duplicate_publishes})"
            )
            print(f"[{self.exchange}] Reconnecting in {WS_RECONNECT_DELAY}s...")
            time.sleep(WS_RECONNECT_DELAY)

    def stop(self):
        """Stop the collector."""
        self.should_run = False
        self._flush_all_buffered_klines()
        self.close_websocket()
