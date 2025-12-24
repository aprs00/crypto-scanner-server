import json
import time
import threading
import websocket
from datetime import datetime, timedelta
from enum import Enum
from urllib.request import Request, urlopen
from typing import Set, List, Dict, Optional
from dataclasses import dataclass, field

from django.conf import settings

from exchange_connections.constants import BinanceContractStatus
from core.constants import RedisPubMessages
from exchange_connections.services.klines_ingest import (
    build_model_from_ws,
    bulk_insert_klines,
)
from core.redis_config import get_redis_connection


BINANCE_FUTURES_EXCHANGE_INFO_URL = "https://fapi.binance.com/fapi/v1/exchangeInfo"
BINANCE_FUTURES_WS_URL = "wss://fstream.binance.com/stream"
BINANCE_USER_AGENT = "crypto-scanner/1.0"
COINGECKO_MARKET_CAP_URL = (
    "https://api.coingecko.com/api/v3/coins/markets"
    "?vs_currency=usd&order=market_cap_desc&per_page={per_page}&page=1&sparkline=false"
)
WS_PING_INTERVAL = 20
WS_PING_TIMEOUT = 30
MAX_STREAMS_PER_CONNECTION = 200
RECONNECT_BASE_DELAY = 5
RECONNECT_MAX_DELAY = 60
WS_24H_RECONNECT_INTERVAL = 23 * 60 * 60
STALE_CONNECTION_THRESHOLD = 120
MARKET_CAP_ZSET_KEY = "market_cap:binance:perpetual"
SYMBOLS_REDIS_KEY = "symbols:binance:perpetual"


class ContractType(str, Enum):
    PERPETUAL = "PERPETUAL"


@dataclass
class SymbolEvent:
    """Represents a symbol listing/delisting event."""

    symbol: str
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())


class KlinesSocketManager:
    def __init__(self):
        self.r = get_redis_connection()
        self.symbols: List[str] = []
        self.active_symbols_set: Set[str] = set()

        self.ws_apps: List[websocket.WebSocketApp] = []
        self.ws_threads: List[threading.Thread] = []
        self._last_pong_time: Dict[int, float] = {}
        self._connection_start_time: Optional[float] = None
        self._manual_close = False
        self._reconnect_count = 0

        self.message_batch: List[Dict] = []
        self.message_lock = threading.Lock()
        self.batch_timestamp: Optional[int] = None

        self.reconnect_event = threading.Event()
        self.reconnect_lock = threading.Lock()
        self.shutdown_event = threading.Event()

        self.symbol_check_interval = 1800
        self.last_symbol_check: Optional[datetime] = None

    @property
    def symbols_count(self) -> int:
        return len(self.symbols)

    def store_error(self, error: str):
        self.r.lpush("error_log", str(error))

    def initialize(self):
        if websocket is None:
            raise RuntimeError("websocket-client is required")

        monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        monitor_thread.start()

    def start(self):
        self.fetch_futures_symbols()
        self._start_socket()

    def stop(self):
        self._manual_close = True
        self._close_all_websockets()
        self._reset_websocket_state()

    def _close_all_websockets(self):
        current_thread = threading.current_thread()

        for ws_app in self.ws_apps:
            try:
                ws_app.close()
            except Exception as e:
                self.store_error(f"Error closing websocket: {e}")

        for ws_thread in self.ws_threads:
            if ws_thread.is_alive() and ws_thread is not current_thread:
                try:
                    ws_thread.join(timeout=5)
                except Exception as e:
                    self.store_error(f"Error joining thread: {e}")

    def _reset_websocket_state(self):
        self.ws_apps = []
        self.ws_threads = []
        self.message_batch = []
        self.batch_timestamp = None
        self._last_pong_time = {}

    def reconnect(self):
        if not self.reconnect_lock.acquire(blocking=False):
            return

        try:
            seconds_in_minute = datetime.now().second
            if seconds_in_minute < 7:
                wait_time = 7 - seconds_in_minute
                print(f"Waiting {wait_time}s to avoid socket update window")
                time.sleep(wait_time)

            self.stop()
            delay = min(
                RECONNECT_BASE_DELAY * (2**self._reconnect_count), RECONNECT_MAX_DELAY
            )
            self._reconnect_count += 1
            print(f"Reconnecting in {delay}s (attempt {self._reconnect_count})")
            time.sleep(delay)

            if not self.shutdown_event.is_set():
                self.start()
        finally:
            self.reconnect_lock.release()

    def fetch_futures_symbols(self):
        """Fetch all perpetual futures symbols from Binance."""
        try:
            exchange_info = self._fetch_json(BINANCE_FUTURES_EXCHANGE_INFO_URL)
            active_symbols = [
                s["symbol"]
                for s in exchange_info["symbols"]
                if s["contractType"] == ContractType.PERPETUAL.value
                and s["status"] == BinanceContractStatus.TRADING.value
            ]

            self._update_symbols_in_redis(active_symbols)
            self._check_symbol_changes(set(active_symbols))
            self.store_top_market_cap_symbols(limit=100)
            self.last_symbol_check = datetime.now()

        except Exception as e:
            self.store_error(f"Error fetching futures symbols: {e}")

    def _fetch_json(self, url: str) -> Dict:
        """Fetch and parse JSON from a URL."""
        request = Request(url, headers={"User-Agent": BINANCE_USER_AGENT})
        with urlopen(request, timeout=10) as response:
            return json.loads(response.read().decode("utf-8"))

    def _update_symbols_in_redis(self, symbols: List[str]):
        """Update the symbols set in Redis."""
        self.r.delete(SYMBOLS_REDIS_KEY)
        if symbols:
            self.r.sadd(SYMBOLS_REDIS_KEY, *symbols)
        self.symbols = symbols

    def _check_symbol_changes(self, new_symbols: Set[str]):
        """Check for symbol changes and trigger reconnect if needed."""
        if not self.active_symbols_set:
            self.active_symbols_set = new_symbols
            return

        removed = self.active_symbols_set - new_symbols
        added = new_symbols - self.active_symbols_set

        if removed:
            print(f"Symbols removed: {removed}")
            self._publish_symbol_events(removed, is_delisting=True)

        if added:
            print(f"Symbols added: {added}")
            self._publish_symbol_events(added, is_delisting=False)

        self.active_symbols_set = new_symbols

        if removed or added:
            self.reconnect_event.set()

    def _publish_symbol_events(self, symbols: Set[str], is_delisting: bool):
        """Publish symbol listing/delisting events to Redis."""
        if is_delisting:
            redis_set = "delisted_symbols"
            key_prefix = "delisted"
            channel = RedisPubMessages.SYMBOL_DELISTED.value
        else:
            redis_set = "newly_listed_symbols"
            key_prefix = "listed"
            channel = RedisPubMessages.SYMBOL_ADDED.value

        for symbol in symbols:
            try:
                event = SymbolEvent(symbol)
                self.r.sadd(redis_set, symbol)
                self.r.set(f"{key_prefix}:{symbol}:timestamp", event.timestamp)
                self.r.publish(channel, f"{symbol}:{event.timestamp}")
            except Exception as e:
                self.store_error(f"Error publishing event for {symbol}: {e}")

    def store_top_market_cap_symbols(self, limit: int = 100):
        """Fetch and store top market cap symbols in Redis sorted set."""
        if not self.symbols or limit <= 0:
            return

        try:
            per_page = min(limit * 2, 250)
            market_data = self._fetch_json(
                COINGECKO_MARKET_CAP_URL.format(per_page=per_page)
            )
        except Exception as e:
            self.store_error(f"Error fetching market cap data: {e}")
            return

        symbol_set = set(self.symbols)
        zadd_args = []

        for asset in market_data:
            if not isinstance(asset, dict):
                continue

            asset_symbol = asset.get("symbol")
            market_cap = asset.get("market_cap")
            if not asset_symbol or market_cap is None:
                continue

            binance_symbol = f"{asset_symbol.upper()}USDT"
            if binance_symbol in symbol_set:
                zadd_args.extend([float(market_cap), binance_symbol])
                if len(zadd_args) >= limit * 2:
                    break

        if zadd_args:
            pipe = self.r.pipeline()
            pipe.delete(MARKET_CAP_ZSET_KEY)
            pipe.zadd(MARKET_CAP_ZSET_KEY, dict(zip(zadd_args[1::2], zadd_args[::2])))
            pipe.execute()

    def _start_socket(self):
        if not self.symbols:
            print("No symbols available for subscription")
            return

        self._manual_close = False
        self._connection_start_time = time.time()

        for idx, symbol_chunk in enumerate(
            self._chunk_list(self.symbols, MAX_STREAMS_PER_CONNECTION)
        ):
            streams = "/".join(f"{s.lower()}@kline_1m" for s in symbol_chunk)
            url = f"{BINANCE_FUTURES_WS_URL}?streams={streams}"

            ws_app = websocket.WebSocketApp(
                url,
                on_message=self._on_message,
                on_error=self._on_error,
                on_close=self._on_close,
                on_open=lambda ws, i=idx: self._on_open(ws, i),
                on_pong=lambda ws, msg, i=idx: self._on_pong(ws, msg, i),
            )
            self.ws_apps.append(ws_app)

            thread = threading.Thread(
                target=ws_app.run_forever,
                kwargs={
                    "ping_interval": WS_PING_INTERVAL,
                    "ping_timeout": WS_PING_TIMEOUT,
                },
                daemon=True,
            )
            self.ws_threads.append(thread)
            thread.start()

    @staticmethod
    def _chunk_list(lst: List, size: int):
        """Yield successive chunks from a list."""
        for i in range(0, len(lst), size):
            yield lst[i : i + size]

    def _on_open(self, _ws, idx: int):
        print(f"WebSocket {idx} connected")
        self._last_pong_time[idx] = time.time()
        self._reconnect_count = 0

    def _on_pong(self, _ws, _msg, idx: int):
        self._last_pong_time[idx] = time.time()

    def _on_error(self, _ws, error):
        self.store_error(f"WebSocket error: {error}")
        if not self._manual_close and not self.shutdown_event.is_set():
            self.reconnect_event.set()

    def _on_close(self, _ws, code, msg):
        if not self._manual_close and not self.shutdown_event.is_set():
            print(f"WebSocket closed: code={code}, msg={msg}")
            self.reconnect_event.set()

    def _on_message(self, _ws, message: str):
        try:
            payload = json.loads(message)
        except json.JSONDecodeError as e:
            self.store_error(f"Invalid JSON: {e}")
            return

        if self._is_error_message(payload):
            self.store_error(str(payload))
            self.reconnect()
            return

        self._handle_kline_message(payload)

    @staticmethod
    def _is_error_message(msg: Dict) -> bool:
        return msg.get("e") == "error"

    def _handle_kline_message(self, msg: Dict):
        """Process incoming kline data."""
        kline = msg.get("data", {}).get("k")

        if not kline or not kline.get("x"):
            return

        batch_to_process = self._add_to_batch(kline)

        if batch_to_process:
            threading.Thread(
                target=self._save_batch,
                args=(batch_to_process,),
                daemon=True,
            ).start()

    def _add_to_batch(self, kline: Dict) -> Optional[List[Dict]]:
        """Add kline to batch, return batch if complete."""
        kline_ts = kline.get("t")

        with self.message_lock:
            if self.batch_timestamp is not None and kline_ts != self.batch_timestamp:
                if self.message_batch:
                    print(
                        f"Discarding {len(self.message_batch)} klines from ts {self.batch_timestamp}, new ts {kline_ts}"
                    )
                self.message_batch = []

            self.batch_timestamp = kline_ts
            self.message_batch.append(kline)

            if len(self.message_batch) >= self.symbols_count:
                batch = self.message_batch
                self.message_batch = []
                self.batch_timestamp = None

                # Check for duplicates before returning
                self._check_batch_duplicates(batch)

                return batch

        return None

    def _check_batch_duplicates(self, batch: List[Dict]):
        """Check for duplicate symbols in batch and log errors."""
        symbols_in_batch = [k.get("s") for k in batch]
        seen = set()
        duplicates = []

        for sym in symbols_in_batch:
            if sym in seen:
                duplicates.append(sym)
            seen.add(sym)

        if duplicates:
            dup_counts = {}
            for sym in symbols_in_batch:
                dup_counts[sym] = dup_counts.get(sym, 0) + 1
            dup_details = {sym: cnt for sym, cnt in dup_counts.items() if cnt > 1}

            error_msg = f"[DUPLICATE ERROR] Batch has {len(duplicates)} duplicate symbols! Details: {dup_details}"
            print(error_msg)
            self.store_error(error_msg)

        # Also check if batch has fewer unique symbols than expected
        unique_count = len(seen)
        if unique_count != self.symbols_count:
            error_msg = f"[BATCH SIZE MISMATCH] Expected {self.symbols_count} unique symbols, got {unique_count}"
            print(error_msg)
            self.store_error(error_msg)

    def _save_batch(self, batch: List[Dict]):
        """Save a batch of klines to the database."""
        try:
            models = [
                build_model_from_ws(
                    kline,
                    exchange="binance",
                    contract_type=ContractType.PERPETUAL.value.lower(),
                )
                for kline in batch
            ]

            if settings.STORE_TO_DB:
                bulk_insert_klines(models, chunk_size=len(models))

            payload = json.dumps(
                {
                    "timestamp": batch[0]["t"],
                    "newest_values": {
                        k["s"]: {
                            "price": float(k["c"]),
                            "volume": float(k["v"]),
                            "trades": float(k["n"]),
                        }
                        for k in batch
                    },
                }
            )
            self.r.publish(RedisPubMessages.KLINE_SAVED_TO_DB.value, payload)

        except Exception as e:
            self.store_error(f"Batch save error: {e}")

    def _monitor_loop(self):
        """Background monitoring for reconnects and symbol changes."""
        time.sleep(5)

        while not self.shutdown_event.is_set():
            try:
                self._check_and_handle_reconnect()
                self._check_symbol_refresh()
            except Exception as e:
                self.store_error(f"Monitor error: {e}")

            time.sleep(60)

    def _check_and_handle_reconnect(self):
        """Check various reconnect conditions."""
        # Handle pending reconnect event
        if self.reconnect_event.is_set():
            self.reconnect_event.clear()
            self.reconnect()
            return

        # Proactive 24h reconnect
        if self._connection_start_time:
            if time.time() - self._connection_start_time > WS_24H_RECONNECT_INTERVAL:
                print("Proactive 24h reconnect")
                self._reconnect_count = 0
                self.reconnect()
                return

        # Stale connection check
        current_time = time.time()
        for idx, last_pong in self._last_pong_time.items():
            if current_time - last_pong > STALE_CONNECTION_THRESHOLD:
                print(f"Connection {idx} stale, reconnecting")
                self.reconnect_event.set()
                return

    def _check_symbol_refresh(self):
        """Periodically refresh symbols list."""
        should_refresh = (
            self.last_symbol_check is None
            or datetime.now() - self.last_symbol_check
            > timedelta(seconds=self.symbol_check_interval)
        )
        if should_refresh:
            self.fetch_futures_symbols()

    def main(self):
        """Main entry point."""
        self.initialize()
        self.start()

        try:
            while not self.shutdown_event.is_set():
                time.sleep(1)
        except KeyboardInterrupt:
            print("Shutting down...")
        finally:
            self.shutdown_event.set()
            self.stop()


def main():
    KlinesSocketManager().main()
