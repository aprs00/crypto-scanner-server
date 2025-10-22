import json
import time
import threading
import websocket
from datetime import datetime, timedelta
from enum import Enum
from urllib.error import URLError
from urllib.request import Request, urlopen

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
WS_PING_INTERVAL = 20
WS_PING_TIMEOUT = 10


class ContractType(str, Enum):
    PERPETUAL = "PERPETUAL"


class KlinesSocketManager:
    def __init__(self):
        self.ws_app = None
        self.ws_thread = None
        self.r = get_redis_connection()
        self.stream_name = None
        self.symbols_executed = set()
        self.message_batch = []
        self.symbols = []
        self.symbols_count = 0
        self.active_symbols_set = set()
        self.symbol_check_interval = 1800
        self.last_symbol_check = None
        self.reconnect_event = threading.Event()
        self.reconnect_lock = threading.Lock()
        self.shutdown_event = threading.Event()
        self._manual_close = False

    def store_error(self, error):
        self.r.execute_command(f"LPUSH error_log {str(error)}")

    def initialize(self):
        if websocket is None:
            raise RuntimeError(
                "websocket-client is required when python-binance is unavailable."
            )
        monitor_thread = threading.Thread(target=self.monitor_symbol_changes)
        monitor_thread.daemon = True
        monitor_thread.start()

    def stop(self):
        self._manual_close = True
        if self.ws_app is not None:
            try:
                self.ws_app.close()
            except Exception as exc:
                self.store_error(f"Error closing websocket: {exc}")
        if (
            self.ws_thread is not None
            and self.ws_thread.is_alive()
            and threading.current_thread() is not self.ws_thread
        ):
            self.ws_thread.join(timeout=5)
        self.ws_app = None
        self.ws_thread = None
        self.stream_name = None

    def reconnect(self):
        if not self.reconnect_lock.acquire(blocking=False):
            return
        try:
            self.stop()
            self.message_batch = []
            time.sleep(5)
            if not self.shutdown_event.is_set():
                self.start()
        finally:
            self.reconnect_lock.release()

    def fetch_futures_symbols(self):
        """Fetch all futures symbols from Binance API and return both list and set."""
        try:
            request = Request(
                BINANCE_FUTURES_EXCHANGE_INFO_URL,
                headers={"User-Agent": BINANCE_USER_AGENT},
            )
            with urlopen(request, timeout=10) as response:
                exchange_info = json.loads(response.read().decode("utf-8"))

            active_symbols = [
                symbol["symbol"]
                for symbol in exchange_info["symbols"]
                if symbol["contractType"] == ContractType.PERPETUAL.value.upper()
                and symbol["status"] == BinanceContractStatus.TRADING.value
            ]

            self.r.execute_command("DEL", "symbols:binance:perpetual")
            if active_symbols:
                self.r.execute_command(
                    "SADD", "symbols:binance:perpetual", *active_symbols
                )

            self.symbols = active_symbols
            self.symbols_count = len(self.symbols)

            new_active_set = set(active_symbols)
            should_reconnect = False

            if self.active_symbols_set:
                needs_reconnect = self.handle_symbol_changes(
                    self.active_symbols_set, new_active_set
                )
                if needs_reconnect:
                    self.active_symbols_set = new_active_set
                    should_reconnect = True

            self.active_symbols_set = new_active_set

            if should_reconnect:
                self.reconnect_event.set()

            self.last_symbol_check = datetime.now()

        except (URLError, ValueError, KeyError) as e:
            self.store_error(f"Error fetching futures symbols: {str(e)}")
            print(f"Error fetching symbols, falling back to default symbols: {str(e)}")
            self.symbols_count = len(self.symbols)
            return []
        except Exception as e:
            self.store_error(f"Unexpected error fetching futures symbols: {str(e)}")
            print(f"Unexpected error fetching symbols: {str(e)}")
            self.symbols_count = len(self.symbols)
            return []

    def handle_symbol_changes(self, old_symbols, new_symbols):
        """Handle added and removed symbols."""
        removed_symbols = old_symbols - new_symbols
        added_symbols = new_symbols - old_symbols

        if removed_symbols:
            print(f"Symbols removed from Binance: {removed_symbols}")
            self.handle_delisted_symbols(removed_symbols)
            return True

        if added_symbols:
            print(f"New symbols added to Binance: {added_symbols}")
            self.handle_new_symbols(added_symbols)
            return True

        return False

    def handle_delisted_symbols(self, symbols):
        """Handle delisted symbols - mark or delete from database."""
        for symbol in symbols:
            try:
                self.delete_symbol_data(symbol)

                self.r.sadd("delisted_symbols", symbol)
                self.r.set(f"delisted:{symbol}:timestamp", datetime.now().isoformat())

                self.r.publish(
                    RedisPubMessages.SYMBOL_DELISTED.value,
                    f"{symbol}:{datetime.now().isoformat()}",
                )

            except Exception as e:
                self.store_error(f"Error handling delisted symbol {symbol}: {str(e)}")

    def delete_symbol_data(self, symbol):
        """Delete symbol data from database - use with caution."""
        try:
            # Delete in batches to avoid locking
            # batch_size = 1000
            # while True:
            #     deleted = Kline.objects.filter(symbol=symbol)[:batch_size].delete()
            #     if deleted[0] == 0:
            #         break
            #     time.sleep(0.1)  # Small delay between batches

            print(f"Deleted all data for {symbol}")

        except Exception as e:
            self.store_error(f"Error deleting symbol {symbol}: {str(e)}")

    def handle_new_symbols(self, symbols):
        """Handle newly listed symbols."""
        for symbol in symbols:
            try:
                self.r.sadd("newly_listed_symbols", symbol)
                self.r.set(f"listed:{symbol}:timestamp", datetime.now().isoformat())

                self.r.publish(
                    RedisPubMessages.SYMBOL_ADDED.value,
                    f"{symbol}:{datetime.now().isoformat()}",
                )

            except Exception as e:
                self.store_error(f"Error handling new symbol {symbol}: {str(e)}")

    def monitor_symbol_changes(self):
        """Background thread to periodically check for symbol changes."""
        # Wait for initial start() to complete to avoid race condition
        time.sleep(5)

        while not self.shutdown_event.is_set():
            try:
                if self.reconnect_event.is_set():
                    self.reconnect_event.clear()
                    self.reconnect()
                    continue

                if (
                    self.last_symbol_check is None
                    or datetime.now() - self.last_symbol_check
                    > timedelta(seconds=self.symbol_check_interval)
                ):
                    self.fetch_futures_symbols()
            except Exception as e:
                self.store_error(f"Error in symbol monitor thread: {str(e)}")

            time.sleep(60)

    def start(self):
        self.fetch_futures_symbols()
        self._start_socket()

    def _start_socket(self):
        if websocket is None:
            print("websocket-client is not installed; cannot start stream")
            return

        if not self.symbols:
            print("No active symbols available to subscribe to Binance stream")
            return

        streams = [f"{symbol.lower()}@kline_1m" for symbol in self.symbols]
        stream_query = "/".join(streams)
        stream_url = f"{BINANCE_FUTURES_WS_URL}?streams={stream_query}"

        self.stream_name = stream_url
        self._manual_close = False

        self.ws_app = websocket.WebSocketApp(
            stream_url,
            on_message=self._on_ws_message,
            on_error=self._on_ws_error,
            on_close=self._on_ws_close,
        )

        self.ws_thread = threading.Thread(
            target=self.ws_app.run_forever,
            kwargs={"ping_interval": WS_PING_INTERVAL, "ping_timeout": WS_PING_TIMEOUT},
        )
        self.ws_thread.daemon = True
        self.ws_thread.start()

    def _on_ws_message(self, _ws, message):
        try:
            payload = json.loads(message)
        except json.JSONDecodeError as exc:
            self.store_error(f"Invalid JSON from Binance stream: {exc}")
            return

        self.handle_message(payload)

    def _on_ws_error(self, _ws, error):
        self.store_error(f"Websocket error: {error}")
        if not self._manual_close and not self.shutdown_event.is_set():
            self.reconnect_event.set()

    def _on_ws_close(self, _ws, close_status_code, close_msg):
        if not self._manual_close and not self.shutdown_event.is_set():
            print(
                f"Websocket closed unexpectedly: code={close_status_code}, msg={close_msg}"
            )
            self.reconnect_event.set()

    def check_reconnection_signal(self):
        """Check if reconnection has been signaled from another thread."""
        if self.reconnect_event.is_set() and not self.shutdown_event.is_set():
            print("Reconnection signal detected, executing reconnection")
            self.reconnect_event.clear()
            self.reconnect()
            return True
        return False

    def handle_message(self, msg):
        """Handle incoming websocket messages."""
        if self.check_reconnection_signal():
            return

        if self.is_message_error(msg):
            error_code = msg.get("code", "")
            error_msg = msg.get("msg", "")

            if "Invalid symbol" in error_msg or error_code == -1121:
                self.handle_invalid_symbol_error(msg)

            self.store_error(str(msg))
            self.reconnect()
            return

        msg_data = msg.get("data")
        if not msg_data:
            return
        kline_data = msg_data["k"]

        if kline_data.get("x"):
            self.message_batch.append(kline_data)

            if len(self.message_batch) == self.symbols_count:
                batch_copy = list(self.message_batch)
                self.message_batch = []
                thread = threading.Thread(
                    target=self._save_batch_sync, args=(batch_copy,)
                )
                thread.start()

                self.r.publish(
                    RedisPubMessages.KLINE_SAVED_TO_DB.value, kline_data["t"]
                )

    def handle_invalid_symbol_error(self, error_msg):
        """Handle specific invalid symbol errors."""
        try:
            error_str = str(error_msg)
            print("Invalid symbol error detected:", error_str)
            if "symbol" in error_str.lower():
                self.fetch_futures_symbols()

        except Exception as e:
            self.store_error(f"Error handling invalid symbol: {str(e)}")

    def _save_batch_sync(self, batch):
        try:
            models = [
                build_model_from_ws(
                    kline_dict,
                    exchange="binance",
                    contract_type=ContractType.PERPETUAL.value.lower(),
                )
                for kline_dict in batch
            ]

            if settings.STORE_TO_DB:
                bulk_insert_klines(models, chunk_size=len(models) or 1)
        except Exception as e:
            self.store_error(f"kline_batch_save_error: {e}")

    def main(self):
        self.initialize()
        self.start()
        try:
            while not self.shutdown_event.is_set():
                time.sleep(1)
        except KeyboardInterrupt:
            print("Keyboard interrupt received, shutting down Binance stream")
        finally:
            self.shutdown_event.set()
            self.stop()

    @staticmethod
    def is_message_error(msg):
        return "e" in msg and msg["e"] == "error"


def main():
    ksm = KlinesSocketManager()
    ksm.main()
