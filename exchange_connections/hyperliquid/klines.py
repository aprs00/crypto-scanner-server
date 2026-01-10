import json
import time
import threading
import requests
import websocket
from decimal import Decimal
from typing import Set, Optional

from exchange_connections.base import BaseKlineCollector
from exchange_connections.candle_types import NormalizedCandle
from core.constants import Exchange

HYPERLIQUID_WS_URL = "wss://api.hyperliquid.xyz/ws"
HYPERLIQUID_INFO_URL = "https://api.hyperliquid.xyz/info"
WS_PING_INTERVAL = 0
WS_PING_TIMEOUT = None
PONG_TIMEOUT_SECONDS = 90
MAX_CONNECTION_DURATION_SECONDS = 3600  # Reconnect every 1 hour to avoid HL disconnects


class HyperliquidKlineCollector(BaseKlineCollector):
    """
    Collects 1-minute klines from Hyperliquid perpetual futures.

    Unlike Binance (which sends closed candles), Hyperliquid streams updates
    on every trade. We accumulate these until the minute changes, then flush
    all candles together and immediately save to DB.
    """

    def __init__(self):
        super().__init__(exchange=Exchange.HYPERLIQUID, contract_type="perpetual")
        self.ws: Optional[websocket.WebSocketApp] = None
        self.ws_connected = False
        self.generate_synthetic_candles = True
        self.pending_candles: dict[str, dict] = {}
        self.pending_lock = threading.Lock()
        self.connection_start_time = 0
        self.last_heartbeat_time = 0
        self.last_pong_time = 0
        self.heartbeat_count = 0

    def fetch_perpetual_symbols(self) -> Set[str]:
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
            self.log_error(f"Failed to fetch symbols: {e}")
            return set()

    def normalize_candle(self, raw_data: dict) -> Optional[NormalizedCandle]:
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
            self.log_error(f"Failed to normalize candle: {e}")
            return None

    def map_coingecko_symbol(self, coingecko_symbol: str) -> Optional[str]:
        return coingecko_symbol

    def _on_message(self, ws, message):
        try:
            data = json.loads(message)
            if data.get("channel") == "pong":
                print("PONG ")
                self.last_pong_time = time.time()
                if self.heartbeat_count % 10 == 0:
                    elapsed = int(time.time() - self.connection_start_time)
                    print(
                        f"[hyperliquid] Pong received (uptime: {elapsed}s, heartbeats: {self.heartbeat_count})"
                    )
            elif data.get("channel") == "candle" and data.get("data"):
                self._handle_candle(data["data"])
        except Exception as e:
            self.log_error(f"Message error: {e}")

    def _handle_candle(self, candle_data: dict):
        """Accumulate candle updates, save to DB when minute changes."""
        symbol = candle_data.get("s")
        if not symbol:
            return

        candle_minute = int(candle_data.get("t", 0))

        if candle_minute <= self.last_processed_timestamp:
            return

        with self.pending_lock:
            if self.pending_candles:
                prev_minute = int(next(iter(self.pending_candles.values()))["t"])
                if candle_minute > prev_minute:
                    self._save_candles_locked(prev_minute)
                elif candle_minute < prev_minute:
                    return

            self.pending_candles[symbol] = candle_data

    def _save_candles(self, minute_ts: int):
        """Send candles to base class and force immediate DB save (acquires lock)."""
        with self.pending_lock:
            self._save_candles_locked(minute_ts)

    def _save_candles_locked(self, minute_ts: int):
        """Send candles to base class. Must be called while holding pending_lock."""
        if not self.pending_candles:
            return

        for raw in self.pending_candles.values():
            candle = self.normalize_candle(raw)
            if candle:
                self.process_kline(candle)

        count = len(self.pending_candles)
        self.pending_candles.clear()

        self._process_batch(minute_ts)
        print(f"[hyperliquid] Saved {count} candles for ts={minute_ts}")

    def _on_error(self, ws, error):
        self.log_error(f"WebSocket error: {error}")

    def _on_close(self, ws, code, msg):
        self.ws_connected = False
        duration = (
            int(time.time() - self.connection_start_time)
            if self.connection_start_time
            else 0
        )
        with self.pending_lock:
            self.pending_candles.clear()
        print(
            f"[hyperliquid] WebSocket closed: code={code}, msg={msg}, duration={duration}s, heartbeats_sent={self.heartbeat_count}"
        )

    def _on_open(self, ws):
        self.ws_connected = True
        self.connection_start_time = time.time()
        self.last_pong_time = time.time()  # Initialize pong time on connect
        self.heartbeat_count = 0
        print("[hyperliquid] WebSocket connected")
        threading.Thread(target=self._setup, daemon=True).start()
        threading.Thread(target=self._heartbeat_loop, daemon=True).start()

    def _setup(self):
        """Subscribe to symbols and start background checker."""
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
                self.log_error(f"Subscribe failed for {symbol}: {e}")
        print("[hyperliquid] Subscribed")

        if not self.last_prices:
            self._fetch_initial_prices()
        else:
            print(
                f"[hyperliquid] Skipping initial price fetch, already have {len(self.last_prices)} prices"
            )
        self._backfill_gaps()
        self._run_stale_checker()

    def _heartbeat_loop(self):
        """Send JSON heartbeat every 50s to prevent 60s server timeout (matches official SDK)."""
        while self.ws_connected:
            time.sleep(50)
            if self.ws_connected and self.ws:
                # Proactive reconnect every hour to avoid unexpected HL disconnects
                connection_duration = time.time() - self.connection_start_time
                if connection_duration >= MAX_CONNECTION_DURATION_SECONDS:
                    current_second = time.time() % 60
                    if current_second < 10:
                        wait_time = 10 - current_second
                    else:
                        wait_time = 70 - current_second  # Wait until next minute + 10s
                    print(
                        f"[hyperliquid] Connection duration {connection_duration:.0f}s, "
                        f"scheduling reconnect in {wait_time:.0f}s (at 10s mark)"
                    )
                    time.sleep(wait_time)
                    print(
                        f"[hyperliquid] Proactive reconnect after {int(time.time() - self.connection_start_time)}s"
                    )
                    self.ws.close()
                    break

                # Check for stale connection (no pong received recently)
                time_since_pong = time.time() - self.last_pong_time
                if time_since_pong > PONG_TIMEOUT_SECONDS:
                    current_second = time.time() % 60
                    if current_second > 20:
                        wait_time = 60 - current_second + 1
                        print(
                            f"[hyperliquid] Delaying reconnect by {wait_time:.0f}s to preserve minute boundary"
                        )
                        time.sleep(wait_time)

                    # Save pending candles before disconnect
                    with self.pending_lock:
                        if self.pending_candles:
                            prev_minute = int(
                                next(iter(self.pending_candles.values()))["t"]
                            )
                            self._save_candles_locked(prev_minute)

                    self.log_error(
                        f"No pong received in {time_since_pong:.0f}s, forcing reconnect"
                    )
                    self.ws.close()
                    break

                try:
                    self.ws.send(json.dumps({"method": "ping"}))
                    self.heartbeat_count += 1
                    self.last_heartbeat_time = time.time()
                    if self.heartbeat_count % 20 == 0:
                        elapsed = int(time.time() - self.connection_start_time)
                        print(
                            f"[hyperliquid] Heartbeat #{self.heartbeat_count} sent (uptime: {elapsed}s / {elapsed//60}m)"
                        )
                except Exception as e:
                    self.log_error(f"Heartbeat failed: {e}")

    def _fetch_initial_prices(self):
        """Bootstrap last_prices for synthetic candle generation."""
        print("[hyperliquid] Fetching initial prices...")
        end_ms = int(time.time() * 1000)
        start_ms = end_ms - 3600000

        count = 0
        for symbol in self.symbols:
            try:
                resp = requests.post(
                    HYPERLIQUID_INFO_URL,
                    headers={"Content-Type": "application/json"},
                    json={
                        "type": "candleSnapshot",
                        "req": {
                            "coin": symbol,
                            "interval": "1m",
                            "startTime": start_ms,
                            "endTime": end_ms,
                        },
                    },
                    timeout=10,
                )
                resp.raise_for_status()
                candles = resp.json()
                if candles:
                    self.last_prices[symbol] = Decimal(str(candles[-1]["c"]))
                    count += 1
                time.sleep(0.04)
            except Exception:
                pass
        print(f"[hyperliquid] Initialized {count} prices")

    def _backfill_gaps(self):
        """Detect and backfill any gaps in kline data after reconnection."""
        print("[hyperliquid] Checking for gaps to backfill...")
        print("hyperliquid] last_processed_timestamp:", self.last_processed_timestamp)

        if self.last_processed_timestamp == 0:
            return

        current_time_ms = int(time.time() * 1000)
        current_minute_ms = (current_time_ms // 60000) * 60000
        expected_next_minute = self.last_processed_timestamp + 60000

        if expected_next_minute >= current_minute_ms:
            return

        gap_minutes = (current_minute_ms - expected_next_minute) // 60000
        if gap_minutes == 0:
            return

        print(
            f"[hyperliquid] Detected gap: {gap_minutes} missing minutes after ts={self.last_processed_timestamp}"
        )

        timestamps_to_fill = []
        ts = expected_next_minute
        while ts < current_minute_ms:
            timestamps_to_fill.append(ts)
            ts += 60000

        for timestamp_ms in timestamps_to_fill:
            print(f"[hyperliquid] Backfilling ts={timestamp_ms}...")
            candles_by_symbol = {}

            for symbol in self.symbols:
                if not self.ws_connected:
                    print("[hyperliquid] Backfill interrupted - WebSocket disconnected")
                    return

                try:
                    resp = requests.post(
                        HYPERLIQUID_INFO_URL,
                        headers={"Content-Type": "application/json"},
                        json={
                            "type": "candleSnapshot",
                            "req": {
                                "coin": symbol,
                                "interval": "1m",
                                "startTime": timestamp_ms,
                                "endTime": timestamp_ms + 60000,
                            },
                        },
                        timeout=10,
                    )
                    resp.raise_for_status()
                    candles = resp.json()
                    if candles:
                        raw_candle = candles[0]
                        normalized = self.normalize_candle(raw_candle)
                        if normalized:
                            candles_by_symbol[symbol] = normalized
                    time.sleep(0.04)
                except Exception as e:
                    self.log_error(
                        f"Backfill failed for {symbol} at ts={timestamp_ms}: {e}"
                    )

            if candles_by_symbol:
                print(
                    f"[hyperliquid] Backfilled {len(candles_by_symbol)} candles for ts={timestamp_ms}"
                )
                for candle in candles_by_symbol.values():
                    self.process_kline(candle)

        print(f"[hyperliquid] Gap backfill completed")

    def _run_stale_checker(self):
        """Flush pending candles if their minute has passed."""
        while self.ws_connected:
            time.sleep(5)
            with self.pending_lock:
                if self.pending_candles:
                    current_min = int(time.time() * 1000) // 60000 * 60000
                    pending_min = int(next(iter(self.pending_candles.values()))["t"])
                    if pending_min < current_min:
                        self._save_candles_locked(pending_min)

    def connect_websocket(self) -> Optional[threading.Thread]:
        if not self.symbols:
            return None

        print("[hyperliquid] Connecting...")
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

    def on_symbols_changed(self):
        if self.ws:
            self.ws.close()


def main():
    collector = HyperliquidKlineCollector()
    try:
        collector.run()
    except KeyboardInterrupt:
        print("Shutting down...")
        collector.stop()


if __name__ == "__main__":
    main()
