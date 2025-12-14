import json
import time
import threading
import gc
import numpy as np
import redis
import msgpack
from typing import Dict, List, Optional

from exchange_connections.constants import KLINE_FIELD_MAP
from exchange_connections.selectors import (
    get_exchange_symbols,
    get_historical_kline_data,
    get_symbol_kline_data,
)
from core.constants import RedisPubMessages, tf_options
from core.redis_config import get_redis_connection
from core.notifications import notification_service
from correlations.services.save_correlations import save_correlation_matrix_to_db
from correlations.db_utils import cleanup_old_correlation_data

class CorrelationTracker:
    """
    Maintains running statistics for O(1) correlation updates.

    For N symbols, stores:
    - sum_x[N]: sum of values per symbol
    - sum_xx[N]: sum of squared values per symbol
    - sum_xy[N,N]: sum of products for each pair
    - count: number of data points in window
    """

    sum_x: np.ndarray
    sum_xx: np.ndarray
    sum_xy: np.ndarray

    def __init__(self, window_size: int, n_symbols: int):
        self.window_size = window_size
        self.n_symbols = n_symbols
        self.count = 0
        self.sum_x = np.zeros(n_symbols, dtype=np.float64)
        self.sum_xx = np.zeros(n_symbols, dtype=np.float64)
        self.sum_xy = np.zeros((n_symbols, n_symbols), dtype=np.float64)

    def initialize(self, symbol_data: Dict[int, np.ndarray]):
        """Initialize from historical data keyed by symbol index."""
        if not symbol_data:
            return

        length = min(len(d) for d in symbol_data.values())
        if length == 0:
            return

        effective_length = min(length, self.window_size)

        # Build data matrix and compute sums
        for idx, data in symbol_data.items():
            segment = data[-effective_length:].astype(np.float64)
            self.sum_x[idx] = segment.sum()
            self.sum_xx[idx] = (segment * segment).sum()

        # Compute sum_xy via matrix multiplication
        arr = np.vstack(
            [
                symbol_data[i][-effective_length:].astype(np.float64)
                for i in range(self.n_symbols)
            ]
        )
        self.sum_xy = arr @ arr.T
        self.count = effective_length

    def update(self, new_vals: np.ndarray, old_vals: Optional[np.ndarray] = None):
        """Update running sums with new values, removing old if window full."""
        if self.count >= self.window_size and old_vals is not None:
            mask = ~np.isnan(old_vals)
            if mask.any():
                vals = old_vals[mask]
                self.sum_x[mask] -= vals
                self.sum_xx[mask] -= vals * vals
                self.sum_xy[np.ix_(mask, mask)] -= np.outer(vals, vals)
            self.count -= 1

        mask = ~np.isnan(new_vals)
        if mask.any():
            vals = new_vals[mask]
            self.sum_x[mask] += vals
            self.sum_xx[mask] += vals * vals
            self.sum_xy[np.ix_(mask, mask)] += np.outer(vals, vals)

        self.count = min(self.count + 1, self.window_size)

    def get_correlations(self) -> List[float]:
        """Return upper triangle of correlation matrix as flat list."""
        if self.count <= 1 or self.n_symbols <= 1:
            return []

        c = np.float64(self.count)
        means = self.sum_x / c
        var = (self.sum_xx / c) - means * means
        var = np.where(var <= 0, np.nan, var)

        cov = (self.sum_xy / c) - np.outer(means, means)
        denom = np.sqrt(np.outer(var, var))

        with np.errstate(invalid="ignore", divide="ignore"):
            corr = cov / denom

        corr = np.clip(corr, -1.0, 1.0)
        corr = np.nan_to_num(corr, nan=0.0)
        np.fill_diagonal(corr, 1.0)

        # Extract upper triangle
        i, j = np.triu_indices(self.n_symbols, k=1)
        return [float(x) for x in corr[i, j]]


class CorrelationCalculator:
    """Main correlation calculator with Redis pubsub integration."""

    def __init__(self):
        self.redis = get_redis_connection()
        self.lock = threading.RLock()
        self.symbols: List[str] = []
        self.symbol_to_idx: Dict[str, int] = {}
        self.hours_options: List[int] = []
        self.trackers: Dict[tuple, CorrelationTracker] = (
            {}
        )  # (hours, data_type) -> tracker
        self.initialized = False
        self.pending_updates: List[tuple] = []
        self.cleanup_stop = threading.Event()

    def _rebuild_indices(self):
        """Rebuild symbol index mapping."""
        self.symbol_to_idx = {s: i for i, s in enumerate(self.symbols)}

    def _init_trackers(self):
        """Initialize all correlation trackers from historical data."""
        n = len(self.symbols)
        print(f"Initializing correlations for {n} symbols ({n*(n-1)//2:,} pairs)")

        max_hours = max(self.hours_options)
        print(f"Fetching {max_hours}h of historical data...")

        start = time.time()
        all_data = get_historical_kline_data(hours=max_hours, symbols=self.symbols)
        print(f"Data fetch: {time.time() - start:.2f}s")

        for hours in sorted(self.hours_options, reverse=True):
            window = hours * 60

            for data_type in KLINE_FIELD_MAP:
                tracker = CorrelationTracker(window, n)

                indexed = {}
                for sym in self.symbols:
                    if sym in all_data and data_type in all_data[sym]:
                        idx = self.symbol_to_idx[sym]
                        data = np.asarray(all_data[sym][data_type], dtype=np.float64)
                        # Trim to window size for smaller timeframes
                        indexed[idx] = data[-window:] if len(data) > window else data

                if indexed:
                    tracker.initialize(indexed)

                self.trackers[(hours, data_type)] = tracker

            print(f"Initialized {hours}h timeframe")

        del all_data
        gc.collect()

    def _update_trackers(self, newest: Dict, oldest_by_hours: Dict[int, Dict]):
        """Update all trackers with new/old values."""
        n = len(self.symbols)

        for hours in self.hours_options:
            oldest = oldest_by_hours.get(hours, {})
            window = hours * 60

            for data_type in KLINE_FIELD_MAP:
                tracker = self.trackers.get((hours, data_type))
                if not tracker or tracker.n_symbols != n:
                    continue

                new_arr = np.full(n, np.nan, dtype=np.float64)
                old_arr = np.full(n, np.nan, dtype=np.float64)

                for sym, idx in self.symbol_to_idx.items():
                    if sym in newest and data_type in newest[sym]:
                        new_arr[idx] = newest[sym][data_type]
                    if (
                        tracker.count >= window
                        and sym in oldest
                        and data_type in oldest[sym]
                    ):
                        old_arr[idx] = oldest[sym][data_type]

                tracker.update(new_arr, old_arr if tracker.count >= window else None)

    def _cache_correlations(self, save_to_db: bool = True):
        """Cache correlation matrices to Redis and optionally DB."""
        pipe = self.redis.pipeline()

        for hours in self.hours_options:
            for data_type in KLINE_FIELD_MAP:
                tracker = self.trackers.get((hours, data_type))
                if not tracker:
                    continue

                matrix = tracker.get_correlations()
                key = f"correlations:{data_type}:{hours}:binance:perpetual"
                packed_data = msgpack.packb(matrix)
                if packed_data is not None:
                    pipe.set(key, packed_data)

                # Save 1h correlations to DB
                if save_to_db and hours == 1:
                    try:
                        save_correlation_matrix_to_db(
                            symbols=self.symbols,
                            correlation_matrix=matrix,
                            data_type=data_type,
                            hours=hours,
                            exchange="binance",
                            contract_type="perpetual",
                        )
                    except Exception as e:
                        print(f"DB save failed ({data_type}): {e}")

        pipe.execute()
        notification_service.send_correlation_update()

    def update_correlations(
        self,
        newest: Optional[Dict] = None,
        kline_timestamp_ms: Optional[int] = None,
        save_to_db: bool = True,
    ):
        """Main update method - fetch data, update trackers, cache results."""
        with self.lock:
            # Get newest values if not provided
            if newest is None:
                newest = get_symbol_kline_data(
                    symbols=self.symbols,
                    exchange="binance",
                    contract_type="perpetual",
                )

            # Validate we have data for all symbols
            missing = [s for s in self.symbols if s not in newest]
            if missing:
                try:
                    extra = get_symbol_kline_data(
                        symbols=missing,
                        exchange="binance",
                        contract_type="perpetual",
                    )
                    newest.update(extra)
                except Exception as e:
                    print(f"Failed to fetch missing symbols: {e}")
                    return

            # Fetch oldest values for each timeframe
            oldest_by_hours = {}
            for hours in self.hours_options:
                oldest_by_hours[hours] = get_symbol_kline_data(
                    symbols=self.symbols,
                    exchange="binance",
                    contract_type="perpetual",
                    hours=hours,
                    kline_timestamp_ms=kline_timestamp_ms,
                )

            self._update_trackers(newest, oldest_by_hours)
            self._cache_correlations(save_to_db)

    def add_symbol(self, symbol: str):
        """Add new symbol to tracking."""
        with self.lock:
            if symbol in self.symbols:
                return

            available = get_exchange_symbols()
            if symbol not in available:
                print(f"Symbol {symbol} not available")
                return

            min_points = min(self.hours_options) * 60
            data = get_historical_kline_data(
                hours=max(self.hours_options), symbols=[symbol]
            )

            if symbol not in data:
                print(f"No data for {symbol}")
                return

            points = min(len(data[symbol].get(dt, [])) for dt in KLINE_FIELD_MAP)
            if points < min_points:
                print(f"Limited data for {symbol}: {points}/{min_points}, adding anyway")

            print(f"Adding {symbol}")
            self.symbols = available
            self._rebuild_indices()
            self._init_trackers()
            self.update_correlations()

    def remove_symbol(self, symbol: str):
        """Remove symbol from tracking."""
        with self.lock:
            if symbol not in self.symbols:
                return

            print(f"Removing {symbol}")
            idx = self.symbol_to_idx[symbol]
            self.symbols = get_exchange_symbols()
            self._rebuild_indices()

            # Remove from all trackers
            for tracker in self.trackers.values():
                tracker.sum_x = np.delete(tracker.sum_x, idx)
                tracker.sum_xx = np.delete(tracker.sum_xx, idx)
                tracker.sum_xy = np.delete(tracker.sum_xy, idx, axis=0)
                tracker.sum_xy = np.delete(tracker.sum_xy, idx, axis=1)
                tracker.n_symbols = len(self.symbols)

    def _cleanup_loop(self):
        """Background cleanup of old correlation data."""
        while not self.cleanup_stop.wait(timeout=900):
            try:
                cleanup_old_correlation_data(retention_hours=4)
            except Exception as e:
                print(f"Cleanup failed: {e}")

    def _pubsub_loop(self):
        """Listen for Redis pubsub messages."""
        channels = [
            RedisPubMessages.KLINE_SAVED_TO_DB.value,
            RedisPubMessages.SYMBOL_ADDED.value,
            RedisPubMessages.SYMBOL_DELISTED.value,
        ]

        while True:
            try:
                pubsub = self.redis.pubsub()
                pubsub.subscribe(*channels)
                pubsub.get_message()

                for msg in pubsub.listen():
                    if msg["type"] != "message":
                        continue

                    channel = msg["channel"]
                    data = msg.get("data", b"")

                    if isinstance(data, bytes):
                        data = data.decode("utf-8")

                    if channel == RedisPubMessages.KLINE_SAVED_TO_DB.value:
                        self._handle_kline_update(data)
                    elif channel == RedisPubMessages.SYMBOL_ADDED.value:
                        self.add_symbol(data.split(":")[0])
                    elif channel == RedisPubMessages.SYMBOL_DELISTED.value:
                        self.remove_symbol(data.split(":")[0])

            except (redis.ConnectionError, redis.TimeoutError) as e:
                print(f"Redis error: {e}, reconnecting...")
                time.sleep(5)
            except Exception as e:
                print(f"Pubsub error: {e}")
                time.sleep(5)

    def _handle_kline_update(self, data: str):
        """Process kline update message."""
        try:
            payload = json.loads(data)
        except json.JSONDecodeError:
            return

        newest = payload.get("newest_values")
        timestamp = payload.get("timestamp")

        if not isinstance(newest, dict):
            return

        if not self.initialized:
            self.pending_updates.append((newest, timestamp))
            print(f"Queued update (pending: {len(self.pending_updates)})")
            return

        start = time.time()
        self.update_correlations(newest, timestamp)
        print(f"Update completed in {time.time() - start:.2f}s")

    def run(self):
        """Main entry point."""
        self.hours_options = list(tf_options["correlation"].values())
        self.symbols = get_exchange_symbols()
        self._rebuild_indices()

        pubsub_thread = threading.Thread(target=self._pubsub_loop, daemon=True)
        pubsub_thread.start()

        cleanup_thread = threading.Thread(target=self._cleanup_loop, daemon=True)
        cleanup_thread.start()

        time.sleep(2)

        print("Initializing correlation trackers...")
        start = time.time()
        self._init_trackers()
        print(f"Initialization completed in {time.time() - start:.2f}s")

        self.initialized = True

        if self.pending_updates:
            print(f"Processing {len(self.pending_updates)} pending updates...")
            for newest, ts in self.pending_updates:
                self.update_correlations(newest, ts, save_to_db=False)
            self.pending_updates.clear()

        print("Ready for real-time updates")

        try:
            pubsub_thread.join()
        except KeyboardInterrupt:
            print("Shutting down...")
            self.cleanup_stop.set()
