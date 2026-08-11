from __future__ import annotations

import importlib
import logging
import queue
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass, replace
from datetime import datetime, timezone

from django.db import close_old_connections

from footprints.models import FootprintMarket, InstrumentSpec
from footprints.specs import load_runtime_instruments
from footprints.storage.clickhouse import (
    ClickHouseAccess,
    ClickHouseExecutionStore,
    ClickHouseFailureKind,
    ClickHouseSettings,
    ExecutionIdentityConflict,
    ExecutionIdentitySignature,
    RecentExecutionIdentity,
    classify_clickhouse_error,
    create_clickhouse_client,
)
from footprints.storage.clickhouse_schema import ensure_clickhouse_schema
from footprints.storage.executions import StoredExecution
from footprints.types import NormalizedTrade, RuntimeInstrument, TakerSide


logger = logging.getLogger(__name__)

ADAPTER_MODULES = {
    "binance": "footprints.collectors.binance",
    "bybit": "footprints.collectors.bybit",
    "hyperliquid": "footprints.collectors.hyperliquid",
}

_MAX_SOURCE_EPOCH = 2**32 - 1
QUEUE_SIZE = 100_000
COMMIT_BATCH_SIZE = 500
COMMIT_INTERVAL_SECONDS = 0.25
RECONNECT_DELAY_SECONDS = 1.0
RETRY_INITIAL_SECONDS = 0.25
RETRY_MAX_SECONDS = 30.0
RECENT_TRADE_LIMIT = 50_000
SHUTDOWN_DRAIN_SECONDS = 45.0
COLLECTOR_OPERATION_TIMEOUT_SECONDS = 10


@dataclass(frozen=True, slots=True)
class _MarketMetadata:
    market_id: int
    price_scale: int
    quantity_scale: int
    default_spec_id: int
    specifications: tuple[tuple[int, int], ...]

    def specification_at(self, timestamp_us: int) -> int:
        selected = self.default_spec_id
        for effective_us, specification_id in self.specifications:
            if effective_us > timestamp_us:
                break
            selected = specification_id
        return selected


class FootprintCollector:
    """Collect exact public executions directly into ClickHouse.

    The bounded queue is the only buffer. A ClickHouse failure closes the
    current source connection and the writer retries the unchanged in-memory
    batch, so executions are never silently skipped while the process keeps
    running.
    """

    def __init__(self, exchange: str):
        if exchange not in ADAPTER_MODULES:
            raise ValueError(f"unsupported footprint exchange: {exchange}")
        self.exchange = exchange
        self.adapter = importlib.import_module(ADAPTER_MODULES[exchange])
        self.stop_event = threading.Event()
        self._stream_stop_event = threading.Event()

        self.queue: queue.Queue[StoredExecution] = queue.Queue(maxsize=QUEUE_SIZE)
        self.commit_batch_size = COMMIT_BATCH_SIZE
        self.commit_interval_seconds = COMMIT_INTERVAL_SECONDS
        self.reconnect_delay_seconds = RECONNECT_DELAY_SECONDS
        self.retry_initial_seconds = RETRY_INITIAL_SECONDS
        self.retry_max_seconds = RETRY_MAX_SECONDS
        self.recent_trade_limit = RECENT_TRADE_LIMIT
        self.shutdown_drain_seconds = SHUTDOWN_DRAIN_SECONDS

        self.instruments: dict[str, RuntimeInstrument] = {}
        self._markets: dict[str, _MarketMetadata] = {}
        self._market_scales: dict[int, tuple[int, int]] = {}
        self._connection_source_epoch = 0
        self._last_source_epoch = 0
        self._next_hyperliquid_sequence = 0
        self._ingest_lock = threading.RLock()
        self._recent_trades: dict[
            int, OrderedDict[str, ExecutionIdentitySignature]
        ] = {}

        self._fatal_error: BaseException | None = None
        self._fatal_lock = threading.Lock()
        self._shutdown_deadline: float | None = None
        self._uncommitted_batch_count = 0
        self._clickhouse_lock = threading.RLock()
        self._clickhouse_access: ClickHouseAccess | None = None
        self._execution_store: ClickHouseExecutionStore | None = None

    def on_trade(self, trade: NormalizedTrade) -> None:
        market = self._markets.get(trade.market_key)
        if market is None:
            return

        with self._ingest_lock:
            source_epoch = self._connection_source_epoch
            if source_epoch <= 0:
                error = RuntimeError(
                    "execution received before a ClickHouse source epoch was assigned"
                )
                self._set_fatal(error)
                raise error

            stored_trade = trade
            if self.exchange == "hyperliquid":
                stored_trade = replace(
                    trade,
                    venue_sequence=self._next_hyperliquid_sequence,
                )

            try:
                instrument_spec_id = market.specification_at(
                    stored_trade.exchange_timestamp_us
                )
                execution = StoredExecution.from_trade(
                    market_id=market.market_id,
                    instrument_spec_id=instrument_spec_id,
                    source_epoch=source_epoch,
                    price_scale=market.price_scale,
                    quantity_scale=market.quantity_scale,
                    trade=stored_trade,
                )
                signature = _execution_identity_signature(execution)
                recent = self._recent_trades.setdefault(
                    market.market_id,
                    OrderedDict(),
                )
                previous = recent.get(trade.trade_id)
                if previous is not None:
                    if previous != signature:
                        raise ExecutionIdentityConflict(
                            "conflicting immutable execution "
                            f"{trade.market_key}:{trade.trade_id}"
                        )
                    recent.move_to_end(trade.trade_id)
                    return
                self.queue.put_nowait(execution)
            except queue.Full as exc:
                error = RuntimeError("footprint ClickHouse ingestion queue is full")
                self._set_fatal(error)
                raise error from exc
            except ExecutionIdentityConflict as exc:
                self._set_fatal(exc)
                raise
            except Exception as exc:
                error = RuntimeError(
                    "failed to map a normalized execution to exact integer atoms"
                )
                self._set_fatal(error)
                raise error from exc

            if self.exchange == "hyperliquid":
                self._next_hyperliquid_sequence += 1
            recent[trade.trade_id] = signature
            recent.move_to_end(trade.trade_id)
            while len(recent) > self.recent_trade_limit:
                recent.popitem(last=False)

    def run(self) -> None:
        writer = threading.Thread(
            target=self._writer_loop,
            name=f"{self.exchange}-clickhouse-writer",
            daemon=False,
        )
        writer.start()
        try:
            while not self.stop_event.is_set():
                try:
                    self._refresh_instruments()
                except Exception:
                    logger.exception(
                        "Failed to refresh %s execution specifications",
                        self.exchange,
                    )
                    if self.stop_event.wait(self.reconnect_delay_seconds):
                        break
                    continue

                if not self.instruments:
                    raise RuntimeError(
                        f"No enabled footprint markets configured for {self.exchange}"
                    )

                source_epoch = self._allocate_source_epoch_with_retry()
                if source_epoch is None:
                    break
                symbols = {
                    instrument.symbol: market_key
                    for market_key, instrument in self.instruments.items()
                }
                self._stream_stop_event.clear()
                logger.info(
                    "Starting %s raw execution stream at source epoch %s for %s",
                    self.exchange,
                    source_epoch,
                    sorted(symbols),
                )
                try:
                    self.adapter.listen(
                        symbols,
                        self.on_trade,
                        self._stream_stop_event,
                    )
                except Exception:
                    if self._fatal_error is None:
                        logger.exception("%s execution stream failed", self.exchange)
                    else:
                        logger.error(
                            "%s execution ingestion stopped: %s",
                            self.exchange,
                            self._fatal_error,
                        )

                if self._fatal_error is not None or self.stop_event.is_set():
                    break
                if not self._wait_until_drained():
                    break
                if self.stop_event.wait(self.reconnect_delay_seconds):
                    break
                logger.info("Reconnecting %s execution stream", self.exchange)
        finally:
            self.stop()
            writer.join()
            uncommitted = self._uncommitted_batch_count + self.queue.qsize()
            if uncommitted:
                logger.error(
                    "Footprint collector stopped with uncommitted executions "
                    "exchange=%s count=%s",
                    self.exchange,
                    uncommitted,
                )
            self._close_clickhouse()
            close_old_connections()

        if self._fatal_error is not None:
            raise RuntimeError(
                "footprint collector terminated after an execution ingestion failure"
            ) from self._fatal_error

    def stop(self) -> None:
        if self._shutdown_deadline is None:
            self._shutdown_deadline = (
                time.monotonic() + self.shutdown_drain_seconds
            )
        self.stop_event.set()
        self._stream_stop_event.set()

    def _refresh_instruments(self) -> None:
        close_old_connections()
        try:
            instruments = load_runtime_instruments(self.exchange)
            market_ids = [instrument.market_id for instrument in instruments.values()]
            markets = {
                market.pk: market
                for market in FootprintMarket.objects.filter(pk__in=market_ids)
            }
            specifications = list(
                InstrumentSpec.objects.filter(market_id__in=market_ids).order_by(
                    "market_id", "effective_from", "pk"
                )
            )
        finally:
            close_old_connections()

        history: dict[int, list[tuple[int, int]]] = {
            market_id: [] for market_id in market_ids
        }
        for specification in specifications:
            history.setdefault(specification.market_id, []).append(
                (
                    _datetime_to_microseconds(specification.effective_from),
                    specification.pk,
                )
            )

        metadata: dict[str, _MarketMetadata] = {}
        scales: dict[int, tuple[int, int]] = {}
        for market_key, instrument in instruments.items():
            market = markets.get(instrument.market_id)
            if market is None:
                raise RuntimeError(
                    f"configured footprint market {instrument.market_id} disappeared"
                )
            metadata[market_key] = _MarketMetadata(
                market_id=instrument.market_id,
                price_scale=int(market.price_scale),
                quantity_scale=int(market.quantity_scale),
                default_spec_id=instrument.spec_id,
                specifications=tuple(history.get(instrument.market_id, ())),
            )
            scales[instrument.market_id] = (
                int(market.price_scale),
                int(market.quantity_scale),
            )

        with self._ingest_lock:
            self.instruments = instruments
            self._markets = metadata
            self._market_scales = scales

    def _allocate_source_epoch_with_retry(self) -> int | None:
        attempt = 0
        while not self.stop_event.is_set():
            try:
                return self._allocate_source_epoch()
            except Exception as exc:
                self._stream_stop_event.set()
                self._close_clickhouse()
                failure_kind = classify_clickhouse_error(exc, is_write=False)
                if failure_kind == ClickHouseFailureKind.FATAL:
                    self._set_fatal(exc)
                    return None
                attempt += 1
                delay = self._retry_delay(attempt)
                logger.warning(
                    "ClickHouse source-epoch read failed for %s; retrying in %.3fs",
                    self.exchange,
                    delay,
                )
                if self.stop_event.wait(delay):
                    return None
        return None

    def _allocate_source_epoch(self) -> int:
        market_ids = sorted(self._market_scales)
        if not market_ids:
            raise RuntimeError("cannot allocate a source epoch without markets")
        store = self._ensure_execution_store()
        recent = store.recent_execution_identities(
            market_ids,
            per_market_limit=self.recent_trade_limit,
        )
        self._seed_recent_trades(recent)
        observed = store.max_source_epoch(market_ids)
        source_epoch = max(observed, self._last_source_epoch) + 1
        if source_epoch > _MAX_SOURCE_EPOCH:
            raise OverflowError("ClickHouse execution source epoch exhausted UInt32")
        with self._ingest_lock:
            self._connection_source_epoch = source_epoch
            self._last_source_epoch = source_epoch
            self._next_hyperliquid_sequence = 0
        return source_epoch

    def _seed_recent_trades(
        self,
        rows_by_market: dict[int, tuple[RecentExecutionIdentity, ...]],
    ) -> None:
        seeded: dict[int, OrderedDict[str, ExecutionIdentitySignature]] = {}
        for market_id in sorted(self._market_scales):
            cache: OrderedDict[str, ExecutionIdentitySignature] = OrderedDict()
            # ClickHouse returns newest first. Insert oldest first so the newest
            # identity remains at the LRU tail.
            for row in reversed(rows_by_market.get(market_id, ())):
                if row.market_id != market_id:
                    raise RuntimeError(
                        "ClickHouse recent execution identity was assigned "
                        "to the wrong market"
                    )
                previous = cache.get(row.trade_id)
                if previous is not None and previous != row.signature:
                    raise ExecutionIdentityConflict(
                        "conflicting immutable execution "
                        f"market={market_id}:{row.trade_id}"
                    )
                cache[row.trade_id] = row.signature
                cache.move_to_end(row.trade_id)
            while len(cache) > self.recent_trade_limit:
                cache.popitem(last=False)
            seeded[market_id] = cache

        with self._ingest_lock:
            for market_id, cache in seeded.items():
                current = self._recent_trades.get(market_id, {})
                for trade_id in current.keys() & cache.keys():
                    if current[trade_id] != cache[trade_id]:
                        raise ExecutionIdentityConflict(
                            "conflicting immutable execution "
                            f"market={market_id}:{trade_id}"
                        )
            self._recent_trades = seeded

    def _writer_loop(self) -> None:
        while self._fatal_error is None:
            if self.stop_event.is_set():
                if self.queue.empty():
                    return
                if (
                    self._shutdown_deadline is None
                    or time.monotonic() >= self._shutdown_deadline
                ):
                    return
            try:
                first = self.queue.get(timeout=self.commit_interval_seconds)
            except queue.Empty:
                continue

            members = [first]
            deadline = time.monotonic() + self.commit_interval_seconds
            while len(members) < self.commit_batch_size:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                try:
                    members.append(self.queue.get(timeout=remaining))
                except queue.Empty:
                    break

            try:
                batch = tuple(members)
                if not self._insert_with_retry(batch):
                    self._uncommitted_batch_count += len(batch)
                    return
            except Exception as exc:
                logger.exception("Unexpected direct ClickHouse writer failure")
                self._uncommitted_batch_count += len(members)
                self._set_fatal(exc)
                return
            finally:
                for _member in members:
                    self.queue.task_done()
            if (
                self.stop_event.is_set()
                and self._shutdown_deadline is not None
                and time.monotonic() >= self._shutdown_deadline
            ):
                return

    def _insert_with_retry(self, batch: tuple[StoredExecution, ...]) -> bool:
        attempt = 0
        while True:
            try:
                self._ensure_execution_store().insert_batch(batch)
                return True
            except Exception as exc:
                self._stream_stop_event.set()
                self._close_clickhouse()
                failure_kind = (
                    ClickHouseFailureKind.FATAL
                    if isinstance(exc, ExecutionIdentityConflict)
                    else classify_clickhouse_error(exc, is_write=True)
                )
                if failure_kind == ClickHouseFailureKind.FATAL:
                    self._set_fatal(exc)
                    return False
                if self.stop_event.is_set() and not self._wait_for_shutdown_retry(
                    self._retry_delay(attempt + 1)
                ):
                    logger.warning(
                        "ClickHouse raw execution insert failed (%s); shutdown "
                        "drain deadline expired",
                        failure_kind.value,
                    )
                    return False
                attempt += 1
                delay = self._retry_delay(attempt)
                logger.warning(
                    "ClickHouse raw execution insert failed (%s); "
                    "retrying unchanged rows in %.3fs",
                    failure_kind.value,
                    delay,
                )
                if not self.stop_event.is_set():
                    self.stop_event.wait(delay)

    def _retry_delay(self, attempt: int) -> float:
        exponent = max(0, min(attempt - 1, 30))
        return min(
            self.retry_max_seconds,
            self.retry_initial_seconds * (2**exponent),
        )

    def _wait_for_shutdown_retry(self, delay: float) -> bool:
        deadline = self._shutdown_deadline
        if deadline is None:
            return False
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return False
        time.sleep(min(delay, remaining))
        return time.monotonic() < deadline

    def _wait_until_drained(self) -> bool:
        while self._fatal_error is None:
            with self.queue.all_tasks_done:
                if self.queue.unfinished_tasks == 0:
                    return True
                self.queue.all_tasks_done.wait(timeout=0.1)
        return False

    def _ensure_execution_store(self) -> ClickHouseExecutionStore:
        with self._clickhouse_lock:
            if self._execution_store is not None:
                return self._execution_store
            configuration = ClickHouseSettings.from_environment()
            client = create_clickhouse_client(
                configuration,
                role=f"collector-{self.exchange}",
                for_inserts=True,
                bootstrap_database=True,
                operation_timeout_seconds=COLLECTOR_OPERATION_TIMEOUT_SECONDS,
            )
            access = ClickHouseAccess(client)
            try:
                ensure_clickhouse_schema(access, configuration.database)
            except Exception:
                access.close()
                raise
            self._clickhouse_access = access
            self._execution_store = ClickHouseExecutionStore(
                access,
                database=configuration.database,
            )
            return self._execution_store

    def _close_clickhouse(self) -> None:
        with self._clickhouse_lock:
            access = self._clickhouse_access
            self._clickhouse_access = None
            self._execution_store = None
        if access is None:
            return
        try:
            access.close()
        except Exception:
            logger.warning("Failed to close the %s ClickHouse client", self.exchange)

    def _set_fatal(self, error: BaseException) -> None:
        with self._fatal_lock:
            if self._fatal_error is None:
                self._fatal_error = error
        self.stop_event.set()
        self._stream_stop_event.set()


def _execution_identity_signature(
    execution: StoredExecution,
) -> ExecutionIdentitySignature:
    return (
        execution.trade.exchange_timestamp_us,
        execution.price_atoms,
        execution.quantity_atoms,
        execution.trade.taker_side == TakerSide.BUY,
        execution.instrument_spec_id,
    )


def _datetime_to_microseconds(value: datetime) -> int:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    else:
        value = value.astimezone(timezone.utc)
    seconds = int(value.timestamp())
    return seconds * 1_000_000 + value.microsecond
