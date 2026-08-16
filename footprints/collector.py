from __future__ import annotations

import importlib
import logging
import queue
import threading
import time
from collections import OrderedDict, deque
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone

from django.db import close_old_connections
from redis.exceptions import RedisError

from core.redis_config import get_redis_connection
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
from footprints.wakeups import publish_execution_wakeup


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
STREAM_CONNECTIONS = 2
STANDBY_BUFFER_TRADES = 50_000
STREAM_READY_TIMEOUT_SECONDS = 15.0
STREAM_JOIN_TIMEOUT_SECONDS = 5.0


@dataclass(slots=True)
class _HotStandbySlot:
    ready: bool = False
    buffers: dict[str, deque[NormalizedTrade]] = field(default_factory=dict)

    def reset(self) -> None:
        self.ready = False
        self.buffers.clear()


class ExecutionSourceOrderError(RuntimeError):
    """A unique execution would sort behind the current projector cursor."""


class _HotStandbyRouter:
    """Send one ordered feed to storage while a second stays hot."""

    def __init__(self, collector: FootprintCollector) -> None:
        self.collector = collector
        self.lock = threading.RLock()
        self.slots = {slot: _HotStandbySlot() for slot in range(STREAM_CONNECTIONS)}
        self.leader_slot: int | None = None
        self.anchors: dict[str, NormalizedTrade] = {}

    def _forward(self, trade: NormalizedTrade) -> None:
        self.collector.on_trade(trade)
        self.anchors[trade.market_key] = trade

    @staticmethod
    def _buffer(state: _HotStandbySlot, trade: NormalizedTrade) -> None:
        state.buffers.setdefault(
            trade.market_key,
            deque(maxlen=STANDBY_BUFFER_TRADES),
        ).append(trade)

    def start(self, slot: int) -> None:
        with self.lock:
            self.slots[slot].reset()

    def ready(self, slot: int) -> None:
        with self.lock:
            state = self.slots[slot]
            if self.collector._stream_stopping():
                return
            state.ready = True
            logger.info(
                "%s execution feed subscription-ready slot=%s",
                self.collector.exchange,
                slot,
            )
            if self.leader_slot is None:
                for market_key in sorted(state.buffers):
                    for trade in state.buffers[market_key]:
                        self._forward(trade)
                state.buffers.clear()
                self.leader_slot = slot
                logger.info(
                    "%s execution leader selected slot=%s",
                    self.collector.exchange,
                    slot,
                )

    def trade(
        self,
        slot: int,
        trade: NormalizedTrade,
    ) -> None:
        with self.lock:
            state = self.slots[slot]
            if self.collector._stream_stopping():
                return
            if state.ready and slot == self.leader_slot:
                self._forward(trade)
                return
            self._buffer(state, trade)

    def disconnected(self, slot: int) -> bool:
        """Promote a ready standby; return false when coverage was lost."""

        with self.lock:
            state = self.slots[slot]
            state.ready = False
            if self.collector._stream_stopping():
                if slot == self.leader_slot:
                    self.leader_slot = None
                return True
            if self.leader_slot is None:
                return not any(state.buffers.values())
            if slot != self.leader_slot:
                return True

            self.leader_slot = None
            candidate = next(
                (
                    (candidate_slot, candidate_state)
                    for candidate_slot, candidate_state in self.slots.items()
                    if candidate_slot != slot and candidate_state.ready
                ),
                None,
            )
            if candidate is None:
                return False

            candidate_slot, candidate_state = candidate
            replay: dict[str, tuple[NormalizedTrade, ...]] = {}
            overlap = True
            for market_key, anchor in self.anchors.items():
                buffered = tuple(candidate_state.buffers.get(market_key, ()))
                anchor_index = next(
                    (
                        index
                        for index, trade in enumerate(buffered)
                        if trade.trade_id == anchor.trade_id
                    ),
                    None,
                )
                if anchor_index is None:
                    overlap = False
                    shared_index = next(
                        (
                            index
                            for index in range(len(buffered) - 1, -1, -1)
                            if self.collector._is_recent_trade(
                                market_key,
                                buffered[index].trade_id,
                            )
                        ),
                        None,
                    )
                    anchor_index = shared_index if shared_index is not None else 0
                replay[market_key] = buffered[anchor_index:]

            if overlap:
                for market_key, buffered in candidate_state.buffers.items():
                    replay.setdefault(market_key, tuple(buffered))
            else:
                source_epoch = self.collector._advance_source_epoch()
                self.anchors.clear()
                logger.error(
                    "%s execution handoff had no overlap; source_epoch=%s",
                    self.collector.exchange,
                    source_epoch,
                )

            for market_key in sorted(replay):
                for trade in replay[market_key]:
                    self._forward(trade)
            candidate_state.buffers.clear()
            self.leader_slot = candidate_slot
            logger.warning(
                "%s execution leader failed over slot=%s",
                self.collector.exchange,
                candidate_slot,
            )
            return True


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
    """Arbitrate redundant public execution feeds into one ClickHouse tape.

    Two independently reconnecting source legs share one logical source epoch.
    Only the leader is admitted; the standby continuously buffers the same
    feed for immediate handoff. The bounded queues remain the only buffers, so
    exhaustion is fatal instead of lossy.
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
        self._bybit_sequence_order: dict[str, tuple[int | None, int]] = {}
        self._last_admitted_cursors: dict[str, tuple[int, int, int, int, str]] = {}
        self._ingest_lock = threading.RLock()
        self._recent_trades: dict[
            int, OrderedDict[str, ExecutionIdentitySignature]
        ] = {}

        self._fatal_error: BaseException | None = None
        self._fatal_lock = threading.Lock()
        self._feed_status_lock = threading.RLock()
        self._active_stream_stop_events: set[threading.Event] = set()
        self._shutdown_deadline: float | None = None
        self._uncommitted_batch_count = 0
        self._clickhouse_lock = threading.RLock()
        self._clickhouse_access: ClickHouseAccess | None = None
        self._execution_store: ClickHouseExecutionStore | None = None
        self._wakeup_redis = None

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
            elif self.exchange == "bybit":
                if trade.venue_sequence is None:
                    error = ExecutionSourceOrderError(
                        "Bybit execution is missing its cross sequence"
                    )
                    self._set_fatal(error)
                    raise error
                previous_sequence, next_index = self._bybit_sequence_order.get(
                    trade.market_key,
                    (None, 0),
                )
                batch_index = (
                    next_index if trade.venue_sequence == previous_sequence else 0
                )
                stored_trade = replace(trade, batch_index=batch_index)

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
                cursor = (
                    execution.source_epoch,
                    execution.source_sequence,
                    execution.trade.batch_index,
                    execution.trade.exchange_timestamp_us,
                    execution.trade.trade_id,
                )
                previous_cursor = self._last_admitted_cursors.get(trade.market_key)
                if previous_cursor is not None and cursor <= previous_cursor:
                    raise ExecutionSourceOrderError(
                        "execution would regress the canonical source order "
                        f"{trade.market_key}:{trade.trade_id} "
                        f"cursor={cursor} previous={previous_cursor}"
                    )
                self.queue.put_nowait(execution)
            except queue.Full as exc:
                error = RuntimeError("footprint ClickHouse ingestion queue is full")
                self._set_fatal(error)
                raise error from exc
            except ExecutionIdentityConflict as exc:
                self._set_fatal(exc)
                raise
            except ExecutionSourceOrderError as exc:
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
            elif self.exchange == "bybit":
                self._bybit_sequence_order[trade.market_key] = (
                    stored_trade.venue_sequence,
                    stored_trade.batch_index + 1,
                )
            recent[trade.trade_id] = signature
            recent.move_to_end(trade.trade_id)
            while len(recent) > self.recent_trade_limit:
                recent.popitem(last=False)
            self._last_admitted_cursors[trade.market_key] = cursor

    def _is_recent_trade(self, market_key: str, trade_id: str) -> bool:
        market = self._markets.get(market_key)
        return market is not None and trade_id in self._recent_trades.get(
            market.market_id,
            (),
        )

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
                    "Starting %s redundant execution generation source_epoch=%s "
                    "connections=%s symbols=%s",
                    self.exchange,
                    source_epoch,
                    STREAM_CONNECTIONS,
                    sorted(symbols),
                )
                try:
                    self._run_stream_generation(symbols)
                except Exception:
                    if self._fatal_error is None:
                        raise
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
                logger.warning(
                    "Restarting %s execution coverage in a new source epoch",
                    self.exchange,
                )
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
            self._close_wakeup_redis()
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
        self._stop_active_streams()

    def _run_stream_generation(
        self,
        symbols: dict[str, str],
    ) -> None:
        router = _HotStandbyRouter(self)
        self._stream_stop_event.clear()
        if self.stop_event.is_set():
            return
        workers = [
            threading.Thread(
                target=self._stream_worker,
                args=(slot, symbols, router),
                name=f"{self.exchange}-execution-feed-{slot}",
                daemon=True,
            )
            for slot in range(STREAM_CONNECTIONS)
        ]
        for worker in workers:
            worker.start()
        try:
            self._stream_stop_event.wait()
        finally:
            self._stop_active_streams()
            for worker in workers:
                worker.join(timeout=STREAM_JOIN_TIMEOUT_SECONDS)
            stuck = [worker.name for worker in workers if worker.is_alive()]
            if stuck and self._fatal_error is None:
                self._set_fatal(
                    RuntimeError(
                        "execution feed workers did not stop: " + ", ".join(stuck)
                    )
                )

    def _stream_worker(
        self,
        slot: int,
        symbols: dict[str, str],
        router: _HotStandbyRouter,
    ) -> None:
        while not self._stream_stopping():
            attempt_stop_event = threading.Event()
            ready_event = threading.Event()
            router.start(slot)
            self._register_stream_attempt(attempt_stop_event)
            if attempt_stop_event.is_set():
                self._unregister_stream_attempt(attempt_stop_event)
                return

            def report_ready() -> None:
                ready_event.set()
                router.ready(slot)

            def report_trade(trade: NormalizedTrade) -> None:
                router.trade(slot, trade)

            def stop_if_unready() -> None:
                if not ready_event.is_set():
                    attempt_stop_event.set()

            readiness_timeout = threading.Timer(
                STREAM_READY_TIMEOUT_SECONDS,
                stop_if_unready,
            )
            readiness_timeout.daemon = True
            readiness_timeout.start()

            try:
                self.adapter.listen(
                    symbols,
                    report_trade,
                    attempt_stop_event,
                    on_ready=report_ready,
                )
            except Exception:
                if not self._stream_stopping():
                    logger.exception(
                        "%s execution feed failed slot=%s",
                        self.exchange,
                        slot,
                    )
            finally:
                readiness_timeout.cancel()
                self._unregister_stream_attempt(attempt_stop_event)

            if not router.disconnected(slot):
                if not self._stream_stopping():
                    logger.error(
                        "%s execution coverage lost; opening a new source epoch",
                        self.exchange,
                    )
                self._stop_active_streams()
                return
            if self._stream_stopping():
                return
            logger.warning(
                "%s execution feed reconnecting slot=%s in %.3fs",
                self.exchange,
                slot,
                self.reconnect_delay_seconds,
            )
            if self._stream_stop_event.wait(self.reconnect_delay_seconds):
                return

    def _stream_stopping(self) -> bool:
        return (
            self._fatal_error is not None
            or self.stop_event.is_set()
            or self._stream_stop_event.is_set()
        )

    def _register_stream_attempt(
        self,
        stop_event: threading.Event,
    ) -> None:
        with self._feed_status_lock:
            self._active_stream_stop_events.add(stop_event)
            if self.stop_event.is_set() or self._stream_stop_event.is_set():
                stop_event.set()

    def _unregister_stream_attempt(self, stop_event: threading.Event) -> None:
        with self._feed_status_lock:
            self._active_stream_stop_events.discard(stop_event)

    def _stop_active_streams(self) -> None:
        self._stream_stop_event.set()
        with self._feed_status_lock:
            members = tuple(self._active_stream_stop_events)
        for stop_event in members:
            stop_event.set()

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
                self._stop_active_streams()
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
            self._bybit_sequence_order.clear()
            self._last_admitted_cursors.clear()
        return source_epoch

    def _advance_source_epoch(self) -> int:
        """Expose an unverified live handoff without dropping buffered trades."""

        with self._ingest_lock:
            source_epoch = self._last_source_epoch + 1
            if source_epoch > _MAX_SOURCE_EPOCH:
                error = OverflowError(
                    "ClickHouse execution source epoch exhausted UInt32"
                )
                self._set_fatal(error)
                raise error
            self._connection_source_epoch = source_epoch
            self._last_source_epoch = source_epoch
            self._next_hyperliquid_sequence = 0
            self._bybit_sequence_order.clear()
            self._last_admitted_cursors.clear()
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
                self._notify_committed_executions(batch)
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
                self._stop_active_streams()
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

    def _notify_committed_executions(
        self,
        batch: tuple[StoredExecution, ...],
    ) -> None:
        try:
            if self._wakeup_redis is None:
                self._wakeup_redis = get_redis_connection()
            publish_execution_wakeup(
                {execution.market_id for execution in batch},
                redis_client=self._wakeup_redis,
            )
        except RedisError:
            logger.warning(
                "Failed to wake chart maintenance after %s execution commit",
                self.exchange,
            )
            self._close_wakeup_redis()

    def _close_wakeup_redis(self) -> None:
        client = self._wakeup_redis
        self._wakeup_redis = None
        if client is None:
            return
        try:
            client.close()
        except RedisError:
            logger.warning("Failed to close the %s wakeup client", self.exchange)

    def _set_fatal(self, error: BaseException) -> None:
        with self._fatal_lock:
            if self._fatal_error is None:
                self._fatal_error = error
        self.stop_event.set()
        self._stop_active_streams()


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
