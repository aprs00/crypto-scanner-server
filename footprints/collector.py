from __future__ import annotations

import importlib
import logging
import os
import queue
import threading
import time
from collections import OrderedDict, deque
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from pathlib import Path

from django.db import close_old_connections
from redis.exceptions import RedisError

from core.redis_config import get_redis_connection
from footprints.leadership import FootprintWriterLeadership
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
from footprints.storage.outbox import ExecutionOutbox
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
STREAM_SHUTDOWN_TIMEOUT_SECONDS = 15.0
STREAM_ROTATION_INTERVAL_SECONDS = 60 * 60
LEADERSHIP_RETRY_SECONDS = 0.5
LEADERSHIP_VERIFY_SECONDS = 1.0
COORDINATION_INTERVAL_SECONDS = 0.1

STATE_DIRECTORY_ENVIRONMENT = "FOOTPRINT_STATE_DIR"
FEEDS_READY_MARKER = "feeds-ready"
WRITER_ACTIVE_MARKER = "writer-active"
HANDOFF_VERIFIED_MARKER = "handoff-verified"


@dataclass(slots=True)
class _HotStandbySlot:
    ready: bool = False
    ready_since: float | None = None
    buffers: dict[str, deque[NormalizedTrade]] = field(default_factory=dict)

    def reset(self) -> None:
        self.ready = False
        self.ready_since = None
        self.buffers.clear()


class ExecutionSourceOrderError(RuntimeError):
    """A unique execution would sort behind the current projector cursor."""


class _HotStandbyRouter:
    """Buffer redundant feeds until one writer is allowed to become active."""

    def __init__(self, collector: FootprintCollector) -> None:
        self.collector = collector
        self.lock = threading.RLock()
        self.slots = {slot: _HotStandbySlot() for slot in range(STREAM_CONNECTIONS)}
        self.leader_slot: int | None = None
        self.anchors: dict[str, NormalizedTrade] = {}
        self.active = False
        self.activation_overlap_verified = False

    def _forward(self, trade: NormalizedTrade) -> None:
        self.collector.on_trade(trade)
        self.anchors[trade.market_key] = trade

    def _buffer(self, state: _HotStandbySlot, trade: NormalizedTrade) -> None:
        buffered = state.buffers.setdefault(
            trade.market_key,
            deque(maxlen=STANDBY_BUFFER_TRADES),
        )
        if not self.active and len(buffered) >= STANDBY_BUFFER_TRADES:
            if self.collector.exchange in ("binance", "bybit"):
                # Keep the newest overlap. Activation must still prove continuity.
                if not getattr(self, "buffer_eviction_reported", False):
                    logger.warning(
                        "%s warm buffer reached capacity; retaining newest overlap",
                        self.collector.exchange,
                    )
                    self.collector._record_coverage_issue("warm_buffer_evicted")
                    self.buffer_eviction_reported = True
                buffered.append(trade)
                return
            error = RuntimeError(
                "footprint warm-standby buffer is full "
                f"exchange={self.collector.exchange} market={trade.market_key}"
            )
            self.collector._set_fatal(error)
            raise error
        buffered.append(trade)

    def _publish_readiness(self) -> None:
        self.collector._set_feeds_ready(self._feeds_ready())

    def _feeds_ready(self) -> bool:
        if not all(state.ready for state in self.slots.values()):
            return False
        return self.active or all(
            any(state.buffers.values()) for state in self.slots.values()
        )

    def start(self, slot: int) -> None:
        with self.lock:
            self.slots[slot].reset()
            self._publish_readiness()

    def ready(self, slot: int) -> None:
        with self.lock:
            state = self.slots[slot]
            if self.collector._stream_stopping():
                return
            state.ready = True
            state.ready_since = time.monotonic()
            logger.info(
                "%s execution feed subscription-ready slot=%s",
                self.collector.exchange,
                slot,
            )
            if self.leader_slot is None:
                self.leader_slot = slot
                logger.info(
                    "%s execution local leader selected slot=%s",
                    self.collector.exchange,
                    slot,
                )
            self._publish_readiness()

    def feeds_ready(self) -> bool:
        with self.lock:
            return self._feeds_ready()

    def activate(self) -> bool:
        """Replay the selected feed once both subscriptions are ready."""

        with self.lock:
            if self.active:
                return True
            if not all(state.ready for state in self.slots.values()):
                return False
            if self.leader_slot is None:
                ready = [
                    (state.ready_since, slot)
                    for slot, state in self.slots.items()
                    if state.ready_since is not None
                ]
                if not ready:
                    return False
                self.leader_slot = min(ready)[1]

            overlapping_slots = [
                slot
                for slot, state in self.slots.items()
                if state.ready
                and any(
                    self.collector._is_recent_trade(market_key, trade.trade_id)
                    for market_key, trades in state.buffers.items()
                    for trade in trades
                )
            ]
            self.activation_overlap_verified = bool(overlapping_slots)
            if overlapping_slots and self.leader_slot not in overlapping_slots:
                self.leader_slot = min(
                    overlapping_slots,
                    key=lambda slot: (self.slots[slot].ready_since, slot),
                )

            leader_state = self.slots[self.leader_slot]
            buffered_count = sum(
                len(members) for members in leader_state.buffers.values()
            )
            for market_key in sorted(leader_state.buffers):
                for trade in leader_state.buffers[market_key]:
                    self._forward(trade)
            leader_state.buffers.clear()
            self.active = True
            logger.info(
                "%s execution writer activated slot=%s replayed=%s",
                self.collector.exchange,
                self.leader_slot,
                buffered_count,
            )
            return True

    def trade(
        self,
        slot: int,
        trade: NormalizedTrade,
    ) -> None:
        with self.lock:
            state = self.slots[slot]
            if self.collector._stream_stopping():
                return
            if self.active and state.ready and slot == self.leader_slot:
                self._forward(trade)
                return
            self._buffer(state, trade)
            if not self.active:
                self._publish_readiness()

    def rotation_candidate(self) -> int | None:
        """Return the oldest leg only when every leg is ready."""

        with self.lock:
            if not self.active:
                return None
            ready = [
                (state.ready_since, slot)
                for slot, state in self.slots.items()
                if state.ready and state.ready_since is not None
            ]
            if len(ready) != len(self.slots):
                return None
            return min(ready)[1]

    def disconnected(self, slot: int, *, planned: bool = False) -> bool:
        """Promote a ready standby; return false when coverage was lost."""

        with self.lock:
            state = self.slots[slot]
            state.ready = False
            state.ready_since = None
            self._publish_readiness()
            if self.collector._stream_stopping():
                if slot == self.leader_slot:
                    self.leader_slot = None
                return True
            if not self.active:
                state.buffers.clear()
                if slot == self.leader_slot:
                    self.leader_slot = next(
                        (
                            candidate_slot
                            for candidate_slot, candidate_state in self.slots.items()
                            if candidate_slot != slot and candidate_state.ready
                        ),
                        None,
                    )
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
                self.collector._record_coverage_issue("unverified_feed_handoff")
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
            if planned:
                logger.info(
                    "%s execution leader rotated to slot=%s",
                    self.collector.exchange,
                    candidate_slot,
                )
            else:
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

    Each process warms two independently reconnecting source legs before one
    process is elected as the exchange writer. The active process admits one
    leg and keeps the other buffered for immediate in-process failover.
    """

    def __init__(self, exchange: str, *, standby: bool = False):
        if exchange not in ADAPTER_MODULES:
            raise ValueError(f"unsupported footprint exchange: {exchange}")
        self.exchange = exchange
        self.adapter = importlib.import_module(ADAPTER_MODULES[exchange])
        self.stop_event = threading.Event()
        self._stream_stop_event = threading.Event()
        self._promotion_event = threading.Event()
        if not standby:
            self._promotion_event.set()

        self.queue: queue.Queue[StoredExecution] = queue.Queue(maxsize=QUEUE_SIZE)
        self.commit_batch_size = COMMIT_BATCH_SIZE
        self.commit_interval_seconds = COMMIT_INTERVAL_SECONDS
        self.reconnect_delay_seconds = RECONNECT_DELAY_SECONDS
        self.retry_initial_seconds = RETRY_INITIAL_SECONDS
        self.retry_max_seconds = RETRY_MAX_SECONDS
        self.recent_trade_limit = RECENT_TRADE_LIMIT
        self.shutdown_drain_seconds = SHUTDOWN_DRAIN_SECONDS
        self.stream_rotation_interval_seconds = STREAM_ROTATION_INTERVAL_SECONDS

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
        self._state_lock = threading.RLock()
        state_directory = os.environ.get(STATE_DIRECTORY_ENVIRONMENT, "").strip()
        self._state_directory = Path(state_directory) if state_directory else None
        self._writer_fenced = threading.Event()
        self._feed_status_lock = threading.RLock()
        self._active_stream_stop_events: dict[int, threading.Event] = {}
        self._planned_stream_stop_events: set[threading.Event] = set()
        self._shutdown_deadline: float | None = None
        self._uncommitted_batch_count = 0
        self._clickhouse_lock = threading.RLock()
        self._clickhouse_access: ClickHouseAccess | None = None
        self._execution_store: ClickHouseExecutionStore | None = None
        self._wakeup_redis = None
        self._outbox_directory = (
            os.environ.get("FOOTPRINT_OUTBOX_DIR", "").strip()
            if exchange in ("binance", "bybit")
            else ""
        )
        self._outbox: ExecutionOutbox | None = None
        self._outbox_ready = threading.Event()
        self._last_trade_at = 0.0
        self._coverage_issue = None

    def _record_coverage_issue(self, reason):
        self._coverage_issue = {"at": time.time(), "reason": reason}

    def promote(self) -> None:
        """Allow an explicitly warm candidate to compete for leadership."""

        if not self._promotion_event.is_set():
            logger.info("%s footprint standby promotion requested", self.exchange)
            self._promotion_event.set()

    def _reset_state_markers(self) -> None:
        directory = self._state_directory
        if directory is None:
            return
        directory.mkdir(parents=True, exist_ok=True)
        for name in (
            FEEDS_READY_MARKER,
            WRITER_ACTIVE_MARKER,
            HANDOFF_VERIFIED_MARKER,
        ):
            (directory / name).unlink(missing_ok=True)

    def _set_state_marker(
        self,
        name: str,
        present: bool,
        *,
        required: bool = False,
    ) -> None:
        directory = self._state_directory
        if directory is None:
            return
        try:
            with self._state_lock:
                marker = directory / name
                if present:
                    directory.mkdir(parents=True, exist_ok=True)
                    marker.touch()
                else:
                    marker.unlink(missing_ok=True)
        except OSError:
            logger.exception(
                "Failed to update %s footprint state marker %s",
                self.exchange,
                name,
            )
            if required:
                raise

    def _set_feeds_ready(self, ready: bool) -> None:
        self._set_state_marker(FEEDS_READY_MARKER, ready)

    def _set_writer_active(self, active: bool) -> None:
        self._set_state_marker(WRITER_ACTIVE_MARKER, active)

    def _set_handoff_verified(
        self,
        verified: bool,
        *,
        required: bool = False,
    ) -> None:
        self._set_state_marker(
            HANDOFF_VERIFIED_MARKER,
            verified,
            required=required,
        )

    def on_trade(self, trade: NormalizedTrade) -> None:
        self._last_trade_at = time.time()
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
        leadership = FootprintWriterLeadership(self.exchange)
        self._reset_state_markers()
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

                symbols = {
                    instrument.symbol: market_key
                    for market_key, instrument in self.instruments.items()
                }
                logger.info(
                    "Starting %s warm execution generation connections=%s symbols=%s",
                    self.exchange,
                    STREAM_CONNECTIONS,
                    sorted(symbols),
                )
                try:
                    self._run_stream_generation(symbols, leadership)
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
                if not self._wait_until_drained(leadership):
                    break
                if leadership.acquired and not self._outbox_directory:
                    try:
                        leadership.close()
                    except Exception as exc:
                        self._writer_fenced.set()
                        self._set_fatal(exc)
                        break
                if self.stop_event.wait(self.reconnect_delay_seconds):
                    break
                logger.warning(
                    "Restarting %s execution coverage in a new source epoch",
                    self.exchange,
                )
        finally:
            self.stop()
            self._join_writer_with_leadership(writer, leadership)
            uncommitted = self._uncommitted_batch_count + self.queue.qsize()
            if uncommitted:
                error = RuntimeError(
                    "Footprint collector stopped with uncommitted executions "
                    f"exchange={self.exchange} count={uncommitted}"
                )
                logger.error("%s", error)
                with self._fatal_lock:
                    if self._fatal_error is None:
                        self._fatal_error = error
            self._close_clickhouse()
            self._close_wakeup_redis()
            if self._outbox is not None:
                self._outbox.close()
            try:
                leadership.close()
            except Exception:
                logger.exception(
                    "Failed to close the %s footprint writer lock",
                    self.exchange,
                )
            close_old_connections()

        if self._fatal_error is not None:
            raise RuntimeError(
                "footprint collector terminated after an execution ingestion failure"
            ) from self._fatal_error

    def stop(self) -> None:
        if self._shutdown_deadline is None:
            self._shutdown_deadline = time.monotonic() + self.shutdown_drain_seconds
        self.stop_event.set()
        self._stop_active_streams()

    def _run_stream_generation(
        self,
        symbols: dict[str, str],
        leadership: FootprintWriterLeadership,
    ) -> None:
        router = _HotStandbyRouter(self)
        self._stream_stop_event.clear()
        self._set_writer_active(False)
        self._set_feeds_ready(False)
        self._set_handoff_verified(False, required=True)
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
            self._coordination_loop(router, leadership)
        finally:
            self._stop_active_streams()
            shutdown_deadline = time.monotonic() + STREAM_SHUTDOWN_TIMEOUT_SECONDS
            for worker in workers:
                worker.join(timeout=max(0.0, shutdown_deadline - time.monotonic()))
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
            self._register_stream_attempt(slot, attempt_stop_event)
            if attempt_stop_event.is_set():
                self._unregister_stream_attempt(slot, attempt_stop_event)
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
                planned_rotation = self._unregister_stream_attempt(
                    slot,
                    attempt_stop_event,
                )

            if not router.disconnected(slot, planned=planned_rotation):
                if not self._stream_stopping():
                    logger.error(
                        "%s execution coverage lost; opening a new source epoch",
                        self.exchange,
                    )
                self._stop_active_streams()
                return
            if self._stream_stopping():
                return
            if planned_rotation:
                logger.info(
                    "%s execution feed reconnecting after rotation slot=%s in %.3fs",
                    self.exchange,
                    slot,
                    self.reconnect_delay_seconds,
                )
            else:
                logger.warning(
                    "%s execution feed reconnecting slot=%s in %.3fs",
                    self.exchange,
                    slot,
                    self.reconnect_delay_seconds,
                )
            if self._stream_stop_event.wait(self.reconnect_delay_seconds):
                return

    def _coordination_loop(
        self,
        router: _HotStandbyRouter,
        leadership: FootprintWriterLeadership,
    ) -> None:
        """Promote one writer, verify its lease, and schedule safe rotations."""

        activated = False
        source_epoch: int | None = None
        next_lock_attempt = 0.0
        next_leadership_verify = 0.0
        next_rotation: float | None = None

        while not self._stream_stopping():
            now = time.monotonic()
            if (
                self._outbox_ready.is_set()
                and leadership.acquired
                and now >= next_leadership_verify
            ):
                # Delivery keeps running even while the next feed generation
                # warms up; its lease must be verified during that period too.
                if not self._verify_writer_leadership(leadership):
                    return
                next_leadership_verify = now + LEADERSHIP_VERIFY_SECONDS
            if not activated:
                if not self._promotion_event.is_set() or not router.feeds_ready():
                    self._stream_stop_event.wait(COORDINATION_INTERVAL_SECONDS)
                    continue

                if not leadership.acquired and now >= next_lock_attempt:
                    try:
                        acquired = leadership.try_acquire()
                    except Exception:
                        logger.exception(
                            "%s footprint writer lock acquisition failed",
                            self.exchange,
                        )
                        acquired = False
                    next_lock_attempt = now + LEADERSHIP_RETRY_SECONDS
                    if acquired:
                        logger.info(
                            "%s footprint writer leadership acquired",
                            self.exchange,
                        )

                if leadership.acquired:
                    if source_epoch is None:
                        if not self._verify_writer_leadership(leadership):
                            return
                        source_epoch = self._allocate_source_epoch_with_retry()
                        if source_epoch is None:
                            return
                        if not self._verify_writer_leadership(leadership):
                            return
                        if self._outbox_directory:
                            self._bridge_recent_trades(router)
                            if not self._verify_writer_leadership(leadership):
                                return
                    if router.activate():
                        activated = True
                        overlap_verified = router.activation_overlap_verified
                        self._set_handoff_verified(
                            overlap_verified,
                            required=True,
                        )
                        self._set_writer_active(True)
                        activated_at = time.monotonic()
                        next_leadership_verify = (
                            activated_at + LEADERSHIP_VERIFY_SECONDS
                        )
                        next_rotation = (
                            activated_at + self.stream_rotation_interval_seconds
                        )
                        logger.info(
                            "%s footprint writer active source_epoch=%s",
                            self.exchange,
                            source_epoch,
                        )
                        if not overlap_verified:
                            self._record_coverage_issue("unverified_activation")
                            logger.warning(
                                "%s footprint handoff has no verified overlap "
                                "source_epoch=%s",
                                self.exchange,
                                source_epoch,
                            )
                    elif not self._outbox_directory:
                        try:
                            leadership.close()
                        except Exception as exc:
                            self._writer_fenced.set()
                            self._set_fatal(exc)
                            return
                        source_epoch = None
                self._stream_stop_event.wait(COORDINATION_INTERVAL_SECONDS)
                continue

            if now >= next_leadership_verify:
                if not self._verify_writer_leadership(leadership):
                    return
                next_leadership_verify = now + LEADERSHIP_VERIFY_SECONDS

            if next_rotation is not None and now >= next_rotation:
                if self._schedule_rotation(router):
                    next_rotation = now + self.stream_rotation_interval_seconds

            self._stream_stop_event.wait(COORDINATION_INTERVAL_SECONDS)

    def _schedule_rotation(self, router: _HotStandbyRouter) -> bool:
        slot = router.rotation_candidate()
        if slot is None or not self._request_stream_rotation(slot):
            return False
        logger.info(
            "%s execution feed scheduled rotation slot=%s",
            self.exchange,
            slot,
        )
        return True

    def _bridge_recent_trades(self, router):
        """Fetch outside the router lock; admit only a bridge with two shared IDs."""
        from footprints.recovery import recent_trades

        with router.lock:
            slot = router.leader_slot
            if slot is None:
                return
            starts = {
                key: rows[0] for key, rows in router.slots[slot].buffers.items() if rows
            }
        for key, first in starts.items():
            market = self._markets[key]
            known = self._recent_trades.get(market.market_id, {})
            if not known or first.trade_id in known:
                continue
            tail = next(reversed(known))
            try:
                snapshot = recent_trades(
                    self.exchange, self.instruments[key].symbol, key
                )
                identities = [trade.trade_id for trade in snapshot]
                left, right = identities.index(tail), identities.index(first.trade_id)
                if snapshot[right] != first:
                    raise ValueError(
                        "recent snapshot disagrees with the buffered endpoint"
                    )
                if left >= right or len(set(identities)) != len(identities):
                    raise ValueError(
                        "recent snapshot does not bridge the stored and buffered tails"
                    )
                bridge = snapshot[left:right]
                if self.exchange == "binance":
                    ids = [int(value) for value in identities[left : right + 1]]
                    if any(b != a + 1 for a, b in zip(ids, ids[1:])):
                        raise ValueError("recent Binance snapshot has an ID gap")
                sequences = [
                    trade.venue_sequence for trade in snapshot[left : right + 1]
                ]
                if sequences != sorted(sequences):
                    raise ValueError("recent snapshot source order regressed")
                with router.lock:
                    rows = router.slots[slot].buffers.get(key)
                    if router.leader_slot != slot or not rows or rows[0] != first:
                        continue
                    # Replay the known endpoint too, to verify its immutable fields.
                    if len(rows) + len(bridge) > STANDBY_BUFFER_TRADES:
                        raise ValueError("recent bridge exceeds warm buffer capacity")
                    rows.extendleft(reversed(bridge))
                logger.info(
                    "%s public REST bridge prepared trades=%s",
                    self.exchange,
                    len(bridge) - 1,
                )
            except Exception as exc:
                self._record_coverage_issue("recent_bridge_unavailable")
                logger.warning(
                    "%s recent bridge unavailable; archive verification remains pending: %s",
                    self.exchange,
                    exc,
                )

    def _verify_writer_leadership(
        self,
        leadership: FootprintWriterLeadership,
    ) -> bool:
        try:
            leadership.verify()
        except Exception as exc:
            self._writer_fenced.set()
            self._set_writer_active(False)
            self._set_fatal(exc)
            return False
        return True

    def _join_writer_with_leadership(
        self,
        writer: threading.Thread,
        leadership: FootprintWriterLeadership,
    ) -> None:
        next_leadership_verify = 0.0
        while writer.is_alive():
            now = time.monotonic()
            if leadership.acquired and now >= next_leadership_verify:
                if not self._verify_writer_leadership(leadership):
                    break
                next_leadership_verify = now + LEADERSHIP_VERIFY_SECONDS
            writer.join(timeout=COORDINATION_INTERVAL_SECONDS)
        writer.join()

    def _stream_stopping(self) -> bool:
        return (
            self._fatal_error is not None
            or self.stop_event.is_set()
            or self._stream_stop_event.is_set()
        )

    def _register_stream_attempt(
        self,
        slot: int,
        stop_event: threading.Event,
    ) -> None:
        with self._feed_status_lock:
            self._active_stream_stop_events[slot] = stop_event
            if self.stop_event.is_set() or self._stream_stop_event.is_set():
                stop_event.set()

    def _unregister_stream_attempt(
        self,
        slot: int,
        stop_event: threading.Event,
    ) -> bool:
        with self._feed_status_lock:
            if self._active_stream_stop_events.get(slot) is stop_event:
                self._active_stream_stop_events.pop(slot, None)
            planned = stop_event in self._planned_stream_stop_events
            self._planned_stream_stop_events.discard(stop_event)
            return planned

    def _request_stream_rotation(self, slot: int) -> bool:
        with self._feed_status_lock:
            stop_event = self._active_stream_stop_events.get(slot)
            if stop_event is None or stop_event.is_set():
                return False
            self._planned_stream_stop_events.add(stop_event)
            stop_event.set()
            return True

    def _stop_active_streams(self) -> None:
        self._set_writer_active(False)
        self._set_feeds_ready(False)
        self._set_handoff_verified(False)
        self._stream_stop_event.set()
        with self._feed_status_lock:
            members = tuple(self._active_stream_stop_events.values())
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
        while not self._stream_stopping():
            try:
                return self._allocate_source_epoch()
            except Exception as exc:
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
                if self._stream_stop_event.wait(delay) or self.stop_event.is_set():
                    return None
        return None

    def _allocate_source_epoch(self) -> int:
        market_ids = sorted(self._market_scales)
        if not market_ids:
            raise RuntimeError("cannot allocate a source epoch without markets")
        if self._outbox_directory and self._outbox is None:
            self._outbox = ExecutionOutbox(
                self._outbox_directory,
                max_bytes=int(
                    os.environ.get("FOOTPRINT_OUTBOX_MAX_BYTES", 2 * 1024**3)
                ),
                reserve_bytes=int(
                    os.environ.get("FOOTPRINT_DISK_RESERVE_BYTES", 5 * 1024**3)
                ),
            )
        if self._outbox is not None and self._outbox.state("epoch") is not None:
            observed = self._outbox.state("epoch")
            recent = self._outbox.recent()
        else:
            store = self._ensure_execution_store()
            recent = store.recent_execution_identities(
                market_ids, per_market_limit=self.recent_trade_limit
            )
            self._seed_recent_trades(recent)
            observed = store.max_source_epoch(market_ids)
            if self._outbox is not None:
                self._outbox.seed(observed, recent)
        self._seed_recent_trades(recent)
        source_epoch = max(observed, self._last_source_epoch) + 1
        if source_epoch > _MAX_SOURCE_EPOCH:
            raise OverflowError("ClickHouse execution source epoch exhausted UInt32")
        with self._ingest_lock:
            self._connection_source_epoch = source_epoch
            self._last_source_epoch = source_epoch
            self._next_hyperliquid_sequence = 0
            self._bybit_sequence_order.clear()
            self._last_admitted_cursors.clear()
        if self._outbox is not None:
            self._outbox.set_state("epoch", source_epoch)
            self._outbox_ready.set()
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
            if self._outbox is not None:
                self._outbox.set_state("epoch", source_epoch)
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
        if self._outbox_directory:
            self._journal_loop()
            return
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
            if self._writer_fenced.is_set():
                return False
            try:
                self._ensure_execution_store().insert_batch(batch)
                if self._writer_fenced.is_set():
                    return False
                return True
            except Exception as exc:
                if self._writer_fenced.is_set():
                    return False
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

    def _journal_loop(self) -> None:
        drainer = threading.Thread(target=self._drain_outbox, daemon=False)
        drainer.start()
        next_health = 0.0
        try:
            while not self.stop_event.is_set() or not self.queue.empty():
                if time.monotonic() >= next_health:
                    self._write_outbox_health()
                    next_health = time.monotonic() + 5
                try:
                    first = self.queue.get(timeout=0.1)
                except queue.Empty:
                    continue
                batch = [first]
                deadline = time.monotonic() + 0.1
                while len(batch) < self.commit_batch_size:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        break
                    try:
                        batch.append(self.queue.get(timeout=remaining))
                    except queue.Empty:
                        break
                try:
                    if self._outbox is None:
                        raise RuntimeError(
                            "execution admitted before outbox initialization"
                        )
                    self._outbox.append(batch)
                except Exception as exc:
                    self._uncommitted_batch_count += len(batch)
                    self._set_fatal(exc)
                    break
                finally:
                    for _ in batch:
                        self.queue.task_done()
        except Exception as exc:
            self._set_fatal(exc)
        finally:
            drainer.join()
            self._write_outbox_health()

    def _drain_outbox(self) -> None:
        reconcile = True
        attempt = 0
        while not self.stop_event.is_set() and not self._writer_fenced.is_set():
            if not self._outbox_ready.wait(0.1):
                continue
            try:
                pending = self._outbox.first()
                if pending is None:
                    self.stop_event.wait(0.1)
                    continue
                batch_id, batch = pending
                store = self._ensure_execution_store()
                missing = store.missing_executions(batch) if reconcile else batch
                if self._writer_fenced.is_set() or self.stop_event.is_set():
                    return
                if missing:
                    store.insert_batch(missing)
                if self._writer_fenced.is_set():
                    return
                self._outbox.acknowledge(batch_id)
                self._notify_committed_executions(batch)
                reconcile, attempt = False, 0
            except Exception as exc:
                if (
                    classify_clickhouse_error(exc, is_write=True)
                    == ClickHouseFailureKind.FATAL
                ):
                    self._set_fatal(exc)
                    return
                self._close_clickhouse()
                reconcile = True
                attempt += 1
                logger.warning(
                    "%s delivery delayed; durable capture continues: %s",
                    self.exchange,
                    exc,
                )
                self.stop_event.wait(self._retry_delay(attempt))

    def _write_outbox_health(self) -> None:
        if self._state_directory is None:
            return
        try:
            import json

            state = {
                "at": time.time(),
                "last_trade_at": self._last_trade_at,
                "capture": not self.stop_event.is_set(),
                "fatal": str(self._fatal_error or ""),
                "initialized": self._outbox_ready.is_set(),
            }
            if self._outbox is not None:
                state.update(self._outbox.stats())
                if self._coverage_issue is not None:
                    self._outbox.set_state("coverage_issue", self._coverage_issue)
                state["coverage_issue"] = self._outbox.state("coverage_issue")
            from footprints.collectors.rejected import rejection_stats

            state.update(rejection_stats(self.exchange))
            self._state_directory.mkdir(parents=True, exist_ok=True)
            temporary = self._state_directory / "health.json.tmp"
            temporary.write_text(json.dumps(state))
            temporary.replace(self._state_directory / "health.json")
        except Exception:
            logger.exception("Failed to write %s outbox health", self.exchange)

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

    def _wait_until_drained(
        self,
        leadership: FootprintWriterLeadership,
    ) -> bool:
        next_leadership_verify = 0.0
        while self._fatal_error is None:
            with self.queue.all_tasks_done:
                if self.queue.unfinished_tasks == 0:
                    return True
                self.queue.all_tasks_done.wait(timeout=0.1)
            now = time.monotonic()
            if leadership.acquired and now >= next_leadership_verify:
                if not self._verify_writer_leadership(leadership):
                    return False
                next_leadership_verify = now + LEADERSHIP_VERIFY_SECONDS
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
