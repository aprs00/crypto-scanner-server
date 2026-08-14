from __future__ import annotations

import logging
import time
from collections import deque
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone as dt_timezone
from typing import Any

from django.db import transaction
from django.db.models import Q

from footprints.chart_live import publish_chart_error, publish_chart_update
from footprints.chart_serialization import serialize_active_bar, serialize_chart_bar
from footprints.constants import (
    CHART_BAR_INSERT_CHUNK,
    CHART_BUILD_EXECUTION_CHUNK,
    CHART_DEFAULT_LOOKBACK_DAYS,
    CHART_LIVE_MAX_LEVELS,
    CHART_MAX_LEVELS_PER_BAR,
    CHART_MAX_SERIES_PER_MARKET,
    CHART_SERIES_LEASE_SECONDS,
    CHART_SNAPSHOT_BARS,
)
from footprints.models import (
    ChartSeries,
    ChartSeriesStatus,
    ChartType,
    FootprintMarket,
    InstrumentSpec,
)
from footprints.storage.charts import (
    ChartLevel,
    ClickHouseChartStore,
    ClickHouseProjectorExecutionStore,
    DetailedChartBar,
    ProjectorCursor,
    ProjectorExecution,
    ProjectorSpecDivisors,
)
from footprints.storage.clickhouse import (
    ClickHouseAccess,
    ClickHouseFailureKind,
    ClickHouseSettings,
    classify_clickhouse_error,
    create_clickhouse_client,
)
from footprints.storage.clickhouse_schema import ensure_clickhouse_schema
from footprints.tick import TickConfig, TickEngine
from footprints.trev import TrevConfig, TrevEngine, TrevExpansionLimitError


logger = logging.getLogger(__name__)

LIVE_EXECUTION_LIMIT = 50_000
LIVE_UPDATE_CHUNK_BARS = 50


class ChartDensityLimitError(ValueError):
    """A native footprint bar exceeds the public V0 payload envelope."""


class TransientChartBuildError(RuntimeError):
    """A queued build can be replayed after a short storage backoff."""


@dataclass(slots=True)
class ChartStores:
    client: Any
    executions: ClickHouseProjectorExecutionStore
    bars: ClickHouseChartStore

    def close(self) -> None:
        self.client.close()


def open_chart_stores(*, role: str) -> ChartStores:
    configuration = ClickHouseSettings.from_environment()
    client = create_clickhouse_client(
        configuration,
        role=role,
        for_inserts=True,
        bootstrap_database=True,
    )
    access = ClickHouseAccess(client)
    try:
        ensure_clickhouse_schema(access, configuration.database)
        return ChartStores(
            client=client,
            executions=ClickHouseProjectorExecutionStore(
                access,
                database=configuration.database,
                spec_divisors=_projector_spec_divisors(),
                load_spec_divisor=_projector_spec_divisor,
            ),
            bars=ClickHouseChartStore(access, database=configuration.database),
        )
    except Exception:
        access.close()
        raise


def recover_interrupted_builds() -> int:
    return ChartSeries.objects.filter(status=ChartSeriesStatus.BUILDING).update(
        status=ChartSeriesStatus.QUEUED,
        error_code="",
    )


def series_lease_cutoff(value: datetime) -> datetime:
    return value - timedelta(seconds=CHART_SERIES_LEASE_SECONDS)


def expire_stale_series(*, now: datetime, market_id: int | None = None) -> int:
    rows = ChartSeries.objects.filter(last_requested_at__lt=series_lease_cutoff(now))
    if market_id is not None:
        rows = rows.filter(market_id=market_id)
    deleted, _details = rows.exclude(status=ChartSeriesStatus.BUILDING).delete()
    return deleted


def request_chart_series(
    *,
    market: FootprintMarket,
    chart_type: str,
    parameter_a: int,
    parameter_b: int,
    lookback_days: int,
    now: datetime,
) -> ChartSeries | None:
    """Create or renew one bounded shared configuration for a market."""

    with transaction.atomic():
        locked_market = FootprintMarket.objects.select_for_update().get(pk=market.pk)
        expire_stale_series(now=now, market_id=locked_market.pk)
        series = ChartSeries.objects.filter(
            market=locked_market,
            chart_type=chart_type,
            parameter_a=parameter_a,
            parameter_b=parameter_b,
            lookback_days=lookback_days,
        ).first()
        if series is not None:
            series.last_requested_at = now
            series.save(update_fields=["last_requested_at"])
            return series
        live_count = ChartSeries.objects.filter(
            Q(last_requested_at__gte=series_lease_cutoff(now))
            | Q(status=ChartSeriesStatus.BUILDING),
            market=locked_market,
        ).count()
        if live_count >= CHART_MAX_SERIES_PER_MARKET:
            return None
        return ChartSeries.objects.create(
            market=locked_market,
            chart_type=chart_type,
            parameter_a=parameter_a,
            parameter_b=parameter_b,
            lookback_days=lookback_days,
            anchor_at=lookback_anchor(now, lookback_days),
            last_requested_at=now,
        )


def refresh_series_lease(series_key: str, *, now: datetime) -> str | None:
    """Renew an existing live lease and return its lifecycle status."""

    updated = ChartSeries.objects.filter(
        public_key=series_key,
        last_requested_at__gte=series_lease_cutoff(now),
    ).update(last_requested_at=now)
    if not updated:
        return None
    status = (
        ChartSeries.objects.filter(public_key=series_key)
        .values_list("status", flat=True)
        .first()
    )
    return None if status is None else str(status)


def claim_queued_market_series() -> tuple[int, ...]:
    """Claim or resume every build for the oldest waiting market."""

    cutoff = series_lease_cutoff(datetime.now(tz=dt_timezone.utc))
    with transaction.atomic():
        oldest = (
            ChartSeries.objects.select_for_update(skip_locked=True)
            .filter(
                status__in=(ChartSeriesStatus.QUEUED, ChartSeriesStatus.BUILDING),
                last_requested_at__gte=cutoff,
            )
            .order_by("created_at", "pk")
            .first()
        )
        if oldest is None:
            return ()
        series_ids = tuple(
            ChartSeries.objects.select_for_update(skip_locked=True)
            .filter(
                market_id=oldest.market_id,
                status__in=(ChartSeriesStatus.QUEUED, ChartSeriesStatus.BUILDING),
                last_requested_at__gte=cutoff,
            )
            .order_by("created_at", "pk")
            .values_list("pk", flat=True)[:CHART_MAX_SERIES_PER_MARKET]
        )
        ChartSeries.objects.filter(pk__in=series_ids).update(
            status=ChartSeriesStatus.BUILDING,
            error_code="",
        )
        return tuple(int(series_id) for series_id in series_ids)


def build_next_queued_series(
    *,
    execution_store: ClickHouseProjectorExecutionStore,
    bar_store: ClickHouseChartStore,
) -> bool:
    series_ids = claim_queued_market_series()
    if not series_ids:
        return False
    try:
        build_series_batch(
            series_ids,
            execution_store=execution_store,
            bar_store=bar_store,
        )
    except Exception as exc:
        if _is_transient_build_error(exc):
            for series_id in series_ids:
                _requeue_build(series_id)
            raise TransientChartBuildError("chart storage is temporarily unavailable") from exc
        for series_id in series_ids:
            _mark_build_failed(series_id, exc)
        raise
    return True


def build_series(
    series_id: int,
    *,
    execution_store: ClickHouseProjectorExecutionStore,
    bar_store: ClickHouseChartStore,
) -> None:
    """Process one resumable history chunk for a claimed chart series."""

    build_series_batch(
        (series_id,),
        execution_store=execution_store,
        bar_store=bar_store,
    )


@dataclass(slots=True)
class _PendingBuild:
    series: ChartSeries
    engine: TickEngine | TrevEngine
    anchor_timestamp_us: int
    cursor: ProjectorCursor | None = None
    error: Exception | None = None


def _pending_build(series: ChartSeries) -> _PendingBuild:
    if series.checkpoint_state is None:
        engine = _new_engine(series)
        cursor = None
    else:
        engine, cursor = _decode_checkpoint(series)
    return _PendingBuild(
        series=series,
        engine=engine,
        anchor_timestamp_us=_datetime_us(series.anchor_at),
        cursor=cursor,
    )


def build_series_batch(
    series_ids: Iterable[int],
    *,
    execution_store: ClickHouseProjectorExecutionStore,
    bar_store: ClickHouseChartStore,
) -> None:
    """Replay one bounded market chunk for all pending configurations."""

    identities = tuple(dict.fromkeys(series_ids))
    if not identities:
        raise ValueError("at least one chart series is required")
    series_rows = list(
        ChartSeries.objects.filter(pk__in=identities).order_by("created_at", "pk")
    )
    if {series.pk for series in series_rows} != set(identities):
        raise RuntimeError("one or more chart series disappeared during build")
    if any(series.status != ChartSeriesStatus.BUILDING for series in series_rows):
        raise RuntimeError("chart series is no longer claimed")
    market_ids = {series.market_id for series in series_rows}
    if len(market_ids) != 1:
        raise ValueError("one chart build batch must belong to one market")

    started = time.monotonic()
    pending = [_pending_build(series) for series in series_rows]
    start_timestamp_us = min(item.anchor_timestamp_us for item in pending)
    cursors = [item.cursor for item in pending]
    after_cursor = (
        min(cursor for cursor in cursors if cursor is not None)
        if cursors and all(cursor is not None for cursor in cursors)
        else None
    )
    executions = execution_store.iter_executions(
        market_id=series_rows[0].market_id,
        start_timestamp_us=start_timestamp_us,
        after_cursor=after_cursor,
        limit=CHART_BUILD_EXECUTION_CHUNK,
    )
    execution_count = 0
    stored_bar_count = 0
    bar_batch: list[DetailedChartBar] = []
    for execution in executions:
        execution_count += 1
        execution_cursor = execution.cursor
        for item in pending:
            if item.error is not None:
                continue
            if item.cursor is not None and execution_cursor <= item.cursor:
                continue
            if execution.timestamp_us < item.anchor_timestamp_us:
                item.cursor = execution_cursor
                continue
            try:
                if (
                    item.engine.state.last_order is not None
                    and item.cursor is not None
                    and execution.source_epoch != item.cursor[0]
                ):
                    segment_bars = item.engine.truncate_segment().bars
                else:
                    segment_bars = ()
                execution_bars = _push_execution(item.engine, execution).bars
                if segment_bars or execution_bars:
                    stored = [
                        _stored_bar(item.series, bar)
                        for bar in (*segment_bars, *execution_bars)
                    ]
                else:
                    stored = ()
            except Exception as exc:
                item.error = exc
                continue
            item.cursor = execution_cursor
            bar_batch.extend(stored)
            if len(bar_batch) >= CHART_BAR_INSERT_CHUNK:
                stored_bar_count += bar_store.append(bar_batch)
                bar_batch.clear()

    stored_bar_count += bar_store.append(bar_batch)
    complete = execution_count < CHART_BUILD_EXECUTION_CHUNK

    with transaction.atomic():
        locked_rows = {
            series.pk: series
            for series in ChartSeries.objects.select_for_update().filter(
                pk__in=identities
            )
        }
        if set(locked_rows) != set(identities) or any(
            series.status != ChartSeriesStatus.BUILDING
            for series in locked_rows.values()
        ):
            raise RuntimeError("chart series changed during build")
        for item in pending:
            locked = locked_rows[item.series.pk]
            if item.error is None:
                locked.status = (
                    ChartSeriesStatus.READY
                    if complete
                    else ChartSeriesStatus.BUILDING
                )
                locked.checkpoint_state = _checkpoint_state(
                    item.engine,
                    item.cursor,
                )
                locked.error_code = ""
            else:
                locked.status = ChartSeriesStatus.FAILED
                locked.checkpoint_state = None
                locked.error_code = _error_code(item.error)
            locked.save(
                update_fields=[
                    "status",
                    "checkpoint_state",
                    "error_code",
                ]
            )
            _RUNTIMES.pop(item.series.pk, None)

    elapsed_ms = round((time.monotonic() - started) * 1_000, 2)
    logger.info(
        "Processed chart build chunk market_id=%s series=%s executions=%s "
        "bars=%s complete=%s elapsed_ms=%s",
        series_rows[0].market_id,
        len(pending),
        execution_count,
        stored_bar_count,
        complete,
        elapsed_ms,
    )


@dataclass(slots=True)
class SeriesRuntime:
    engine: TickEngine | TrevEngine
    cursor: ProjectorCursor | None


def advance_ready_series(
    *,
    execution_store: ClickHouseProjectorExecutionStore,
    bar_store: ClickHouseChartStore,
    market_ids: Iterable[int] | None = None,
) -> int:
    """Read each market tail once and fan it into every ready chart engine."""

    selected_market_ids = None
    if market_ids is not None:
        selected_market_ids = tuple(sorted(set(market_ids)))
        if not selected_market_ids:
            return 0
        if any(
            isinstance(market_id, bool)
            or not isinstance(market_id, int)
            or market_id <= 0
            for market_id in selected_market_ids
        ):
            raise ValueError("market_ids must contain positive integers")

    query = ChartSeries.objects.filter(
        status=ChartSeriesStatus.READY,
        last_requested_at__gte=series_lease_cutoff(
            datetime.now(tz=dt_timezone.utc)
        ),
    )
    if selected_market_ids is not None:
        query = query.filter(market_id__in=selected_market_ids)
    rows = list(query.order_by("market_id", "pk"))
    if selected_market_ids is None:
        ready_ids = {series.pk for series in rows}
        for series_id in list(_RUNTIMES):
            if series_id not in ready_ids:
                _RUNTIMES.pop(series_id, None)

    by_market: dict[int, list[tuple[ChartSeries, SeriesRuntime]]] = {}
    for series in rows:
        try:
            runtime = _runtime_for(series)
        except Exception:
            logger.exception("Invalid checkpoint for chart series %s", series.public_key)
            runtime = None
        if runtime is None:
            ChartSeries.objects.filter(pk=series.pk).update(
                status=ChartSeriesStatus.QUEUED,
                error_code="MISSING_CHECKPOINT",
                checkpoint_state=None,
            )
            _RUNTIMES.pop(series.pk, None)
            continue
        by_market.setdefault(series.market_id, []).append((series, runtime))

    processed_total = 0
    for market_id, entries in by_market.items():
        try:
            processed_total += _advance_market(
                market_id,
                entries,
                execution_store=execution_store,
                bar_store=bar_store,
            )
        except Exception:
            for series, _runtime in entries:
                _RUNTIMES.pop(series.pk, None)
            raise
    return processed_total


def _advance_market(
    market_id: int,
    entries: list[tuple[ChartSeries, SeriesRuntime]],
    *,
    execution_store: ClickHouseProjectorExecutionStore,
    bar_store: ClickHouseChartStore,
) -> int:
    earliest_cursor = None
    cursors = [runtime.cursor for _series, runtime in entries]
    if cursors and all(cursor is not None for cursor in cursors):
        earliest_cursor = min(cursor for cursor in cursors if cursor is not None)
    start_timestamp_us = min(
        _datetime_us(series.anchor_at)
        for series, _runtime in entries
    )
    stream = execution_store.iter_executions(
        market_id=market_id,
        start_timestamp_us=start_timestamp_us,
        after_cursor=earliest_cursor,
        limit=LIVE_EXECUTION_LIMIT,
    )
    emitted: dict[int, deque[DetailedChartBar]] = {
        series.pk: deque(maxlen=CHART_SNAPSHOT_BARS)
        for series, _runtime in entries
    }
    bar_batch: list[DetailedChartBar] = []
    touched: set[int] = set()
    raw_count = 0
    failed: dict[int, Exception] = {}
    for execution in stream:
        raw_count += 1
        execution_cursor = execution.cursor
        for series, runtime in entries:
            if series.pk in failed:
                continue
            if runtime.cursor is not None and execution_cursor <= runtime.cursor:
                continue
            try:
                if (
                    runtime.cursor is not None
                    and execution.source_epoch != runtime.cursor[0]
                ):
                    for bar in runtime.engine.truncate_segment().bars:
                        stored = _stored_bar(series, bar)
                        emitted[series.pk].append(stored)
                        bar_batch.append(stored)
                for bar in _push_execution(runtime.engine, execution).bars:
                    stored = _stored_bar(series, bar)
                    emitted[series.pk].append(stored)
                    bar_batch.append(stored)
                runtime.cursor = execution_cursor
                touched.add(series.pk)
            except Exception as exc:
                failed[series.pk] = exc
        if len(bar_batch) >= CHART_BAR_INSERT_CHUNK:
            bar_store.append(bar_batch)
            bar_batch.clear()

    bar_store.append(bar_batch)

    publications: list[
        tuple[ChartSeries, SeriesRuntime, deque[DetailedChartBar]]
    ] = []
    with transaction.atomic():
        for series, runtime in entries:
            if series.pk in failed:
                error = failed[series.pk]
                ChartSeries.objects.filter(pk=series.pk).update(
                    status=ChartSeriesStatus.FAILED,
                    error_code=_error_code(error),
                    checkpoint_state=None,
                )
                _RUNTIMES.pop(series.pk, None)
                continue
            if series.pk not in touched:
                continue
            new_bars = emitted[series.pk]
            checkpoint = _checkpoint_state(
                runtime.engine,
                runtime.cursor,
            )
            updated = ChartSeries.objects.filter(
                pk=series.pk,
                status=ChartSeriesStatus.READY,
            ).update(
                checkpoint_state=checkpoint,
                error_code="",
            )
            if updated:
                publications.append((series, runtime, new_bars))
            else:
                _RUNTIMES.pop(series.pk, None)

    for series, _runtime in entries:
        error = failed.get(series.pk)
        if error is None:
            continue
        try:
            publish_chart_error(str(series.public_key), _error_code(error))
        except Exception:
            logger.exception("Failed to publish chart error for %s", series.public_key)
    if not publications:
        return raw_count

    specs = {
        spec.pk: spec for spec in InstrumentSpec.objects.filter(market_id=market_id)
    }
    for series, runtime, new_bars in publications:
        try:
            serialized = [serialize_chart_bar(bar, specs) for bar in new_bars]
            active = _serialize_active(runtime.engine, specs)
            if not serialized:
                publish_chart_update(
                    str(series.public_key),
                    bars=[],
                    active_bar=active,
                )
                continue
            for chunk in _live_update_chunks(serialized, active):
                publish_chart_update(
                    str(series.public_key),
                    bars=chunk,
                    active_bar=active,
                )
        except Exception:
            logger.exception("Failed to publish chart update for %s", series.public_key)
    return raw_count


def _live_update_chunks(bars: list[dict], active: dict | None):
    active_levels = len(active["levels"]) if active is not None else 0
    chunk: list[dict] = []
    chunk_levels = active_levels
    for bar in bars:
        bar_levels = len(bar["levels"])
        if chunk and (
            len(chunk) >= LIVE_UPDATE_CHUNK_BARS
            or chunk_levels + bar_levels > CHART_LIVE_MAX_LEVELS
        ):
            yield chunk
            chunk = []
            chunk_levels = active_levels
        if chunk_levels + bar_levels > CHART_LIVE_MAX_LEVELS:
            raise ChartDensityLimitError(
                f"live update exceeds {CHART_LIVE_MAX_LEVELS} native levels"
            )
        chunk.append(bar)
        chunk_levels += bar_levels
    if chunk:
        yield chunk


def active_bar_for_series(series: ChartSeries):
    if not series.checkpoint_state:
        return None
    engine, _cursor = _decode_checkpoint(series)
    active = engine.active_snapshot()
    _ensure_native_level_limit(active)
    return active


def _runtime_for(series: ChartSeries) -> SeriesRuntime | None:
    current = _RUNTIMES.get(series.pk)
    if current is not None:
        return current
    if not series.checkpoint_state:
        return None
    engine, cursor = _decode_checkpoint(series)
    runtime = SeriesRuntime(
        engine=engine,
        cursor=cursor,
    )
    _RUNTIMES[series.pk] = runtime
    return runtime


def _new_engine(series: ChartSeries) -> TickEngine | TrevEngine:
    if series.chart_type == ChartType.TICK:
        return TickEngine(TickConfig(series.parameter_a))
    if series.chart_type == ChartType.TREV:
        return TrevEngine(TrevConfig(series.parameter_a, series.parameter_b))
    raise ValueError("UNSUPPORTED_CHART_TYPE")


def _push_execution(
    engine: TickEngine | TrevEngine,
    execution: ProjectorExecution,
):
    rank = 0 if engine.state.last_order is None else engine.state.last_order.rank + 1
    update = engine.push_raw(
        timestamp_us=execution.timestamp_us,
        rank=rank,
        price_tick=execution.price_tick,
        quantity_units=execution.quantity_units,
        is_taker_buy=execution.is_taker_buy,
        spec_id=execution.instrument_spec_id,
    )
    active = engine.state.active
    if active is not None and len(active.levels) > CHART_MAX_LEVELS_PER_BAR:
        raise ChartDensityLimitError(
            f"active chart bar exceeds {CHART_MAX_LEVELS_PER_BAR} native levels"
        )
    return update


def _stored_bar(series: ChartSeries, bar) -> DetailedChartBar:
    if len(bar.levels) > CHART_MAX_LEVELS_PER_BAR:
        raise ChartDensityLimitError(
            f"closed chart bar exceeds {CHART_MAX_LEVELS_PER_BAR} native levels"
        )
    opened_at = _datetime_from_us(bar.opened_timestamp_us)
    ended_at = _datetime_from_us(max(bar.opened_timestamp_us, bar.ended_timestamp_us))
    return DetailedChartBar(
        series_id=series.pk,
        bar_index=int(bar.bar_index),
        opened_at=opened_at,
        ended_at=ended_at,
        instrument_spec_id=int(bar.spec_id),
        open_tick=int(bar.open_tick),
        high_tick=int(bar.high_tick),
        low_tick=int(bar.low_tick),
        close_tick=int(bar.close_tick),
        buy_units=int(bar.buy_units),
        sell_units=int(bar.sell_units),
        trade_count=int(bar.trade_count),
        break_before=bool(bar.break_before),
        levels=tuple(
            ChartLevel(
                price_tick=int(level.price_tick),
                buy_units=int(level.buy_units),
                sell_units=int(level.sell_units),
            )
            for level in bar.levels
        ),
    )


def _checkpoint_state(
    engine: TickEngine | TrevEngine,
    cursor: ProjectorCursor | None,
) -> dict:
    return {
        "engine": engine.snapshot(),
        "cursor": list(cursor) if cursor is not None else None,
    }


def _decode_checkpoint(
    series: ChartSeries,
) -> tuple[TickEngine | TrevEngine, ProjectorCursor | None]:
    wrapper = series.checkpoint_state
    if not isinstance(wrapper, dict):
        raise ValueError("CHART_CHECKPOINT_SCHEMA")
    fresh = _new_engine(series)
    if series.chart_type == ChartType.TICK:
        engine = TickEngine.restore(wrapper.get("engine"), config=fresh.config)
    else:
        engine = TrevEngine.restore(wrapper.get("engine"), config=fresh.config)
    cursor_value = wrapper.get("cursor")
    cursor = None if cursor_value is None else _parse_cursor_value(cursor_value)
    return engine, cursor


def _parse_cursor_value(value: object) -> ProjectorCursor:
    if not isinstance(value, (list, tuple)) or len(value) != 5:
        raise ValueError("CHART_CHECKPOINT_CURSOR")
    source_epoch, sequence, batch_index, timestamp_us, trade_id = value
    if (
        any(isinstance(item, bool) or not isinstance(item, int) for item in value[:4])
        or source_epoch <= 0
        or sequence < 0
        or batch_index < 0
        or timestamp_us <= 0
        or not isinstance(trade_id, str)
        or not trade_id
    ):
        raise ValueError("CHART_CHECKPOINT_CURSOR")
    return source_epoch, sequence, batch_index, timestamp_us, trade_id


def _serialize_active(engine, specs: dict[int, InstrumentSpec]) -> dict | None:
    active = engine.active_snapshot()
    _ensure_native_level_limit(active)
    return None if active is None else serialize_active_bar(active, specs)


def _ensure_native_level_limit(bar) -> None:
    if bar is not None and len(bar.levels) > CHART_MAX_LEVELS_PER_BAR:
        raise ChartDensityLimitError(
            f"chart bar exceeds {CHART_MAX_LEVELS_PER_BAR} native levels"
        )


def _projector_spec_divisors() -> dict[int, ProjectorSpecDivisors]:
    result: dict[int, ProjectorSpecDivisors] = {}
    for spec in InstrumentSpec.objects.select_related("market"):
        price_divisor = _scaled_integer(spec.tick_size, int(spec.market.price_scale))
        quantity_divisor = _scaled_integer(
            spec.quantity_step,
            int(spec.market.quantity_scale),
        )
        result[spec.pk] = ProjectorSpecDivisors(price_divisor, quantity_divisor)
    if not result:
        raise ValueError("NO_INSTRUMENT_SPEC")
    return result


def _projector_spec_divisor(spec_id: int) -> ProjectorSpecDivisors | None:
    spec = InstrumentSpec.objects.select_related("market").filter(pk=spec_id).first()
    if spec is None:
        return None
    return ProjectorSpecDivisors(
        _scaled_integer(spec.tick_size, int(spec.market.price_scale)),
        _scaled_integer(spec.quantity_step, int(spec.market.quantity_scale)),
    )


def _scaled_integer(value, scale: int) -> int:
    scaled = value.scaleb(scale)
    integral = scaled.to_integral_value()
    if scaled != integral or integral <= 0:
        raise ValueError("instrument specification is not exact at storage scale")
    return int(integral)


def _mark_build_failed(series_id: int, error: Exception) -> None:
    ChartSeries.objects.filter(
        pk=series_id,
        status=ChartSeriesStatus.BUILDING,
    ).update(
        status=ChartSeriesStatus.FAILED,
        error_code=_error_code(error),
        checkpoint_state=None,
    )
    _RUNTIMES.pop(series_id, None)


def _requeue_build(series_id: int) -> None:
    ChartSeries.objects.filter(
        pk=series_id,
        status=ChartSeriesStatus.BUILDING,
    ).update(
        status=ChartSeriesStatus.QUEUED,
        error_code="",
    )
    _RUNTIMES.pop(series_id, None)


def _is_transient_build_error(error: Exception) -> bool:
    return (
        classify_clickhouse_error(error, is_write=False)
        == ClickHouseFailureKind.RETRYABLE
    )


def _error_code(error: Exception) -> str:
    if isinstance(error, TrevExpansionLimitError):
        return "TREV_EXPANSION_LIMIT"
    if isinstance(error, ChartDensityLimitError):
        return "CHART_BAR_TOO_DENSE"
    message = str(error)
    if message.isupper() and len(message) <= 64:
        return message
    return type(error).__name__[:64] or "CHART_BUILD_FAILED"


def lookback_anchor(
    value: datetime,
    lookback_days: int = CHART_DEFAULT_LOOKBACK_DAYS,
) -> datetime:
    return (value - timedelta(days=lookback_days)).replace(
        minute=0,
        second=0,
        microsecond=0,
    )


def _datetime_us(value: datetime) -> int:
    return int(value.timestamp() * 1_000_000)


def _datetime_from_us(value: int) -> datetime:
    return datetime.fromtimestamp(value / 1_000_000, tz=dt_timezone.utc)


_RUNTIMES: dict[int, SeriesRuntime] = {}
