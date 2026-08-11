from __future__ import annotations

import logging
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone as dt_timezone
from typing import Any

from django.db import transaction
from django.db.models import Q

from footprints.chart_live import publish_chart_error, publish_chart_update
from footprints.chart_serialization import serialize_active_bar, serialize_chart_bar
from footprints.constants import (
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
        checkpoint_state=None,
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
            anchor_at=lookback_anchor(now),
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


def claim_queued_series() -> int | None:
    with transaction.atomic():
        series = (
            ChartSeries.objects.select_for_update(skip_locked=True)
            .filter(
                status=ChartSeriesStatus.QUEUED,
                last_requested_at__gte=series_lease_cutoff(
                    datetime.now(tz=dt_timezone.utc)
                ),
            )
            .order_by("created_at", "pk")
            .first()
        )
        if series is None:
            return None
        series.status = ChartSeriesStatus.BUILDING
        series.error_code = ""
        series.save(update_fields=["status", "error_code"])
        return int(series.pk)


def build_next_queued_series(
    *,
    execution_store: ClickHouseProjectorExecutionStore,
    bar_store: ClickHouseChartStore,
) -> bool:
    series_id = claim_queued_series()
    if series_id is None:
        return False
    try:
        build_series(
            series_id,
            execution_store=execution_store,
            bar_store=bar_store,
        )
    except Exception as exc:
        if _is_transient_build_error(exc):
            _requeue_build(series_id)
            raise TransientChartBuildError("chart storage is temporarily unavailable") from exc
        _mark_build_failed(series_id, exc)
        raise
    return True


def build_series(
    series_id: int,
    *,
    execution_store: ClickHouseProjectorExecutionStore,
    bar_store: ClickHouseChartStore,
) -> None:
    """Replay the fixed V0 lookback and publish one ready chart series."""

    series = ChartSeries.objects.get(pk=series_id)
    if series.status != ChartSeriesStatus.BUILDING:
        raise RuntimeError("chart series is no longer claimed")

    engine = _new_engine(series)
    writer = _BuildWriter(series, bar_store)
    cursor: ProjectorCursor | None = None
    executions = execution_store.iter_executions(
        market_id=series.market_id,
        start_timestamp_us=_datetime_us(series.anchor_at),
    )
    for execution in executions:
        if cursor is not None and execution.source_epoch != cursor[0]:
            writer.add_many(engine.truncate_segment().bars)
        writer.add_many(_push_execution(engine, execution).bars)
        cursor = execution.cursor
    writer.finish()
    checkpoint = _checkpoint_state(engine, cursor)

    with transaction.atomic():
        locked = ChartSeries.objects.select_for_update().get(pk=series.pk)
        if locked.status != ChartSeriesStatus.BUILDING:
            raise RuntimeError("chart series changed during build")
        locked.status = ChartSeriesStatus.READY
        locked.checkpoint_state = checkpoint
        locked.error_code = ""
        locked.save(
            update_fields=[
                "status",
                "checkpoint_state",
                "error_code",
            ]
        )
    _RUNTIMES.pop(series.pk, None)


@dataclass(slots=True)
class _BuildWriter:
    series: ChartSeries
    store: ClickHouseChartStore
    buffer: deque[DetailedChartBar] = field(
        default_factory=lambda: deque(maxlen=CHART_SNAPSHOT_BARS)
    )

    def add_many(self, bars) -> None:
        self.buffer.extend(_stored_bar(self.series, bar) for bar in bars)

    def finish(self) -> None:
        self.store.append(tuple(self.buffer))


@dataclass(slots=True)
class SeriesRuntime:
    engine: TickEngine | TrevEngine
    cursor: ProjectorCursor | None


def advance_ready_series(
    *,
    execution_store: ClickHouseProjectorExecutionStore,
    bar_store: ClickHouseChartStore,
) -> int:
    """Read each market tail once and fan it into every ready chart engine."""

    rows = list(
        ChartSeries.objects.select_related("market")
        .filter(
            status=ChartSeriesStatus.READY,
            last_requested_at__gte=series_lease_cutoff(
                datetime.now(tz=dt_timezone.utc)
            ),
        )
        .order_by("market_id", "pk")
    )
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
    touched: set[int] = set()
    raw_count = 0
    failed: dict[int, Exception] = {}
    for execution in stream:
        raw_count += 1
        for series, runtime in entries:
            if series.pk in failed:
                continue
            if runtime.cursor is not None and execution.cursor <= runtime.cursor:
                continue
            try:
                if (
                    runtime.cursor is not None
                    and execution.source_epoch != runtime.cursor[0]
                ):
                    for bar in runtime.engine.truncate_segment().bars:
                        emitted[series.pk].append(_stored_bar(series, bar))
                for bar in _push_execution(runtime.engine, execution).bars:
                    emitted[series.pk].append(_stored_bar(series, bar))
                runtime.cursor = execution.cursor
                touched.add(series.pk)
            except Exception as exc:
                failed[series.pk] = exc

    all_bars = [bar for values in emitted.values() for bar in values]
    if all_bars:
        bar_store.append(all_bars)

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
        checkpoint_state=None,
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


def lookback_anchor(value: datetime) -> datetime:
    return (value - timedelta(days=CHART_DEFAULT_LOOKBACK_DAYS)).replace(
        minute=0,
        second=0,
        microsecond=0,
    )


def _datetime_us(value: datetime) -> int:
    return int(value.timestamp() * 1_000_000)


def _datetime_from_us(value: int) -> datetime:
    return datetime.fromtimestamp(value / 1_000_000, tz=dt_timezone.utc)


_RUNTIMES: dict[int, SeriesRuntime] = {}
