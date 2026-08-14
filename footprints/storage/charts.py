from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import NamedTuple

from footprints.constants import CHART_SNAPSHOT_BARS, CHART_SNAPSHOT_MAX_LEVELS
from footprints.storage.clickhouse import (
    ClickHouseAccess,
    deduplicated_immediate_insert_settings,
    qualified_table,
)
from footprints.storage.clickhouse_schema import CHART_BAR_COLUMNS


ProjectorCursor = tuple[int, int, int, int, str]


class ProjectorDataError(ValueError):
    """A raw execution cannot be represented in exact projector-native units."""


class ChartDataError(ValueError):
    """A derived chart row is malformed or inconsistent with its schema."""


@dataclass(frozen=True, slots=True)
class ProjectorSpecDivisors:
    price_atoms_per_tick: int
    quantity_atoms_per_unit: int

    def __post_init__(self) -> None:
        _positive_int(self.price_atoms_per_tick, "price_atoms_per_tick")
        _positive_int(self.quantity_atoms_per_unit, "quantity_atoms_per_unit")


class ProjectorExecution(NamedTuple):
    source_epoch: int
    source_sequence: int
    batch_index: int
    timestamp_us: int
    trade_id: str
    price_tick: int
    quantity_units: int
    is_taker_buy: bool
    instrument_spec_id: int

    @property
    def cursor(self) -> ProjectorCursor:
        return (
            self.source_epoch,
            self.source_sequence,
            self.batch_index,
            self.timestamp_us,
            self.trade_id,
        )


class ClickHouseProjectorExecutionStore:
    """Stream raw executions in deterministic order and exact native units."""

    def __init__(
        self,
        access: ClickHouseAccess,
        *,
        database: str,
        spec_divisors: Mapping[int, ProjectorSpecDivisors],
        load_spec_divisor: Callable[[int], ProjectorSpecDivisors | None] | None = None,
    ):
        if not spec_divisors:
            raise ValueError("at least one instrument specification divisor is required")
        self.access = access
        self.qualified_table = qualified_table(database, "raw_executions")
        self.spec_divisors = dict(spec_divisors)
        self.load_spec_divisor = load_spec_divisor

    def iter_executions(
        self,
        *,
        market_id: int,
        start_timestamp_us: int,
        after_cursor: ProjectorCursor | None = None,
        limit: int | None = None,
    ) -> Iterator[ProjectorExecution]:
        _positive_int(market_id, "market_id")
        _positive_int(start_timestamp_us, "start_timestamp_us")
        if after_cursor is not None:
            after_cursor = _projector_cursor(after_cursor)
        if limit is not None:
            _positive_int(limit, "limit")

        conditions = [
            "market_id = {market_id:UInt64}",
            "timestamp_us >= {start_timestamp_us:Int64}",
        ]
        parameters: dict[str, object] = {
            "market_id": market_id,
            "start_timestamp_us": start_timestamp_us,
        }
        if after_cursor is not None:
            conditions.append(
                "("
                "source_epoch > {after_source_epoch:UInt32} "
                "OR (source_epoch = {after_source_epoch:UInt32} "
                "AND source_sequence > {after_source_sequence:UInt64}) "
                "OR (source_epoch = {after_source_epoch:UInt32} "
                "AND source_sequence = {after_source_sequence:UInt64} "
                "AND batch_index > {after_batch_index:UInt32}) "
                "OR (source_epoch = {after_source_epoch:UInt32} "
                "AND source_sequence = {after_source_sequence:UInt64} "
                "AND batch_index = {after_batch_index:UInt32} "
                "AND timestamp_us > {after_timestamp_us:Int64}) "
                "OR (source_epoch = {after_source_epoch:UInt32} "
                "AND source_sequence = {after_source_sequence:UInt64} "
                "AND batch_index = {after_batch_index:UInt32} "
                "AND timestamp_us = {after_timestamp_us:Int64} "
                "AND trade_id > {after_trade_id:String})"
                ")"
            )
            parameters.update(
                {
                    "after_source_epoch": after_cursor[0],
                    "after_source_sequence": after_cursor[1],
                    "after_batch_index": after_cursor[2],
                    "after_timestamp_us": after_cursor[3],
                    "after_trade_id": after_cursor[4],
                }
            )
        limit_clause = ""
        if limit is not None:
            parameters["limit"] = limit
            limit_clause = "LIMIT {limit:UInt64}"

        columns = (
            "source_epoch, source_sequence, batch_index, timestamp_us, trade_id, "
            "price_atoms, quantity_atoms, is_taker_buy, instrument_spec_id"
        )
        sql = f"""
            SELECT {columns}
            FROM {self.qualified_table}
            WHERE {' AND '.join(conditions)}
            ORDER BY
                market_id, source_epoch, source_sequence, batch_index,
                timestamp_us, trade_id
            {limit_clause}
        """

        previous: ProjectorExecution | None = None
        previous_cursor: ProjectorCursor | None = None
        for row in self.access.stream_rows(
            sql,
            parameters=parameters,
            operation="stream_projector_executions",
        ):
            execution = self._execution_from_row(row)
            execution_cursor = execution.cursor
            if after_cursor is not None and execution_cursor <= after_cursor:
                raise ProjectorDataError(
                    "projector execution did not advance after its cursor"
                )
            if previous is not None:
                assert previous_cursor is not None
                if execution_cursor < previous_cursor:
                    raise ProjectorDataError(
                        "projector executions are not in global source order"
                    )
                if execution_cursor == previous_cursor:
                    if execution != previous:
                        raise ProjectorDataError(
                            "conflicting executions share a projector cursor"
                        )
                    continue
            previous = execution
            previous_cursor = execution_cursor
            yield execution

    def _execution_from_row(self, row: Sequence[object]) -> ProjectorExecution:
        if len(row) != 9:
            raise ProjectorDataError("projector execution row has an invalid shape")
        (
            raw_source_epoch,
            raw_source_sequence,
            raw_batch_index,
            raw_timestamp_us,
            raw_trade_id,
            raw_price_atoms,
            raw_quantity_atoms,
            raw_is_taker_buy,
            raw_spec_id,
        ) = row
        # clickhouse-connect maps these constrained ClickHouse integer columns
        # directly to Python ints. Avoid seven generic int conversions per
        # execution on this hottest replay path while retaining an explicit
        # boundary check for fake/corrupt storage adapters.
        if (
            type(raw_source_epoch) is not int
            or raw_source_epoch <= 0
            or type(raw_source_sequence) is not int
            or raw_source_sequence < 0
            or type(raw_batch_index) is not int
            or raw_batch_index < 0
            or type(raw_timestamp_us) is not int
            or raw_timestamp_us <= 0
            or type(raw_price_atoms) is not int
            or raw_price_atoms <= 0
            or type(raw_quantity_atoms) is not int
            or raw_quantity_atoms <= 0
            or type(raw_spec_id) is not int
            or raw_spec_id <= 0
        ):
            raise ProjectorDataError(
                "projector execution contains an invalid native integer"
            )
        if not isinstance(raw_trade_id, str) or not raw_trade_id:
            raise ProjectorDataError("projector execution identity is incomplete")
        if type(raw_is_taker_buy) is not bool:
            raise ProjectorDataError("projector execution side is not boolean")

        source_epoch = raw_source_epoch
        source_sequence = raw_source_sequence
        batch_index = raw_batch_index
        timestamp_us = raw_timestamp_us
        trade_id = raw_trade_id
        price_atoms = raw_price_atoms
        quantity_atoms = raw_quantity_atoms
        spec_id = raw_spec_id

        divisors = self.spec_divisors.get(spec_id)
        if divisors is None and self.load_spec_divisor is not None:
            divisors = self.load_spec_divisor(spec_id)
            if divisors is not None:
                if not isinstance(divisors, ProjectorSpecDivisors):
                    raise ProjectorDataError(
                        "instrument specification divisor loader returned an invalid value"
                    )
                self.spec_divisors[spec_id] = divisors
        if divisors is None:
            raise ProjectorDataError(
                f"no native-unit divisors exist for instrument specification {spec_id}"
            )
        price_tick, price_remainder = divmod(
            price_atoms, divisors.price_atoms_per_tick
        )
        quantity_units, quantity_remainder = divmod(
            quantity_atoms, divisors.quantity_atoms_per_unit
        )
        if price_remainder or quantity_remainder:
            raise ProjectorDataError(
                f"execution atoms are not exactly divisible for instrument specification {spec_id}"
            )
        return ProjectorExecution(
            source_epoch=source_epoch,
            source_sequence=source_sequence,
            batch_index=batch_index,
            timestamp_us=timestamp_us,
            trade_id=trade_id,
            price_tick=price_tick,
            quantity_units=quantity_units,
            is_taker_buy=raw_is_taker_buy,
            instrument_spec_id=spec_id,
        )


@dataclass(frozen=True, slots=True)
class ChartLevel:
    price_tick: int
    buy_units: int
    sell_units: int

    def __post_init__(self) -> None:
        _positive_int(self.price_tick, "level price_tick")
        _nonnegative_int(self.buy_units, "level buy_units")
        _nonnegative_int(self.sell_units, "level sell_units")
        if self.buy_units == 0 and self.sell_units == 0:
            raise ChartDataError("an empty chart level must be omitted")


@dataclass(frozen=True, slots=True)
class DetailedChartBar:
    series_id: int
    bar_index: int
    opened_at: datetime
    ended_at: datetime
    instrument_spec_id: int
    open_tick: int
    high_tick: int
    low_tick: int
    close_tick: int
    buy_units: int
    sell_units: int
    trade_count: int
    break_before: bool
    levels: tuple[ChartLevel, ...] = ()

    def __post_init__(self) -> None:
        _positive_int(self.series_id, "series_id")
        _nonnegative_int(self.bar_index, "bar_index")
        _aware_datetime(self.opened_at, "opened_at")
        _aware_datetime(self.ended_at, "ended_at")
        _positive_int(self.instrument_spec_id, "instrument_spec_id")
        for name in ("open_tick", "high_tick", "low_tick", "close_tick"):
            _positive_int(getattr(self, name), name)
        if not self.low_tick <= min(self.open_tick, self.close_tick) <= self.high_tick:
            raise ChartDataError("chart OHLC bounds are invalid")
        if not self.low_tick <= max(self.open_tick, self.close_tick) <= self.high_tick:
            raise ChartDataError("chart OHLC bounds are invalid")
        _nonnegative_int(self.buy_units, "buy_units")
        _nonnegative_int(self.sell_units, "sell_units")
        _nonnegative_int(self.trade_count, "trade_count")
        if not isinstance(self.break_before, bool):
            raise ChartDataError("break_before must be boolean")
        previous_tick = 0
        for level in self.levels:
            if not isinstance(level, ChartLevel):
                raise ChartDataError("chart levels must contain ChartLevel values")
            if level.price_tick <= previous_tick:
                raise ChartDataError("chart levels must be strictly price-sorted")
            previous_tick = level.price_tick


@dataclass(frozen=True, slots=True)
class ChartSnapshotBar:
    """Validated ClickHouse-native bar optimized for one HTTP snapshot."""

    series_id: int
    bar_index: int
    opened_at: datetime
    instrument_spec_id: int
    open_tick: int
    high_tick: int
    low_tick: int
    close_tick: int
    buy_units: int
    sell_units: int
    trade_count: int
    break_before: bool
    level_ticks: tuple[int, ...]
    level_buy_units: tuple[int, ...]
    level_sell_units: tuple[int, ...]


class ClickHouseChartStore:
    """Sparse historical storage for closed Tick and TREV bars."""

    def __init__(self, access: ClickHouseAccess, *, database: str):
        self.access = access
        self.qualified_table = qualified_table(database, "chart_bars")

    def append(self, bars: Sequence[DetailedChartBar]) -> int:
        if not bars:
            return 0
        rows: list[tuple[object, ...]] = []
        keys: set[tuple[int, int]] = set()
        for bar in bars:
            if not isinstance(bar, DetailedChartBar):
                raise ChartDataError("chart inserts require detailed closed bars")
            key = (bar.series_id, bar.bar_index)
            if key in keys:
                raise ChartDataError(
                    "one chart insert batch contains a duplicate bar index"
                )
            keys.add(key)
            rows.append(_chart_insert_row(bar))
        self.access.insert(
            self.qualified_table,
            rows,
            column_names=CHART_BAR_COLUMNS,
            settings=deduplicated_immediate_insert_settings(),
            operation="insert_chart_bars",
        )
        return len(rows)

    def read_snapshot(
        self,
        *,
        series_id: int,
        limit: int = CHART_SNAPSHOT_BARS,
        max_levels: int = CHART_SNAPSHOT_MAX_LEVELS,
        before_bar_index: int | None = None,
    ) -> list[ChartSnapshotBar]:
        """Read validated native arrays without allocating one object per level."""

        _positive_int(series_id, "series_id")
        _positive_int(limit, "limit")
        _positive_int(max_levels, "max_levels")
        conditions = ["series_id = {series_id:UInt64}"]
        parameters = {
            "series_id": series_id,
            "limit": limit,
            "max_levels": max_levels,
        }
        if before_bar_index is not None:
            _nonnegative_int(before_bar_index, "before_bar_index")
            conditions.append("bar_index < {before_bar_index:UInt64}")
            parameters["before_bar_index"] = before_bar_index
        result = self.access.query(
            f"""
            SELECT {', '.join(CHART_BAR_COLUMNS)}
            FROM
            (
                SELECT
                    {', '.join(CHART_BAR_COLUMNS)},
                    sum(length(level_ticks)) OVER
                    (
                        ORDER BY bar_index DESC
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    ) AS cumulative_levels
                FROM {self.qualified_table} FINAL
                WHERE {' AND '.join(conditions)}
                ORDER BY bar_index DESC
                LIMIT {{limit:UInt64}}
            )
            WHERE cumulative_levels <= {{max_levels:UInt64}}
            ORDER BY bar_index DESC
            """,
            parameters=parameters,
            operation="read_chart_snapshot_bars",
        )
        return [
            _snapshot_bar_from_row(row)
            for row in getattr(result, "result_rows", ())
        ]


def _projector_cursor(value: ProjectorCursor) -> ProjectorCursor:
    if not isinstance(value, tuple) or len(value) != 5:
        raise ValueError("after_cursor must contain five values")
    source_epoch = _database_int(value[0], "after source_epoch", minimum=1)
    source_sequence = _database_int(value[1], "after source_sequence", minimum=0)
    batch_index = _database_int(value[2], "after batch_index", minimum=0)
    timestamp_us = _database_int(value[3], "after timestamp_us", minimum=1)
    trade_id = value[4]
    if not isinstance(trade_id, str) or not trade_id:
        raise ValueError("after trade_id must be a nonempty string")
    return source_epoch, source_sequence, batch_index, timestamp_us, trade_id


def _chart_insert_row(bar: DetailedChartBar) -> tuple[object, ...]:
    return (
        bar.series_id,
        bar.bar_index,
        bar.opened_at.astimezone(timezone.utc),
        bar.ended_at.astimezone(timezone.utc),
        bar.instrument_spec_id,
        bar.open_tick,
        bar.high_tick,
        bar.low_tick,
        bar.close_tick,
        bar.buy_units,
        bar.sell_units,
        bar.trade_count,
        bar.break_before,
        tuple(level.price_tick for level in bar.levels),
        tuple(level.buy_units for level in bar.levels),
        tuple(level.sell_units for level in bar.levels),
    )


def _snapshot_bar_from_row(row: Sequence[object]) -> ChartSnapshotBar:
    if len(row) != len(CHART_BAR_COLUMNS):
        raise ChartDataError("snapshot chart row has an invalid shape")
    raw_ticks, raw_buys, raw_sells = row[-3:]
    if not all(
        isinstance(values, (list, tuple)) for values in (raw_ticks, raw_buys, raw_sells)
    ):
        raise ChartDataError("chart level arrays are not sequences")
    if not len(raw_ticks) == len(raw_buys) == len(raw_sells):
        raise ChartDataError("chart level arrays have different lengths")

    previous_tick = 0
    for tick, buy, sell in zip(raw_ticks, raw_buys, raw_sells, strict=True):
        if type(tick) is not int or tick <= previous_tick:
            raise ChartDataError("chart levels must be strictly positive and price-sorted")
        if (
            type(buy) is not int
            or buy < 0
            or type(sell) is not int
            or sell < 0
            or (buy == 0 and sell == 0)
        ):
            raise ChartDataError("chart level volume is invalid")
        previous_tick = tick

    series_id = _database_int(row[0], "series_id", minimum=1)
    bar_index = _database_int(row[1], "bar_index", minimum=0)
    opened_at = _required_utc_datetime(row[2], "opened_at")
    _required_utc_datetime(row[3], "ended_at")
    instrument_spec_id = _database_int(
        row[4], "instrument_spec_id", minimum=1
    )
    open_tick = _database_int(row[5], "open_tick", minimum=1)
    high_tick = _database_int(row[6], "high_tick", minimum=1)
    low_tick = _database_int(row[7], "low_tick", minimum=1)
    close_tick = _database_int(row[8], "close_tick", minimum=1)
    if not low_tick <= min(open_tick, close_tick) <= high_tick or not (
        low_tick <= max(open_tick, close_tick) <= high_tick
    ):
        raise ChartDataError("chart OHLC bounds are invalid")

    return ChartSnapshotBar(
        series_id=series_id,
        bar_index=bar_index,
        opened_at=opened_at,
        instrument_spec_id=instrument_spec_id,
        open_tick=open_tick,
        high_tick=high_tick,
        low_tick=low_tick,
        close_tick=close_tick,
        buy_units=_database_int(row[9], "buy_units", minimum=0),
        sell_units=_database_int(row[10], "sell_units", minimum=0),
        trade_count=_database_int(row[11], "trade_count", minimum=0),
        break_before=_database_bool(row[12], "break_before"),
        level_ticks=tuple(raw_ticks),
        level_buy_units=tuple(raw_buys),
        level_sell_units=tuple(raw_sells),
    )


def _positive_int(value: object, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{label} must be a positive integer")
    return value


def _nonnegative_int(value: object, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{label} must be a nonnegative integer")
    return value


def _database_int(value: object, label: str, *, minimum: int) -> int:
    if isinstance(value, bool):
        raise ChartDataError(f"{label} is not an integer")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ChartDataError(f"{label} is not an integer") from exc
    if parsed < minimum:
        raise ChartDataError(f"{label} is below its minimum")
    return parsed


def _database_bool(value: object, label: str) -> bool:
    if not isinstance(value, bool):
        raise ChartDataError(f"{label} is not boolean")
    return value


def _aware_datetime(value: object, label: str) -> datetime:
    if (
        not isinstance(value, datetime)
        or value.tzinfo is None
        or value.utcoffset() is None
    ):
        raise ValueError(f"{label} must be a timezone-aware datetime")
    return value


def _required_utc_datetime(value: object, label: str) -> datetime:
    if not isinstance(value, datetime):
        raise ChartDataError(f"{label} is not a datetime")
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
