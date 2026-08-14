"""Deterministic Trend/Reversal (TREV) bar construction.

The engine consumes one already-normalized execution stream.  Prices and
quantities are integers in the instrument's exact native units; callers own
Decimal conversion and must supply a strict total order.  The module has no
Django or persistence dependency, so the same code can drive live projection,
archive replay, and checkpoint verification.

TREV v1 rules
-------------

* The first execution opens an uncommitted segment.  The first later execution
  at a different price establishes direction immediately; same-price trades
  leave it unset.  No bar closes until a configured threshold is reached.
* Thereafter a bar closes at ``continuation_ticks`` from its open in the
  current direction, or at ``reversal_ticks`` from its open in the opposite
  direction.  Equality closes the bar.
* A boundary execution belongs to the bar ending at that boundary.  If one
  execution overshoots several boundaries, intermediate bars are synthetic
  and have zero volume; the execution belongs only to the final boundary bar
  (on equality) or to the remaining active bar (on overshoot).
* Gaps and specification changes terminate, rather than bridge, an active
  segment. Out-of-order input is rejected without mutating state.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Mapping, Sequence

from footprints.constants import TREV_MAX_EXPANSION_PER_EXECUTION


MAX_BARS_PER_TRADE = TREV_MAX_EXPANSION_PER_EXECUTION


class TrevError(ValueError):
    """Base error for invalid TREV input or state."""


class TrevExpansionLimitError(TrevError):
    """One execution would create an unreasonable number of bars."""


class TrevSnapshotError(TrevError):
    """A checkpoint is malformed, incompatible, or corrupt."""


class TrevDirection(str, Enum):
    UNSET = "unset"
    UP = "up"
    DOWN = "down"


@dataclass(frozen=True, slots=True)
class TrevOrder:
    """Replay ordinal plus the execution's independent event timestamp.

    Canonical source order may differ from event-time order.  ``rank`` is the
    adapter-assigned, anchor-relative source ordinal. ``timestamp_us`` remains
    attached for exact bar timestamps and checkpoint identity.
    """

    timestamp_us: int
    rank: int

    def __post_init__(self) -> None:
        if (
            isinstance(self.timestamp_us, bool)
            or not isinstance(self.timestamp_us, int)
            or self.timestamp_us <= 0
        ):
            raise TrevError("timestamp_us must be a positive integer")
        if (
            isinstance(self.rank, bool)
            or not isinstance(self.rank, int)
            or self.rank < 0
        ):
            raise TrevError("rank must be a non-negative integer")

    def to_list(self) -> list[int]:
        return [self.timestamp_us, self.rank]

    @classmethod
    def from_value(cls, value: object, *, field_name: str = "order") -> "TrevOrder":
        if (
            not isinstance(value, Sequence)
            or isinstance(value, (str, bytes))
            or len(value) != 2
        ):
            raise TrevSnapshotError(f"{field_name} must contain [timestamp_us, rank]")
        timestamp_us = _strict_int(value[0], f"{field_name}[0]")
        rank = _strict_int(value[1], f"{field_name}[1]")
        try:
            return cls(timestamp_us=timestamp_us, rank=rank)
        except TrevError as exc:
            raise TrevSnapshotError(str(exc)) from exc


@dataclass(frozen=True, slots=True)
class TrevConfig:
    continuation_ticks: int
    reversal_ticks: int
    max_bars_per_trade: int = MAX_BARS_PER_TRADE

    def __post_init__(self) -> None:
        for name, value in (
            ("continuation_ticks", self.continuation_ticks),
            ("reversal_ticks", self.reversal_ticks),
            ("max_bars_per_trade", self.max_bars_per_trade),
        ):
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise TrevError(f"{name} must be a positive integer")
        if self.max_bars_per_trade > MAX_BARS_PER_TRADE:
            raise TrevError(f"max_bars_per_trade cannot exceed {MAX_BARS_PER_TRADE}")

@dataclass(frozen=True, slots=True)
class TrevTrade:
    """Minimal exact execution input expected by :class:`TrevEngine`."""

    order: TrevOrder
    price_tick: int
    quantity_units: int
    is_taker_buy: bool
    spec_id: int

    def __post_init__(self) -> None:
        if (
            isinstance(self.price_tick, bool)
            or not isinstance(self.price_tick, int)
            or self.price_tick <= 0
        ):
            raise TrevError("price_tick must be a positive integer")
        if (
            isinstance(self.quantity_units, bool)
            or not isinstance(self.quantity_units, int)
            or self.quantity_units <= 0
        ):
            raise TrevError("quantity_units must be a positive integer")
        if not isinstance(self.is_taker_buy, bool):
            raise TrevError("is_taker_buy must be boolean")
        if (
            isinstance(self.spec_id, bool)
            or not isinstance(self.spec_id, int)
            or self.spec_id <= 0
        ):
            raise TrevError("spec_id must be a positive integer")


@dataclass(frozen=True, slots=True)
class TrevLevel:
    price_tick: int
    buy_units: int
    sell_units: int

    def to_list(self) -> list[int]:
        return [self.price_tick, self.buy_units, self.sell_units]


@dataclass(frozen=True, slots=True)
class TrevBar:
    bar_index: int
    spec_id: int
    break_before: bool
    open_tick: int
    high_tick: int
    low_tick: int
    close_tick: int
    buy_units: int
    sell_units: int
    trade_count: int
    levels: tuple[TrevLevel, ...]
    opened_timestamp_us: int
    ended_timestamp_us: int

    @property
    def quantity_units(self) -> int:
        return self.buy_units + self.sell_units

    @property
    def is_synthetic(self) -> bool:
        return self.trade_count == 0


@dataclass(frozen=True, slots=True)
class TrevUpdate:
    """Bars closed by one deterministic engine transition."""

    bars: tuple[TrevBar, ...]


_EMPTY_TREV_UPDATE = TrevUpdate(bars=())


@dataclass(slots=True)
class _ActiveBar:
    open_tick: int
    last_tick: int
    high_tick: int
    low_tick: int
    break_before: bool = False
    levels: dict[int, list[int]] = field(default_factory=dict)
    buy_units: int = 0
    sell_units: int = 0
    trade_count: int = 0
    first_order: TrevOrder | None = None
    last_order: TrevOrder | None = None

    @classmethod
    def empty(cls, open_tick: int, *, break_before: bool = False) -> "_ActiveBar":
        return cls(
            open_tick=open_tick,
            last_tick=open_tick,
            high_tick=open_tick,
            low_tick=open_tick,
            break_before=break_before,
        )

    def add(self, trade: TrevTrade) -> None:
        self.add_raw(
            order=trade.order,
            price_tick=trade.price_tick,
            quantity_units=trade.quantity_units,
            is_taker_buy=trade.is_taker_buy,
        )

    def add_raw(
        self,
        *,
        order: TrevOrder,
        price_tick: int,
        quantity_units: int,
        is_taker_buy: bool,
    ) -> None:
        self.last_tick = price_tick
        self.high_tick = max(self.high_tick, price_tick)
        self.low_tick = min(self.low_tick, price_tick)
        values = self.levels.setdefault(price_tick, [0, 0])
        if is_taker_buy:
            values[0] += quantity_units
            self.buy_units += quantity_units
        else:
            values[1] += quantity_units
            self.sell_units += quantity_units
        self.trade_count += 1
        if self.first_order is None:
            self.first_order = order
        self.last_order = order

    def levels_tuple(self) -> tuple[TrevLevel, ...]:
        return tuple(
            TrevLevel(price_tick=price_tick, buy_units=values[0], sell_units=values[1])
            for price_tick, values in sorted(self.levels.items())
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "openTick": self.open_tick,
            "lastTick": self.last_tick,
            "highTick": self.high_tick,
            "lowTick": self.low_tick,
            "breakBefore": self.break_before,
            "buyUnits": self.buy_units,
            "sellUnits": self.sell_units,
            "tradeCount": self.trade_count,
            "levels": [level.to_list() for level in self.levels_tuple()],
            "firstOrder": _order_value(self.first_order),
            "lastOrder": _order_value(self.last_order),
        }


@dataclass(slots=True)
class TrevState:
    """Mutable replay state.  Persist it only through ``TrevEngine.snapshot``."""

    direction: TrevDirection = TrevDirection.UNSET
    next_bar_index: int = 0
    break_before_next: bool = False
    spec_id: int | None = None
    last_order: TrevOrder | None = None
    active: _ActiveBar | None = None


class TrevEngine:
    """Single-stream deterministic TREV v1 state machine.

    Instances are intentionally not thread-safe.  A caller should serialize
    one market/configuration stream, and rebuild from the last clean checkpoint
    if the source order changes.
    """

    def __init__(self, config: TrevConfig, state: TrevState | None = None):
        self.config = config
        self.state = state or TrevState()
        self._validate_state()

    def push(self, trade: TrevTrade) -> TrevUpdate:
        """Consume one execution and return newly closed bars plus active state."""

        if not isinstance(trade, TrevTrade):
            raise TrevError("trade must be a TrevTrade")
        return self._push_trusted(
            order=trade.order,
            price_tick=trade.price_tick,
            quantity_units=trade.quantity_units,
            is_taker_buy=trade.is_taker_buy,
            spec_id=trade.spec_id,
        )

    def _push_trusted(
        self,
        *,
        order: TrevOrder,
        price_tick: int,
        quantity_units: int,
        is_taker_buy: bool,
        spec_id: int,
    ) -> TrevUpdate:
        if (
            self.state.last_order is not None
            and order.rank <= self.state.last_order.rank
        ):
            raise TrevError("execution order must strictly increase")

        closed: list[TrevBar] = []
        if self.state.spec_id is not None and spec_id != self.state.spec_id:
            closed.extend(self._truncate())

        if self.state.active is None:
            active = _ActiveBar.empty(
                price_tick,
                break_before=self.state.break_before_next,
            )
            self.state.break_before_next = False
            active.add_raw(
                order=order,
                price_tick=price_tick,
                quantity_units=quantity_units,
                is_taker_buy=is_taker_buy,
            )
            self.state.active = active
            self.state.spec_id = spec_id
            self.state.last_order = order
            return TrevUpdate(bars=tuple(closed)) if closed else _EMPTY_TREV_UPDATE

        established_direction: TrevDirection | None = None
        if self.state.direction == TrevDirection.UNSET:
            if price_tick > self.state.active.open_tick:
                established_direction = TrevDirection.UP
            elif price_tick < self.state.active.open_tick:
                established_direction = TrevDirection.DOWN

        expansion_count = self._expansion_count(
            price_tick,
            direction=established_direction or self.state.direction,
        )
        if expansion_count > self.config.max_bars_per_trade:
            raise TrevExpansionLimitError(
                f"execution would create {expansion_count} bars; "
                f"limit is {self.config.max_bars_per_trade}"
            )
        if established_direction is not None:
            self.state.direction = established_direction

        thresholds = self._thresholds(price_tick, expansion_count)
        for index, (close_tick, new_direction) in enumerate(thresholds):
            is_last = index == len(thresholds) - 1
            if is_last and price_tick == close_tick:
                self.state.active.add_raw(
                    order=order,
                    price_tick=price_tick,
                    quantity_units=quantity_units,
                    is_taker_buy=is_taker_buy,
                )
            closed.append(
                self._close_active(
                    close_tick=close_tick,
                    trigger_order=order,
                )
            )
            self.state.direction = new_direction
            self.state.active = _ActiveBar.empty(close_tick)

        if not thresholds or price_tick != thresholds[-1][0]:
            self.state.active.add_raw(
                order=order,
                price_tick=price_tick,
                quantity_units=quantity_units,
                is_taker_buy=is_taker_buy,
            )

        self.state.spec_id = spec_id
        self.state.last_order = order
        return TrevUpdate(bars=tuple(closed)) if closed else _EMPTY_TREV_UPDATE

    def push_raw(
        self,
        *,
        timestamp_us: int,
        rank: int,
        price_tick: int,
        quantity_units: int,
        is_taker_buy: bool,
        spec_id: int,
    ) -> TrevUpdate:
        """Consume one trusted storage row without repeating scalar validation.

        The ClickHouse projector reader has already validated its constrained
        integer schema and exact native-unit divisibility. Public and test
        callers should continue to use :meth:`push`.
        """

        order = object.__new__(TrevOrder)
        object.__setattr__(order, "timestamp_us", timestamp_us)
        object.__setattr__(order, "rank", rank)
        return self._push_trusted(
            order=order,
            price_tick=price_tick,
            quantity_units=quantity_units,
            is_taker_buy=is_taker_buy,
            spec_id=spec_id,
        )

    def truncate_segment(self) -> TrevUpdate:
        """End the current segment at a known data gap or specification change."""

        bars = self._truncate()
        return TrevUpdate(bars=bars) if bars else _EMPTY_TREV_UPDATE

    def active_snapshot(self) -> TrevBar | None:
        active = self.state.active
        spec_id = self.state.spec_id
        if active is None or spec_id is None or active.trade_count == 0:
            return None
        return self._bar_from_active(
            active=active,
            close_tick=active.last_tick,
            trigger_order=None,
            bar_index=self.state.next_bar_index,
        )

    def snapshot(self) -> dict[str, Any]:
        """Return a JSON-safe replay checkpoint."""

        return {
            "direction": self.state.direction.value,
            "nextBarIndex": self.state.next_bar_index,
            "breakBeforeNext": self.state.break_before_next,
            "specId": self.state.spec_id,
            "lastOrder": _order_value(self.state.last_order),
            "active": self.state.active.to_dict() if self.state.active else None,
        }

    @classmethod
    def restore(
        cls,
        snapshot: Mapping[str, Any],
        *,
        config: TrevConfig,
    ) -> "TrevEngine":
        """Verify and restore a checkpoint without accepting partial state."""

        if not isinstance(snapshot, Mapping):
            raise TrevSnapshotError("snapshot must be an object")
        if not isinstance(config, TrevConfig):
            raise TrevSnapshotError("config must be a TrevConfig")
        state_data = snapshot
        try:
            direction = TrevDirection(state_data.get("direction"))
        except ValueError as exc:
            raise TrevSnapshotError("invalid state direction") from exc
        spec_id_value = state_data.get("specId")
        spec_id = (
            None if spec_id_value is None else _strict_int(spec_id_value, "specId")
        )
        if spec_id is not None and spec_id <= 0:
            raise TrevSnapshotError("specId must be positive")
        break_before_next = state_data.get("breakBeforeNext")
        if not isinstance(break_before_next, bool):
            raise TrevSnapshotError("breakBeforeNext must be boolean")
        state = TrevState(
            direction=direction,
            next_bar_index=_non_negative_int(
                state_data.get("nextBarIndex"), "nextBarIndex"
            ),
            break_before_next=break_before_next,
            spec_id=spec_id,
            last_order=_optional_order(state_data.get("lastOrder"), "lastOrder"),
            active=_restore_active(state_data.get("active")),
        )
        try:
            return cls(config=config, state=state)
        except TrevError as exc:
            raise TrevSnapshotError(str(exc)) from exc

    def _truncate(self) -> tuple[TrevBar, ...]:
        bars: tuple[TrevBar, ...] = ()
        if self.state.active is not None and self.state.active.trade_count:
            bar = self._bar_from_active(
                active=self.state.active,
                close_tick=self.state.active.last_tick,
                trigger_order=None,
                bar_index=self.state.next_bar_index,
            )
            self.state.next_bar_index += 1
            bars = (bar,)

        self.state.break_before_next = True
        self.state.direction = TrevDirection.UNSET
        self.state.spec_id = None
        self.state.active = None
        return bars

    def _expansion_count(self, price_tick: int, *, direction: TrevDirection) -> int:
        active = self.state.active
        assert active is not None
        distance = price_tick - active.open_tick
        continuation = self.config.continuation_ticks
        reversal = self.config.reversal_ticks
        if direction == TrevDirection.UNSET:
            return 0
        if direction == TrevDirection.UP:
            if distance >= continuation:
                return distance // continuation
            if distance <= -reversal:
                return 1 + ((-distance - reversal) // continuation)
            return 0
        if distance <= -continuation:
            return (-distance) // continuation
        if distance >= reversal:
            return 1 + ((distance - reversal) // continuation)
        return 0

    def _thresholds(
        self,
        price_tick: int,
        count: int,
    ) -> list[tuple[int, TrevDirection]]:
        if not count:
            return []
        active = self.state.active
        assert active is not None
        open_tick = active.open_tick
        continuation = self.config.continuation_ticks
        reversal = self.config.reversal_ticks
        thresholds: list[tuple[int, TrevDirection]] = []

        if self.state.direction == TrevDirection.UP:
            if price_tick >= open_tick + continuation:
                direction = TrevDirection.UP
                first_close = open_tick + continuation
                step = continuation
            else:
                direction = TrevDirection.DOWN
                first_close = open_tick - reversal
                step = -continuation
        else:
            if price_tick <= open_tick - continuation:
                direction = TrevDirection.DOWN
                first_close = open_tick - continuation
                step = -continuation
            else:
                direction = TrevDirection.UP
                first_close = open_tick + reversal
                step = continuation

        thresholds.append((first_close, direction))
        for _ in range(1, count):
            thresholds.append((thresholds[-1][0] + step, direction))
        return thresholds

    def _close_active(
        self,
        *,
        close_tick: int,
        trigger_order: TrevOrder,
    ) -> TrevBar:
        active = self.state.active
        assert active is not None
        bar = self._bar_from_active(
            active=active,
            close_tick=close_tick,
            trigger_order=trigger_order,
            bar_index=self.state.next_bar_index,
        )
        self.state.next_bar_index += 1
        return bar

    def _bar_from_active(
        self,
        *,
        active: _ActiveBar,
        close_tick: int,
        trigger_order: TrevOrder | None,
        bar_index: int,
    ) -> TrevBar:
        spec_id = self.state.spec_id
        if spec_id is None:
            raise TrevError("active bar requires an instrument specification")
        opened_order = active.first_order or trigger_order or active.last_order
        ended_order = trigger_order or active.last_order or active.first_order
        if opened_order is None or ended_order is None:
            raise TrevError("TREV bar has no event-time anchor")
        return TrevBar(
            bar_index=bar_index,
            spec_id=spec_id,
            break_before=active.break_before,
            open_tick=active.open_tick,
            high_tick=max(active.high_tick, active.open_tick, close_tick),
            low_tick=min(active.low_tick, active.open_tick, close_tick),
            close_tick=close_tick,
            buy_units=active.buy_units,
            sell_units=active.sell_units,
            trade_count=active.trade_count,
            levels=active.levels_tuple(),
            opened_timestamp_us=opened_order.timestamp_us,
            ended_timestamp_us=ended_order.timestamp_us,
        )

    def _validate_state(self) -> None:
        state = self.state
        if (
            isinstance(state.next_bar_index, bool)
            or not isinstance(state.next_bar_index, int)
            or state.next_bar_index < 0
        ):
            raise TrevError("bar index cannot be negative")
        if not isinstance(state.break_before_next, bool):
            raise TrevError("break-before state must be boolean")
        if not isinstance(state.direction, TrevDirection):
            raise TrevError("state direction is invalid")
        if state.spec_id is not None and (
            isinstance(state.spec_id, bool)
            or not isinstance(state.spec_id, int)
            or state.spec_id <= 0
        ):
            raise TrevError("state spec_id must be a positive integer")
        if (state.active is None) != (state.spec_id is None):
            raise TrevError(
                "active state and spec_id must either both exist or both be absent"
            )
        if state.direction != TrevDirection.UNSET and state.active is None:
            raise TrevError("a committed direction requires an active bar")
        if state.active is not None:
            active = state.active
            if not isinstance(active.break_before, bool):
                raise TrevError("active break-before state must be boolean")
            if not (active.low_tick <= active.open_tick <= active.high_tick):
                raise TrevError("active OHLC bounds are invalid")
            if not (active.low_tick <= active.last_tick <= active.high_tick):
                raise TrevError("active last price is outside OHLC bounds")
            if (active.first_order is None) != (active.last_order is None):
                raise TrevError("active execution order bounds are incomplete")
            if (
                active.first_order is not None
                and active.first_order.rank > active.last_order.rank
            ):
                raise TrevError("active execution order bounds are reversed")
            if active.last_order is not None and active.last_order != state.last_order:
                raise TrevError(
                    "active execution order does not match state last_order"
                )
            level_buy = sum(values[0] for values in active.levels.values())
            level_sell = sum(values[1] for values in active.levels.values())
            if (level_buy, level_sell) != (active.buy_units, active.sell_units):
                raise TrevError("active level totals do not match state totals")
            if active.trade_count < 0:
                raise TrevError("active trade count cannot be negative")
            if (active.trade_count == 0) != (not active.levels):
                raise TrevError("active trade count does not match its levels")
        if state.last_order is None and state.active is not None:
            raise TrevError("active state requires last_order")


def _order_value(order: TrevOrder | None) -> list[int] | None:
    return order.to_list() if order else None


def _mapping(value: object, field_name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TrevSnapshotError(f"{field_name} must be an object")
    return value


def _strict_int(value: object, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TrevSnapshotError(f"{field_name} must be an integer")
    return value


def _non_negative_int(value: object, field_name: str) -> int:
    integer = _strict_int(value, field_name)
    if integer < 0:
        raise TrevSnapshotError(f"{field_name} cannot be negative")
    return integer


def _optional_order(value: object, field_name: str) -> TrevOrder | None:
    return None if value is None else TrevOrder.from_value(value, field_name=field_name)


def _restore_active(value: object) -> _ActiveBar | None:
    if value is None:
        return None
    data = _mapping(value, "active")
    levels_value = data.get("levels")
    if not isinstance(levels_value, Sequence) or isinstance(levels_value, (str, bytes)):
        raise TrevSnapshotError("active levels must be an array")
    levels: dict[int, list[int]] = {}
    previous_tick: int | None = None
    for index, level_value in enumerate(levels_value):
        if (
            not isinstance(level_value, Sequence)
            or isinstance(level_value, (str, bytes))
            or len(level_value) != 3
        ):
            raise TrevSnapshotError(f"active level {index} must contain three integers")
        values = [_strict_int(item, f"active.levels[{index}]") for item in level_value]
        price_tick, buy_units, sell_units = values
        if previous_tick is not None and price_tick <= previous_tick:
            raise TrevSnapshotError("active levels must be strictly price-sorted")
        if min(buy_units, sell_units) < 0:
            raise TrevSnapshotError("active level values cannot be negative")
        if buy_units == 0 and sell_units == 0:
            raise TrevSnapshotError("active levels cannot contain empty rows")
        levels[price_tick] = [buy_units, sell_units]
        previous_tick = price_tick

    active = _ActiveBar(
        open_tick=_strict_int(data.get("openTick"), "active.openTick"),
        last_tick=_strict_int(data.get("lastTick"), "active.lastTick"),
        high_tick=_strict_int(data.get("highTick"), "active.highTick"),
        low_tick=_strict_int(data.get("lowTick"), "active.lowTick"),
        break_before=data.get("breakBefore"),
        levels=levels,
        buy_units=_non_negative_int(data.get("buyUnits"), "active.buyUnits"),
        sell_units=_non_negative_int(data.get("sellUnits"), "active.sellUnits"),
        trade_count=_non_negative_int(data.get("tradeCount"), "active.tradeCount"),
        first_order=_optional_order(data.get("firstOrder"), "active.firstOrder"),
        last_order=_optional_order(data.get("lastOrder"), "active.lastOrder"),
    )
    if not isinstance(active.break_before, bool):
        raise TrevSnapshotError("active.breakBefore must be boolean")
    return active
