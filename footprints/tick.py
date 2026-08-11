"""Deterministic trade-count bar construction.

The projector consumes trades already converted to exact integer price and
quantity units.  One input execution always counts as one trade, regardless of
its quantity, and input ``rank`` values must strictly increase.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence

from footprints.constants import TICK_MAX_TRADES_PER_BAR


MAX_TRADES_PER_BAR = TICK_MAX_TRADES_PER_BAR


class TickError(ValueError):
    """Base error for invalid tick-projector input or state."""


class TickSnapshotError(TickError):
    """A checkpoint is malformed or incompatible."""


@dataclass(frozen=True, slots=True)
class TickOrder:
    timestamp_us: int
    rank: int

    def __post_init__(self) -> None:
        if _is_bool_or_not_int(self.timestamp_us) or self.timestamp_us <= 0:
            raise TickError("timestamp_us must be a positive integer")
        if _is_bool_or_not_int(self.rank) or self.rank < 0:
            raise TickError("rank must be a non-negative integer")

    def to_list(self) -> list[int]:
        return [self.timestamp_us, self.rank]

    @classmethod
    def from_value(cls, value: object, *, field_name: str = "order") -> "TickOrder":
        if not _is_array(value) or len(value) != 2:
            raise TickSnapshotError(f"{field_name} must contain [timestamp_us, rank]")
        try:
            return cls(
                timestamp_us=_strict_int(value[0], f"{field_name}[0]"),
                rank=_strict_int(value[1], f"{field_name}[1]"),
            )
        except TickError as exc:
            raise TickSnapshotError(str(exc)) from exc


@dataclass(frozen=True, slots=True)
class TickConfig:
    trades_per_bar: int

    def __post_init__(self) -> None:
        if _is_bool_or_not_int(self.trades_per_bar) or self.trades_per_bar <= 0:
            raise TickError("trades_per_bar must be a positive integer")
        if self.trades_per_bar > MAX_TRADES_PER_BAR:
            raise TickError(f"trades_per_bar cannot exceed {MAX_TRADES_PER_BAR}")

@dataclass(frozen=True, slots=True)
class TickTrade:
    order: TickOrder
    price_tick: int
    quantity_units: int
    is_taker_buy: bool
    spec_id: int

    def __post_init__(self) -> None:
        if not isinstance(self.order, TickOrder):
            raise TickError("order must be a TickOrder")
        if _is_bool_or_not_int(self.price_tick) or self.price_tick <= 0:
            raise TickError("price_tick must be a positive integer")
        if _is_bool_or_not_int(self.quantity_units) or self.quantity_units <= 0:
            raise TickError("quantity_units must be a positive integer")
        if not isinstance(self.is_taker_buy, bool):
            raise TickError("is_taker_buy must be boolean")
        if _is_bool_or_not_int(self.spec_id) or self.spec_id <= 0:
            raise TickError("spec_id must be a positive integer")


@dataclass(frozen=True, slots=True)
class TickLevel:
    price_tick: int
    buy_units: int
    sell_units: int

    def to_list(self) -> list[int]:
        return [self.price_tick, self.buy_units, self.sell_units]


@dataclass(frozen=True, slots=True)
class TickBar:
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
    levels: tuple[TickLevel, ...]
    opened_timestamp_us: int
    ended_timestamp_us: int

    @property
    def quantity_units(self) -> int:
        return self.buy_units + self.sell_units


@dataclass(frozen=True, slots=True)
class TickUpdate:
    bars: tuple[TickBar, ...]


@dataclass(slots=True)
class _ActiveBar:
    open_tick: int
    high_tick: int
    low_tick: int
    last_tick: int
    break_before: bool
    first_order: TickOrder
    last_order: TickOrder
    levels: dict[int, list[int]] = field(default_factory=dict)
    buy_units: int = 0
    sell_units: int = 0
    trade_count: int = 0

    @classmethod
    def start(cls, trade: TickTrade, *, break_before: bool) -> "_ActiveBar":
        active = cls(
            open_tick=trade.price_tick,
            high_tick=trade.price_tick,
            low_tick=trade.price_tick,
            last_tick=trade.price_tick,
            break_before=break_before,
            first_order=trade.order,
            last_order=trade.order,
        )
        active.add(trade)
        return active

    def add(self, trade: TickTrade) -> None:
        self.high_tick = max(self.high_tick, trade.price_tick)
        self.low_tick = min(self.low_tick, trade.price_tick)
        self.last_tick = trade.price_tick
        self.last_order = trade.order
        values = self.levels.setdefault(trade.price_tick, [0, 0])
        if trade.is_taker_buy:
            values[0] += trade.quantity_units
            self.buy_units += trade.quantity_units
        else:
            values[1] += trade.quantity_units
            self.sell_units += trade.quantity_units
        self.trade_count += 1

    def level_rows(self) -> tuple[TickLevel, ...]:
        return tuple(
            TickLevel(price_tick, values[0], values[1])
            for price_tick, values in sorted(self.levels.items())
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "openTick": self.open_tick,
            "highTick": self.high_tick,
            "lowTick": self.low_tick,
            "lastTick": self.last_tick,
            "breakBefore": self.break_before,
            "firstOrder": self.first_order.to_list(),
            "lastOrder": self.last_order.to_list(),
            "buyUnits": self.buy_units,
            "sellUnits": self.sell_units,
            "tradeCount": self.trade_count,
            "levels": [level.to_list() for level in self.level_rows()],
        }


@dataclass(slots=True)
class TickState:
    next_bar_index: int = 0
    break_before_next: bool = False
    spec_id: int | None = None
    last_order: TickOrder | None = None
    active: _ActiveBar | None = None


class TickEngine:
    """Single-stream, non-thread-safe tick-chart projector."""

    def __init__(self, config: TickConfig, state: TickState | None = None):
        if not isinstance(config, TickConfig):
            raise TickError("config must be a TickConfig")
        self.config = config
        self.state = state or TickState()
        self._validate_state()

    def push(self, trade: TickTrade) -> TickUpdate:
        if not isinstance(trade, TickTrade):
            raise TickError("trade must be a TickTrade")
        if (
            self.state.last_order is not None
            and trade.order.rank <= self.state.last_order.rank
        ):
            raise TickError("execution order must strictly increase")

        closed: list[TickBar] = []
        if self.state.spec_id is not None and trade.spec_id != self.state.spec_id:
            closed.extend(self._truncate())

        if self.state.active is None:
            self.state.active = _ActiveBar.start(
                trade,
                break_before=self.state.break_before_next,
            )
            self.state.break_before_next = False
        else:
            self.state.active.add(trade)

        self.state.spec_id = trade.spec_id
        self.state.last_order = trade.order
        if self.state.active.trade_count == self.config.trades_per_bar:
            closed.append(self._close_active())
        return TickUpdate(bars=tuple(closed))

    def push_raw(
        self,
        *,
        timestamp_us: int,
        rank: int,
        price_tick: int,
        quantity_units: int,
        is_taker_buy: bool,
        spec_id: int,
    ) -> TickUpdate:
        """Consume one trusted storage row without repeating scalar validation."""

        order = object.__new__(TickOrder)
        object.__setattr__(order, "timestamp_us", timestamp_us)
        object.__setattr__(order, "rank", rank)
        trade = object.__new__(TickTrade)
        object.__setattr__(trade, "order", order)
        object.__setattr__(trade, "price_tick", price_tick)
        object.__setattr__(trade, "quantity_units", quantity_units)
        object.__setattr__(trade, "is_taker_buy", is_taker_buy)
        object.__setattr__(trade, "spec_id", spec_id)
        return self.push(trade)

    def truncate_segment(self) -> TickUpdate:
        """Close a partial bar and mark the next bar as discontinuous."""

        return TickUpdate(bars=self._truncate())

    def active_snapshot(self) -> TickBar | None:
        if self.state.active is None:
            return None
        return self._bar(self.state.active, self.state.next_bar_index)

    def snapshot(self) -> dict[str, Any]:
        """Return one JSON-safe latest-state checkpoint."""

        return {
            "nextBarIndex": self.state.next_bar_index,
            "breakBeforeNext": self.state.break_before_next,
            "specId": self.state.spec_id,
            "lastOrder": (
                self.state.last_order.to_list() if self.state.last_order else None
            ),
            "active": self.state.active.to_dict() if self.state.active else None,
        }

    @classmethod
    def restore(
        cls,
        snapshot: Mapping[str, Any],
        *,
        config: TickConfig,
    ) -> "TickEngine":
        if not isinstance(snapshot, Mapping):
            raise TickSnapshotError("snapshot must be an object")
        if not isinstance(config, TickConfig):
            raise TickSnapshotError("config must be a TickConfig")
        state_data = snapshot
        break_before_next = state_data.get("breakBeforeNext")
        if not isinstance(break_before_next, bool):
            raise TickSnapshotError("breakBeforeNext must be boolean")
        spec_value = state_data.get("specId")
        spec_id = None if spec_value is None else _strict_int(spec_value, "specId")
        if spec_id is not None and spec_id <= 0:
            raise TickSnapshotError("specId must be positive")
        state = TickState(
            next_bar_index=_non_negative_int(
                state_data.get("nextBarIndex"), "nextBarIndex"
            ),
            break_before_next=break_before_next,
            spec_id=spec_id,
            last_order=_optional_order(state_data.get("lastOrder"), "lastOrder"),
            active=_restore_active(state_data.get("active")),
        )
        try:
            return cls(config, state)
        except TickError as exc:
            raise TickSnapshotError(str(exc)) from exc

    def _truncate(self) -> tuple[TickBar, ...]:
        bars = (self._close_active(),) if self.state.active is not None else ()
        self.state.spec_id = None
        self.state.break_before_next = True
        return bars

    def _close_active(self) -> TickBar:
        active = self.state.active
        if active is None:
            raise TickError("there is no active bar to close")
        bar = self._bar(active, self.state.next_bar_index)
        self.state.next_bar_index += 1
        self.state.active = None
        return bar

    def _bar(self, active: _ActiveBar, bar_index: int) -> TickBar:
        if self.state.spec_id is None:
            raise TickError("active bar requires an instrument specification")
        return TickBar(
            bar_index=bar_index,
            spec_id=self.state.spec_id,
            break_before=active.break_before,
            open_tick=active.open_tick,
            high_tick=active.high_tick,
            low_tick=active.low_tick,
            close_tick=active.last_tick,
            buy_units=active.buy_units,
            sell_units=active.sell_units,
            trade_count=active.trade_count,
            levels=active.level_rows(),
            opened_timestamp_us=active.first_order.timestamp_us,
            ended_timestamp_us=active.last_order.timestamp_us,
        )

    def _validate_state(self) -> None:
        state = self.state
        if _is_bool_or_not_int(state.next_bar_index) or state.next_bar_index < 0:
            raise TickError("bar index cannot be negative")
        if not isinstance(state.break_before_next, bool):
            raise TickError("break-before state must be boolean")
        if state.spec_id is not None and (
            _is_bool_or_not_int(state.spec_id) or state.spec_id <= 0
        ):
            raise TickError("state spec_id must be a positive integer")
        if state.last_order is not None and not isinstance(state.last_order, TickOrder):
            raise TickError("state last_order must be a TickOrder")
        if state.active is not None:
            active = state.active
            if state.spec_id is None or state.last_order is None:
                raise TickError("active state requires a specification and last order")
            if state.break_before_next:
                raise TickError("an active bar cannot also have a pending break")
            if not isinstance(active.break_before, bool):
                raise TickError("active break-before state must be boolean")
            if not (0 < active.trade_count < self.config.trades_per_bar):
                raise TickError("active trade count is outside projector bounds")
            if active.last_order != state.last_order:
                raise TickError(
                    "active execution order does not match state last_order"
                )
            if active.first_order.rank > active.last_order.rank:
                raise TickError("active execution order bounds are reversed")
            if (
                active.last_order.rank - active.first_order.rank + 1
                < active.trade_count
            ):
                raise TickError("active execution count exceeds its order span")
            if len(active.levels) > active.trade_count or not active.levels:
                raise TickError("active levels do not match its trade count")
            if any(
                _is_bool_or_not_int(price_tick)
                or price_tick <= 0
                or len(values) != 2
                or any(_is_bool_or_not_int(value) or value < 0 for value in values)
                or values == [0, 0]
                for price_tick, values in active.levels.items()
            ):
                raise TickError("active levels are invalid")
            if (
                active.low_tick != min(active.levels)
                or active.high_tick != max(active.levels)
                or active.open_tick not in active.levels
                or active.last_tick not in active.levels
            ):
                raise TickError("active OHLC bounds are invalid")
            level_buy = sum(values[0] for values in active.levels.values())
            level_sell = sum(values[1] for values in active.levels.values())
            if (level_buy, level_sell) != (active.buy_units, active.sell_units):
                raise TickError("active level totals do not match state totals")
        elif state.break_before_next and state.spec_id is not None:
            raise TickError("pending break cannot retain an instrument specification")
        elif state.spec_id is not None and (
            state.last_order is None or state.next_bar_index == 0
        ):
            raise TickError("completed state requires a last order and completed bar")

        if state.last_order is None and (
            state.spec_id is not None
            or state.active is not None
            or state.next_bar_index != 0
        ):
            raise TickError("nonempty state requires last_order")


def _restore_active(value: object) -> _ActiveBar | None:
    if value is None:
        return None
    data = _mapping(value, "active")
    levels_value = data.get("levels")
    if not _is_array(levels_value):
        raise TickSnapshotError("active levels must be an array")
    levels: dict[int, list[int]] = {}
    previous_tick: int | None = None
    for index, row in enumerate(levels_value):
        if not _is_array(row) or len(row) != 3:
            raise TickSnapshotError(f"active level {index} must contain three integers")
        price_tick, buy_units, sell_units = (
            _strict_int(item, f"active.levels[{index}]") for item in row
        )
        if previous_tick is not None and price_tick <= previous_tick:
            raise TickSnapshotError("active levels must be strictly price-sorted")
        levels[price_tick] = [buy_units, sell_units]
        previous_tick = price_tick

    break_before = data.get("breakBefore")
    if not isinstance(break_before, bool):
        raise TickSnapshotError("active.breakBefore must be boolean")
    return _ActiveBar(
        open_tick=_strict_int(data.get("openTick"), "active.openTick"),
        high_tick=_strict_int(data.get("highTick"), "active.highTick"),
        low_tick=_strict_int(data.get("lowTick"), "active.lowTick"),
        last_tick=_strict_int(data.get("lastTick"), "active.lastTick"),
        break_before=break_before,
        first_order=TickOrder.from_value(
            data.get("firstOrder"), field_name="active.firstOrder"
        ),
        last_order=TickOrder.from_value(
            data.get("lastOrder"), field_name="active.lastOrder"
        ),
        levels=levels,
        buy_units=_non_negative_int(data.get("buyUnits"), "active.buyUnits"),
        sell_units=_non_negative_int(data.get("sellUnits"), "active.sellUnits"),
        trade_count=_non_negative_int(data.get("tradeCount"), "active.tradeCount"),
    )


def _mapping(value: object, field_name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TickSnapshotError(f"{field_name} must be an object")
    return value


def _optional_order(value: object, field_name: str) -> TickOrder | None:
    return None if value is None else TickOrder.from_value(value, field_name=field_name)


def _strict_int(value: object, field_name: str) -> int:
    if _is_bool_or_not_int(value):
        raise TickSnapshotError(f"{field_name} must be an integer")
    return value


def _non_negative_int(value: object, field_name: str) -> int:
    result = _strict_int(value, field_name)
    if result < 0:
        raise TickSnapshotError(f"{field_name} cannot be negative")
    return result


def _is_bool_or_not_int(value: object) -> bool:
    return isinstance(value, bool) or not isinstance(value, int)


def _is_array(value: object) -> bool:
    return isinstance(value, Sequence) and not isinstance(value, (str, bytes))
