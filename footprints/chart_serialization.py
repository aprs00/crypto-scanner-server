from __future__ import annotations

from decimal import Decimal
from typing import Mapping


def serialize_chart_bar(bar, specs: Mapping[int, object]) -> dict:
    spec = specs[int(bar.instrument_spec_id)]
    return {
        "barIndex": int(bar.bar_index),
        "openedAtMs": int(bar.opened_at.timestamp() * 1_000),
        "breakBefore": bool(bar.break_before),
        "tickSize": _decimal_string(spec.tick_size),
        "quantityStep": _decimal_string(spec.quantity_step),
        "openTick": int(bar.open_tick),
        "highTick": int(bar.high_tick),
        "lowTick": int(bar.low_tick),
        "closeTick": int(bar.close_tick),
        "buyUnits": int(bar.buy_units),
        "sellUnits": int(bar.sell_units),
        "tradeCount": int(bar.trade_count),
        "levels": [
            [int(level.price_tick), int(level.buy_units), int(level.sell_units)]
            for level in bar.levels
        ],
    }


def serialize_snapshot_bar(bar, specs: Mapping[int, object]) -> dict:
    spec = specs[int(bar.instrument_spec_id)]
    return {
        "barIndex": int(bar.bar_index),
        "openedAtMs": int(bar.opened_at.timestamp() * 1_000),
        "breakBefore": bool(bar.break_before),
        "tickSize": _decimal_string(spec.tick_size),
        "quantityStep": _decimal_string(spec.quantity_step),
        "openTick": int(bar.open_tick),
        "highTick": int(bar.high_tick),
        "lowTick": int(bar.low_tick),
        "closeTick": int(bar.close_tick),
        "buyUnits": int(bar.buy_units),
        "sellUnits": int(bar.sell_units),
        "tradeCount": int(bar.trade_count),
        "levels": [
            [price_tick, buy_units, sell_units]
            for price_tick, buy_units, sell_units in zip(
                bar.level_ticks,
                bar.level_buy_units,
                bar.level_sell_units,
                strict=True,
            )
        ],
    }


def serialize_active_bar(bar, specs: Mapping[int, object]) -> dict:
    spec = specs[int(bar.spec_id)]
    return {
        "barIndex": int(bar.bar_index),
        "openedAtMs": int(bar.opened_timestamp_us) // 1_000,
        "breakBefore": bool(bar.break_before),
        "tickSize": _decimal_string(spec.tick_size),
        "quantityStep": _decimal_string(spec.quantity_step),
        "openTick": int(bar.open_tick),
        "highTick": int(bar.high_tick),
        "lowTick": int(bar.low_tick),
        "closeTick": int(bar.close_tick),
        "buyUnits": int(bar.buy_units),
        "sellUnits": int(bar.sell_units),
        "tradeCount": int(bar.trade_count),
        "levels": [
            [int(level.price_tick), int(level.buy_units), int(level.sell_units)]
            for level in bar.levels
        ],
    }


def _decimal_string(value: Decimal) -> str:
    rendered = format(value.normalize(), "f")
    return rendered if "." not in rendered else rendered.rstrip("0").rstrip(".")
