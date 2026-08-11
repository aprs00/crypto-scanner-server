import copy
import json
from unittest import TestCase
from unittest.mock import patch

from footprints.trev import (
    MAX_BARS_PER_TRADE,
    TrevConfig,
    TrevDirection,
    TrevEngine,
    TrevError,
    TrevExpansionLimitError,
    TrevOrder,
    TrevSnapshotError,
    TrevTrade,
)


def execution(
    rank: int,
    price_tick: int,
    quantity_units: int = 1,
    *,
    buy: bool = True,
    spec_id: int = 1,
    timestamp_us: int = 1_700_000_000_000_000,
) -> TrevTrade:
    return TrevTrade(
        order=TrevOrder(timestamp_us=timestamp_us, rank=rank),
        price_tick=price_tick,
        quantity_units=quantity_units,
        is_taker_buy=buy,
        spec_id=spec_id,
    )


class TrevV1Tests(TestCase):
    def setUp(self):
        self.config = TrevConfig(continuation_ticks=3, reversal_ticks=2)
        self.engine = TrevEngine(self.config)

    def test_first_different_price_sets_direction_before_continuation_equality(self):
        first = self.engine.push(execution(0, 100, 2, buy=False))
        self.assertEqual(first.bars, ())
        self.assertEqual(self.engine.state.direction, TrevDirection.UNSET)
        self.assertEqual(self.engine.active_snapshot().open_tick, 100)

        inside = self.engine.push(execution(1, 102, 3))
        self.assertEqual(inside.bars, ())
        self.assertEqual(self.engine.state.direction, TrevDirection.UP)

        boundary = self.engine.push(execution(2, 103, 5))
        self.assertEqual(len(boundary.bars), 1)
        bar = boundary.bars[0]
        self.assertEqual((bar.open_tick, bar.high_tick, bar.low_tick, bar.close_tick), (100, 103, 100, 103))
        self.assertEqual((bar.buy_units, bar.sell_units, bar.trade_count), (8, 2, 3))
        self.assertEqual(bar.levels[-1].price_tick, 103)
        self.assertEqual(bar.levels[-1].buy_units, 5)
        self.assertIsNone(self.engine.active_snapshot())
        self.assertEqual(self.engine.state.direction, TrevDirection.UP)

    def test_push_does_not_materialize_the_active_footprint(self):
        with patch.object(
            TrevEngine,
            "active_snapshot",
            side_effect=AssertionError("active footprint was materialized per execution"),
        ):
            update = self.engine.push(execution(0, 100))

        self.assertEqual(update.bars, ())
        self.assertEqual(self.engine.active_snapshot().trade_count, 1)

    def test_first_down_move_sets_direction_and_continuation_is_symmetric(self):
        self.engine.push(execution(0, 100, 2))
        result = self.engine.push(execution(1, 97, 4, buy=False))

        bar = result.bars[0]
        self.assertEqual((bar.open_tick, bar.high_tick, bar.low_tick, bar.close_tick), (100, 100, 97, 97))
        self.assertEqual((bar.buy_units, bar.sell_units), (2, 4))
        self.assertEqual(self.engine.state.direction, TrevDirection.DOWN)

    def test_same_price_leaves_direction_unset_and_early_opposite_move_is_reversal(self):
        self.engine.push(execution(0, 100, 2))
        same = self.engine.push(execution(1, 100, 3, buy=False))
        self.assertIsNotNone(self.engine.active_snapshot())
        self.assertEqual(self.engine.state.direction, TrevDirection.UNSET)

        up = self.engine.push(execution(2, 101, 5))
        self.assertEqual(up.bars, ())
        self.assertEqual(self.engine.state.direction, TrevDirection.UP)

        reversal = self.engine.push(execution(3, 98, 7, buy=False))
        self.assertEqual(len(reversal.bars), 1)
        self.assertEqual((reversal.bars[0].open_tick, reversal.bars[0].close_tick), (100, 98))
        self.assertEqual(self.engine.state.direction, TrevDirection.DOWN)

    def test_continuation_and_reversal_thresholds_are_measured_from_active_open(self):
        self.engine.push(execution(0, 100))
        self.engine.push(execution(1, 103))

        continuation = self.engine.push(execution(2, 106, 2))
        self.assertEqual(len(continuation.bars), 1)
        self.assertEqual(self.engine.state.direction, TrevDirection.UP)

        reversal = self.engine.push(execution(3, 104, 3, buy=False))
        self.assertEqual(len(reversal.bars), 1)
        self.assertEqual((reversal.bars[0].open_tick, reversal.bars[0].close_tick), (106, 104))
        self.assertEqual(reversal.bars[0].sell_units, 3)
        self.assertEqual(self.engine.state.direction, TrevDirection.DOWN)

        down_continuation = self.engine.push(execution(4, 101, 4, buy=False))
        self.assertEqual((down_continuation.bars[0].open_tick, down_continuation.bars[0].close_tick), (104, 101))
        self.assertEqual(self.engine.state.direction, TrevDirection.DOWN)

        up_reversal = self.engine.push(execution(5, 103, 5))
        self.assertEqual((up_reversal.bars[0].open_tick, up_reversal.bars[0].close_tick), (101, 103))
        self.assertEqual(self.engine.state.direction, TrevDirection.UP)

    def test_open_side_wick_is_kept_on_reversal_bar(self):
        self.engine.push(execution(0, 100))
        self.engine.push(execution(1, 103))
        self.engine.push(execution(2, 105, 4))
        result = self.engine.push(execution(3, 101, 7, buy=False))

        bar = result.bars[0]
        self.assertEqual((bar.open_tick, bar.high_tick, bar.low_tick, bar.close_tick), (103, 105, 101, 101))
        self.assertEqual((bar.buy_units, bar.sell_units), (4, 7))

    def test_overshoot_creates_zero_volume_bars_and_assigns_execution_once(self):
        self.engine.push(execution(0, 100, 2, buy=False))
        result = self.engine.push(execution(1, 108, 7))

        self.assertEqual([(bar.open_tick, bar.close_tick) for bar in result.bars], [(100, 103), (103, 106)])
        self.assertEqual([bar.quantity_units for bar in result.bars], [2, 0])
        self.assertFalse(result.bars[0].is_synthetic)
        self.assertTrue(result.bars[1].is_synthetic)
        self.assertEqual(result.bars[1].levels, ())
        active = self.engine.active_snapshot()
        self.assertEqual((active.open_tick, active.close_tick), (106, 108))
        self.assertEqual(active.quantity_units, 7)
        self.assertEqual(sum(bar.quantity_units for bar in result.bars) + active.quantity_units, 9)

    def test_overshoot_equality_belongs_to_last_completed_bar(self):
        self.engine.push(execution(0, 100, 2, buy=False))
        result = self.engine.push(execution(1, 106, 7))

        self.assertEqual([bar.quantity_units for bar in result.bars], [2, 7])
        self.assertEqual(result.bars[-1].levels[0].price_tick, 106)
        self.assertEqual(result.bars[-1].ended_timestamp_us, execution(1, 106, 7).order.timestamp_us)
        self.assertIsNone(self.engine.active_snapshot())
        self.assertEqual(sum(bar.quantity_units for bar in result.bars), 9)

    def test_reversal_overshoot_only_assigns_trade_to_remaining_active_bar(self):
        self.engine.push(execution(0, 100, 1))
        self.engine.push(execution(1, 103, 2))
        self.engine.push(execution(2, 104, 4))
        result = self.engine.push(execution(3, 94, 9, buy=False))

        self.assertEqual(
            [(bar.open_tick, bar.close_tick) for bar in result.bars],
            [
                (103, 101),
                (101, 98),
                (98, 95),
            ],
        )
        self.assertEqual([bar.quantity_units for bar in result.bars], [4, 0, 0])
        active = self.engine.active_snapshot()
        self.assertEqual(active.quantity_units, 9)
        self.assertEqual((active.open_tick, active.close_tick), (95, 94))
        self.assertEqual(self.engine.state.direction, TrevDirection.DOWN)

    def test_active_snapshot_contains_exact_footprint_without_mutating_state(self):
        self.engine.push(execution(0, 100, 2, buy=False))
        self.engine.push(execution(1, 101, 3))
        before = self.engine.snapshot()
        active = self.engine.active_snapshot()
        after = self.engine.snapshot()

        self.assertEqual(before, after)
        self.assertEqual(active.opened_timestamp_us, active.ended_timestamp_us)
        self.assertEqual((active.open_tick, active.high_tick, active.low_tick, active.close_tick), (100, 101, 100, 101))
        self.assertEqual((active.buy_units, active.sell_units), (3, 2))
        self.assertEqual([level.to_list() for level in active.levels], [[100, 0, 2], [101, 3, 0]])

    def test_gap_truncates_partial_bar_and_starts_a_new_segment(self):
        self.engine.push(execution(0, 100, 2))
        self.engine.push(execution(1, 101, 3, buy=False))
        truncation = self.engine.truncate_segment()

        self.assertEqual(len(truncation.bars), 1)
        bar = truncation.bars[0]
        self.assertEqual(bar.quantity_units, 5)
        self.assertFalse(bar.break_before)
        self.assertIsNone(self.engine.active_snapshot())

        self.engine.push(execution(2, 200, 7))
        restarted = self.engine.active_snapshot()
        self.assertTrue(restarted.break_before)
        self.assertEqual(self.engine.state.direction, TrevDirection.UNSET)
        self.assertEqual(restarted.open_tick, 200)
        self.assertEqual(restarted.bar_index, 1)

    def test_gap_after_boundary_does_not_emit_empty_truncated_bar(self):
        self.engine.push(execution(0, 100))
        self.engine.push(execution(1, 103))
        self.assertIsNone(self.engine.active_snapshot())

        truncation = self.engine.truncate_segment()
        self.assertEqual(truncation.bars, ())
        self.assertTrue(self.engine.state.break_before_next)
        self.assertEqual(self.engine.state.next_bar_index, 1)

    def test_spec_change_truncates_automatically_before_new_trade(self):
        self.engine.push(execution(0, 100, 2, spec_id=10))
        result = self.engine.push(execution(1, 200, 5, spec_id=11))

        self.assertEqual(len(result.bars), 1)
        self.assertEqual(result.bars[0].spec_id, 10)
        active = self.engine.active_snapshot()
        self.assertEqual(active.spec_id, 11)
        self.assertTrue(active.break_before)
        self.assertEqual(active.open_tick, 200)
        self.assertEqual(active.quantity_units, 5)
        self.assertEqual(self.engine.state.direction, TrevDirection.UNSET)

    def test_equal_or_decreasing_order_is_rejected_without_mutating_state(self):
        self.engine.push(execution(0, 100, 2))
        clean = self.engine.snapshot()

        with self.assertRaisesRegex(TrevError, "strictly increase"):
            self.engine.push(execution(0, 103, 100))

        self.assertEqual(self.engine.snapshot(), clean)
        accepted = self.engine.push(execution(1, 103, 100))
        self.assertEqual(len(accepted.bars), 1)

    def test_same_timestamp_accepts_strictly_increasing_rank(self):
        timestamp = 1_700_000_000_000_000
        self.engine.push(execution(0, 100, timestamp_us=timestamp))
        self.engine.push(execution(1, 101, timestamp_us=timestamp))
        self.assertIsNotNone(self.engine.active_snapshot())
        self.assertEqual(self.engine.state.last_order.rank, 1)
        with self.assertRaises(TrevError):
            self.engine.push(execution(0, 102, timestamp_us=timestamp))

    def test_rank_order_accepts_event_timestamp_regression(self):
        self.engine.push(execution(0, 100, timestamp_us=2_000))
        self.engine.push(execution(1, 101, timestamp_us=1_000))
        self.assertIsNotNone(self.engine.active_snapshot())
        self.assertEqual(self.engine.state.last_order.timestamp_us, 1_000)

    def test_expansion_guard_is_transactional_and_exact_limit_is_allowed(self):
        rejected_engine = TrevEngine(TrevConfig(continuation_ticks=1, reversal_ticks=1))
        rejected_engine.push(execution(0, 1))
        before = rejected_engine.snapshot()
        with self.assertRaises(TrevExpansionLimitError):
            rejected_engine.push(execution(1, MAX_BARS_PER_TRADE + 2))
        self.assertEqual(rejected_engine.snapshot(), before)

        boundary_engine = TrevEngine(TrevConfig(continuation_ticks=1, reversal_ticks=1))
        boundary_engine.push(execution(0, 1))
        allowed = boundary_engine.push(execution(1, MAX_BARS_PER_TRADE + 1))
        self.assertEqual(len(allowed.bars), MAX_BARS_PER_TRADE)
        self.assertEqual(allowed.bars[-1].close_tick, MAX_BARS_PER_TRADE + 1)
        self.assertEqual(sum(bar.quantity_units for bar in allowed.bars), 2)

    def test_snapshot_round_trip_is_json_safe_and_future_output_is_identical(self):
        self.engine.push(execution(0, 100, 2, buy=False))
        self.engine.push(execution(1, 103, 3))
        self.engine.push(execution(2, 104, 5))
        snapshot = self.engine.snapshot()

        self.assertEqual(
            set(snapshot),
            {
                "direction",
                "nextBarIndex",
                "breakBeforeNext",
                "specId",
                "lastOrder",
                "active",
            },
        )
        restored = TrevEngine.restore(
            json.loads(json.dumps(snapshot)),
            config=self.config,
        )
        self.assertEqual(restored.config, self.config)
        self.assertEqual(restored.snapshot(), snapshot)

        next_trade = execution(3, 98, 7, buy=False)
        self.assertEqual(restored.push(next_trade), self.engine.push(next_trade))
        self.assertEqual(restored.snapshot(), self.engine.snapshot())

    def test_snapshot_validation_rejects_inconsistent_state(self):
        self.engine.push(execution(0, 100, 2))
        snapshot = self.engine.snapshot()
        tampered = copy.deepcopy(snapshot)
        tampered["active"]["buyUnits"] = 999
        with self.assertRaisesRegex(TrevSnapshotError, "totals"):
            TrevEngine.restore(tampered, config=self.config)

    def test_break_boundary_survives_snapshot_restore(self):
        self.engine.push(execution(0, 100))
        self.engine.truncate_segment()
        self.engine.push(execution(1, 200))

        restored = TrevEngine.restore(self.engine.snapshot(), config=self.config)
        self.assertTrue(restored.active_snapshot().break_before)
        result = restored.push(execution(2, 203))
        self.assertTrue(result.bars[0].break_before)
        self.assertEqual(result.bars[0].close_tick, 203)
        self.assertIsNone(restored.active_snapshot())

    def test_volume_is_conserved_across_mixed_trends_reversals_and_gap(self):
        inputs = [
            execution(0, 100, 2),
            execution(1, 108, 3, buy=False),
            execution(2, 107, 5),
            execution(3, 98, 7, buy=False),
            execution(4, 90, 11),
        ]
        emitted = []
        for item in inputs:
            emitted.extend(self.engine.push(item).bars)
        emitted.extend(self.engine.truncate_segment().bars)

        self.assertEqual(sum(bar.quantity_units for bar in emitted), sum(item.quantity_units for item in inputs))
        self.assertEqual(sum(bar.trade_count for bar in emitted), len(inputs))

    def test_input_validation_rejects_ambiguous_or_inexact_types(self):
        with self.assertRaises(TrevError):
            TrevConfig(continuation_ticks=0, reversal_ticks=1)
        with self.assertRaises(TrevError):
            TrevConfig(continuation_ticks=1, reversal_ticks=1, max_bars_per_trade=MAX_BARS_PER_TRADE + 1)
        with self.assertRaises(TrevError):
            TrevOrder(timestamp_us=0, rank=0)
        with self.assertRaises(TrevError):
            execution(0, 100, quantity_units=0)
        with self.assertRaises(TrevError):
            TrevTrade(
                order=TrevOrder(1, 0),
                price_tick=100,
                quantity_units=1,
                is_taker_buy=1,
                spec_id=1,
            )
