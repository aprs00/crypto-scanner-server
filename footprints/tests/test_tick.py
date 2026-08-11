import copy
import json
from unittest import TestCase

from footprints.tick import (
    MAX_TRADES_PER_BAR,
    TickConfig,
    TickEngine,
    TickError,
    TickOrder,
    TickSnapshotError,
    TickTrade,
)


def execution(
    rank: int,
    price_tick: int,
    quantity_units: int = 1,
    *,
    buy: bool = True,
    spec_id: int = 1,
    timestamp_us: int = 1_700_000_000_000_000,
) -> TickTrade:
    return TickTrade(
        order=TickOrder(timestamp_us=timestamp_us, rank=rank),
        price_tick=price_tick,
        quantity_units=quantity_units,
        is_taker_buy=buy,
        spec_id=spec_id,
    )


class TickProjectorTests(TestCase):
    def setUp(self):
        self.config = TickConfig(trades_per_bar=3)
        self.engine = TickEngine(self.config)

    def test_each_execution_counts_once_and_full_bar_preserves_exact_footprint(self):
        self.assertEqual(self.engine.push(execution(0, 101, 50, buy=False)).bars, ())
        self.assertEqual(self.engine.push(execution(1, 99, 2)).bars, ())
        result = self.engine.push(execution(2, 101, 7))

        self.assertEqual(len(result.bars), 1)
        bar = result.bars[0]
        self.assertEqual(bar.bar_index, 0)
        self.assertEqual(
            (bar.open_tick, bar.high_tick, bar.low_tick, bar.close_tick),
            (101, 101, 99, 101),
        )
        self.assertEqual((bar.buy_units, bar.sell_units, bar.trade_count), (9, 50, 3))
        self.assertEqual(
            [level.to_list() for level in bar.levels],
            [[99, 2, 0], [101, 7, 50]],
        )
        self.assertEqual(bar.quantity_units, 59)
        self.assertIsNone(self.engine.active_snapshot())

    def test_quantity_never_changes_the_trade_threshold(self):
        engine = TickEngine(TickConfig(trades_per_bar=2))
        engine.push(execution(0, 100, 999_999))
        self.assertEqual(engine.active_snapshot().trade_count, 1)
        result = engine.push(execution(1, 100, 1, buy=False))
        self.assertEqual(result.bars[0].trade_count, 2)

    def test_active_snapshot_is_exact_and_does_not_mutate_checkpoint(self):
        self.engine.push(execution(0, 100, 2, buy=False, timestamp_us=2_000))
        self.engine.push(execution(1, 103, 5, timestamp_us=1_000))
        before = self.engine.snapshot()
        active = self.engine.active_snapshot()

        self.assertEqual(self.engine.snapshot(), before)
        self.assertEqual(
            (active.open_tick, active.high_tick, active.low_tick, active.close_tick),
            (100, 103, 100, 103),
        )
        self.assertEqual(
            (active.opened_timestamp_us, active.ended_timestamp_us), (2_000, 1_000)
        )
        self.assertEqual(
            (active.buy_units, active.sell_units, active.trade_count), (5, 2, 2)
        )

    def test_spec_change_closes_partial_and_marks_new_bar_with_break(self):
        self.engine.push(execution(0, 100, 2, spec_id=10))
        result = self.engine.push(execution(1, 200, 5, buy=False, spec_id=11))

        self.assertEqual(len(result.bars), 1)
        old = result.bars[0]
        self.assertEqual((old.spec_id, old.trade_count, old.close_tick), (10, 1, 100))
        self.assertFalse(old.break_before)
        active = self.engine.active_snapshot()
        self.assertEqual(
            (active.spec_id, active.trade_count, active.open_tick), (11, 1, 200)
        )
        self.assertTrue(active.break_before)

    def test_segment_truncation_closes_only_nonempty_partial_and_breaks_next(self):
        self.engine.push(execution(0, 100))
        self.engine.push(execution(1, 101))
        result = self.engine.truncate_segment()

        self.assertEqual(len(result.bars), 1)
        self.assertEqual(result.bars[0].trade_count, 2)
        self.assertEqual(self.engine.truncate_segment().bars, ())
        self.engine.push(execution(2, 300))
        self.assertTrue(self.engine.active_snapshot().break_before)
        self.assertEqual(self.engine.active_snapshot().bar_index, 1)

    def test_break_after_completed_bar_does_not_emit_an_empty_bar(self):
        engine = TickEngine(TickConfig(trades_per_bar=1))
        first = engine.push(execution(0, 100))
        self.assertEqual(len(first.bars), 1)
        self.assertEqual(engine.truncate_segment().bars, ())
        second = engine.push(execution(1, 200))
        self.assertTrue(second.bars[0].break_before)
        self.assertEqual(second.bars[0].bar_index, 1)

    def test_order_must_strictly_increase_without_mutating_state(self):
        self.engine.push(execution(4, 100, timestamp_us=2_000))
        before = self.engine.snapshot()
        with self.assertRaisesRegex(TickError, "strictly increase"):
            self.engine.push(execution(4, 200, timestamp_us=3_000))
        self.assertEqual(self.engine.snapshot(), before)
        self.engine.push(execution(5, 101, timestamp_us=1_000))
        self.assertEqual(self.engine.state.last_order.rank, 5)

    def test_snapshot_round_trip_restores_only_latest_state_and_future_output(self):
        self.engine.push(execution(0, 100, 2, buy=False))
        self.engine.push(execution(1, 102, 3))
        snapshot = json.loads(json.dumps(self.engine.snapshot()))
        restored = TickEngine.restore(snapshot, config=self.config)

        self.assertEqual(
            set(snapshot),
            {"nextBarIndex", "breakBeforeNext", "specId", "lastOrder", "active"},
        )
        self.assertEqual(restored.config, self.config)
        self.assertEqual(restored.snapshot(), snapshot)
        next_trade = execution(2, 99, 7, buy=False)
        self.assertEqual(restored.push(next_trade), self.engine.push(next_trade))
        self.assertEqual(restored.snapshot(), self.engine.snapshot())

    def test_snapshot_validation_rejects_corruption(self):
        self.engine.push(execution(0, 100, 2))
        snapshot = self.engine.snapshot()
        bad_totals = copy.deepcopy(snapshot)
        bad_totals["active"]["buyUnits"] = 999
        with self.assertRaisesRegex(TickSnapshotError, "totals"):
            TickEngine.restore(bad_totals, config=self.config)

        bad_levels = copy.deepcopy(snapshot)
        bad_levels["active"]["levels"].append([100, 1, 0])
        with self.assertRaisesRegex(TickSnapshotError, "price-sorted"):
            TickEngine.restore(bad_levels, config=self.config)

    def test_input_and_resource_configuration_are_strictly_validated(self):
        for value in (0, -1, True, 1.5, MAX_TRADES_PER_BAR + 1):
            with self.subTest(value=value), self.assertRaises(TickError):
                TickConfig(value)
        with self.assertRaises(TickError):
            execution(0, 0)
        with self.assertRaises(TickError):
            execution(0, 100, 0)
        with self.assertRaises(TickError):
            TickTrade(TickOrder(1, 0), 100, 1, 1, 1)
        with self.assertRaises(TickError):
            TickOrder(timestamp_us=1, rank=True)

    def test_volume_and_trade_count_are_conserved_across_boundaries(self):
        inputs = [
            execution(0, 100, 2),
            execution(1, 101, 3, buy=False),
            execution(2, 102, 5),
            execution(3, 200, 7, buy=False, spec_id=2),
            execution(4, 201, 11, spec_id=2),
        ]
        emitted = []
        for trade in inputs:
            emitted.extend(self.engine.push(trade).bars)
        emitted.extend(self.engine.truncate_segment().bars)

        self.assertEqual(sum(bar.trade_count for bar in emitted), len(inputs))
        self.assertEqual(
            sum(bar.quantity_units for bar in emitted),
            sum(t.quantity_units for t in inputs),
        )
