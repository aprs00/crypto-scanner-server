from __future__ import annotations

import queue
from decimal import Decimal
from unittest.mock import Mock, call

from django.test import SimpleTestCase

from footprints.collector import FootprintCollector, _HotStandbyRouter, _MarketMetadata
from footprints.storage.clickhouse import (
    ExecutionIdentityConflict,
    RecentExecutionIdentity,
)
from footprints.types import NormalizedTrade, TakerSide


MARKET_KEY = "binance:perpetual:BTCUSDT"
EVENT_US = 1_700_000_000_123_456


def _trade(
    trade_id: str,
    *,
    market_key: str = MARKET_KEY,
    sequence: int | None = 41,
    timestamp_us: int = EVENT_US,
    price: str = "100.125",
) -> NormalizedTrade:
    return NormalizedTrade(
        market_key=market_key,
        exchange_timestamp_us=timestamp_us,
        trade_id=trade_id,
        venue_sequence=sequence,
        price=Decimal(price),
        quantity=Decimal("0.002500"),
        taker_side=TakerSide.BUY,
    )


def _collector(
    exchange: str = "binance",
    *,
    queue_size: int = 10,
    recent_trade_limit: int = 10,
) -> FootprintCollector:
    collector = FootprintCollector(exchange)
    collector.queue = queue.Queue(maxsize=queue_size)
    collector.commit_interval_seconds = 0.001
    collector.retry_initial_seconds = 0
    collector.retry_max_seconds = 0
    collector.recent_trade_limit = recent_trade_limit
    market_key = MARKET_KEY.replace("binance:", f"{exchange}:", 1)
    collector._markets = {
        market_key: _MarketMetadata(
            market_id=7,
            price_scale=3,
            quantity_scale=6,
            default_spec_id=11,
            specifications=((EVENT_US - 1_000_000, 11),),
        )
    }
    collector._market_scales = {7: (3, 6)}
    collector._connection_source_epoch = 5
    collector._last_source_epoch = 5
    collector._notify_committed_executions = Mock()
    return collector


def _recent_identity(
    trade_id: str,
    *,
    market_id: int = 7,
    timestamp_us: int = EVENT_US,
    price_atoms: int = 100_125,
    quantity_atoms: int = 2_500,
    is_taker_buy: bool = True,
    instrument_spec_id: int = 11,
) -> RecentExecutionIdentity:
    return RecentExecutionIdentity(
        market_id=market_id,
        trade_id=trade_id,
        timestamp_us=timestamp_us,
        price_atoms=price_atoms,
        quantity_atoms=quantity_atoms,
        is_taker_buy=is_taker_buy,
        instrument_spec_id=instrument_spec_id,
    )


def _queued_executions(collector: FootprintCollector):
    executions = []
    while not collector.queue.empty():
        executions.append(collector.queue.get_nowait())
    return executions


class CollectorNormalizationTests(SimpleTestCase):
    def test_normalized_execution_is_frozen_to_exact_atoms_before_queueing(self):
        collector = _collector()

        collector.on_trade(_trade("trade-41"))

        execution = collector.queue.get_nowait()
        self.assertEqual(execution.market_id, 7)
        self.assertEqual(execution.instrument_spec_id, 11)
        self.assertEqual(execution.source_epoch, 5)
        self.assertEqual(execution.source_sequence, 41)
        self.assertEqual(execution.price_atoms, 100_125)
        self.assertEqual(execution.quantity_atoms, 2_500)

    def test_queue_full_stops_source_and_collector_instead_of_dropping(self):
        collector = _collector(queue_size=1)
        collector.on_trade(_trade("first", sequence=1))

        with self.assertRaisesRegex(RuntimeError, "queue is full"):
            collector.on_trade(_trade("second", sequence=2))

        self.assertTrue(collector._stream_stop_event.is_set())
        self.assertTrue(collector.stop_event.is_set())
        self.assertIsNotNone(collector._fatal_error)
        self.assertNotIn("second", collector._recent_trades[7])

    def test_hyperliquid_sequence_is_monotonic_per_epoch_and_dedupe_survives_reconnect(self):
        market_key = "hyperliquid:perpetual:BTCUSDT"
        collector = _collector("hyperliquid", recent_trade_limit=2)
        first = _trade("one", market_key=market_key, sequence=None)
        second = _trade(
            "two",
            market_key=market_key,
            sequence=None,
            timestamp_us=EVENT_US + 1_000,
        )
        collector.on_trade(first)
        collector.on_trade(second)

        first_execution = collector.queue.get_nowait()
        second_execution = collector.queue.get_nowait()
        self.assertEqual(
            (first_execution.source_sequence, second_execution.source_sequence),
            (0, 1),
        )
        collector._connection_source_epoch = 6
        collector._next_hyperliquid_sequence = 0
        collector.on_trade(first)
        third = _trade(
            "three",
            market_key=market_key,
            sequence=None,
            timestamp_us=EVENT_US + 2_000,
        )
        collector.on_trade(third)

        reconnected_execution = collector.queue.get_nowait()
        self.assertEqual(reconnected_execution.source_epoch, 6)
        self.assertEqual(reconnected_execution.source_sequence, 0)
        self.assertLessEqual(len(collector._recent_trades[7]), 2)

    def test_conflicting_recent_trade_identity_is_fatal(self):
        collector = _collector()
        collector.on_trade(_trade("same"))

        with self.assertRaisesRegex(RuntimeError, "conflicting immutable"):
            collector.on_trade(_trade("same", price="100.126"))

        self.assertTrue(collector.stop_event.is_set())
        self.assertTrue(collector._stream_stop_event.is_set())

    def test_restart_seed_suppresses_an_identical_native_execution(self):
        collector = _collector()
        collector._seed_recent_trades(
            {7: (_recent_identity("replayed-after-restart"),)}
        )

        collector.on_trade(_trade("replayed-after-restart"))

        self.assertTrue(collector.queue.empty())
        self.assertFalse(collector.stop_event.is_set())

    def test_restart_seed_makes_a_native_identity_conflict_fatal(self):
        collector = _collector()
        collector._seed_recent_trades(
            {
                7: (
                    _recent_identity(
                        "changed-spec",
                        instrument_spec_id=12,
                    ),
                )
            }
        )

        with self.assertRaisesRegex(
            RuntimeError,
            "conflicting immutable execution",
        ):
            collector.on_trade(_trade("changed-spec"))

        self.assertTrue(collector.queue.empty())
        self.assertTrue(collector.stop_event.is_set())
        self.assertTrue(collector._stream_stop_event.is_set())

    def test_recent_identity_limit_is_applied_independently_per_market(self):
        collector = _collector(recent_trade_limit=2)
        collector._market_scales[8] = (3, 6)
        collector._seed_recent_trades(
            {
                7: tuple(
                    _recent_identity(f"seven-{index}", timestamp_us=EVENT_US + index)
                    for index in (3, 2, 1)
                ),
                8: tuple(
                    _recent_identity(
                        f"eight-{index}",
                        market_id=8,
                        timestamp_us=EVENT_US + index,
                    )
                    for index in (3, 2, 1)
                ),
            }
        )

        self.assertEqual(list(collector._recent_trades[7]), ["seven-2", "seven-3"])
        self.assertEqual(list(collector._recent_trades[8]), ["eight-2", "eight-3"])


class CollectorRedundantFeedTests(SimpleTestCase):
    def test_first_leader_drains_trades_received_before_ready(self):
        collector = _collector()
        router = _HotStandbyRouter(collector)
        router.start(0)
        router.trade(0, _trade("one", sequence=1))

        router.ready(0)

        executions = _queued_executions(collector)
        self.assertEqual([item.trade.trade_id for item in executions], ["one"])

    def test_ready_standby_replays_once_and_becomes_leader(self):
        collector = _collector(queue_size=20)
        router = _HotStandbyRouter(collector)
        router.start(0)
        router.start(1)
        router.ready(0)
        router.ready(1)

        router.trade(0, _trade("one", sequence=1))
        router.trade(1, _trade("one", sequence=1))
        router.trade(1, _trade("two", sequence=2))

        self.assertTrue(router.disconnected(0))
        router.trade(1, _trade("three", sequence=3))

        executions = _queued_executions(collector)
        self.assertEqual(
            [execution.trade.trade_id for execution in executions],
            ["one", "two", "three"],
        )
        self.assertEqual({execution.source_epoch for execution in executions}, {5})
        self.assertEqual(router.leader_slot, 1)

    def test_standby_disconnect_does_not_interrupt_the_leader(self):
        collector = _collector(queue_size=20)
        router = _HotStandbyRouter(collector)
        router.start(0)
        router.start(1)
        router.ready(0)
        router.ready(1)
        router.trade(0, _trade("one", sequence=1))

        self.assertTrue(router.disconnected(1))
        router.trade(0, _trade("two", sequence=2))

        executions = _queued_executions(collector)
        self.assertEqual(
            [execution.trade.trade_id for execution in executions],
            ["one", "two"],
        )
        self.assertEqual(router.leader_slot, 0)

    def test_leader_disconnect_without_ready_standby_loses_coverage(self):
        collector = _collector()
        router = _HotStandbyRouter(collector)
        router.start(0)
        router.ready(0)
        router.trade(0, _trade("one", sequence=1))

        self.assertFalse(router.disconnected(0))
        self.assertIsNone(router.leader_slot)

    def test_unverified_handoff_keeps_buffered_trade_in_a_new_epoch(self):
        collector = _collector(queue_size=20)
        router = _HotStandbyRouter(collector)
        router.start(0)
        router.start(1)
        router.ready(0)
        router.ready(1)
        router.trade(0, _trade("one", sequence=1))
        router.trade(1, _trade("two", sequence=2))

        self.assertTrue(router.disconnected(0))

        executions = _queued_executions(collector)
        self.assertEqual(
            [(item.trade.trade_id, item.source_epoch) for item in executions],
            [("one", 5), ("two", 6)],
        )
        router.trade(1, _trade("three", sequence=3))
        self.assertEqual(collector.queue.get_nowait().source_epoch, 6)

    def test_unverified_handoff_replays_after_the_latest_shared_trade(self):
        collector = _collector(queue_size=20)
        router = _HotStandbyRouter(collector)
        router.start(0)
        router.start(1)
        router.ready(0)
        router.ready(1)
        router.trade(0, _trade("one", sequence=1))
        router.trade(0, _trade("two", sequence=2))
        router.trade(0, _trade("three", sequence=3))
        router.trade(1, _trade("two", sequence=2))
        router.trade(1, _trade("four", sequence=4))

        self.assertTrue(router.disconnected(0))

        executions = _queued_executions(collector)
        self.assertEqual(
            [(item.trade.trade_id, item.source_epoch) for item in executions],
            [("one", 5), ("two", 5), ("three", 5), ("four", 6)],
        )

    def test_shutdown_does_not_promote_or_replay_standby(self):
        collector = _collector(queue_size=20)
        router = _HotStandbyRouter(collector)
        router.start(0)
        router.start(1)
        router.ready(0)
        router.ready(1)
        router.trade(0, _trade("one", sequence=1))
        router.trade(1, _trade("one", sequence=1))
        router.trade(1, _trade("two", sequence=2))
        collector._stream_stop_event.set()

        self.assertTrue(router.disconnected(0))

        executions = _queued_executions(collector)
        self.assertEqual([item.trade.trade_id for item in executions], ["one"])
        self.assertIsNone(router.leader_slot)

    def test_conflicting_standby_copy_remains_fatal_on_promotion(self):
        collector = _collector(queue_size=20)
        router = _HotStandbyRouter(collector)
        router.start(0)
        router.start(1)
        router.ready(0)
        router.ready(1)
        router.trade(0, _trade("same", sequence=1))
        router.trade(1, _trade("same", sequence=1, price="100.126"))

        with self.assertRaisesRegex(RuntimeError, "conflicting immutable"):
            router.disconnected(0)

        self.assertIsInstance(collector._fatal_error, ExecutionIdentityConflict)
        self.assertTrue(collector.stop_event.is_set())

    def test_bybit_handoff_assigns_one_canonical_batch_order(self):
        market_key = "bybit:perpetual:BTCUSDT"
        collector = _collector("bybit", queue_size=20)
        router = _HotStandbyRouter(collector)
        router.start(0)
        router.start(1)
        router.ready(0)
        router.ready(1)

        router.trade(
            0,
            _trade("one", market_key=market_key, sequence=50),
        )
        router.trade(
            1,
            _trade("one", market_key=market_key, sequence=50),
        )
        router.trade(
            1,
            _trade("two", market_key=market_key, sequence=50),
        )
        self.assertTrue(router.disconnected(0))

        executions = _queued_executions(collector)
        self.assertEqual(
            [execution.trade.trade_id for execution in executions],
            ["one", "two"],
        )
        self.assertEqual(
            [execution.trade.batch_index for execution in executions],
            [0, 1],
        )


class CollectorDeliveryTests(SimpleTestCase):
    def test_writer_drains_healthy_queue_after_shutdown_starts(self):
        collector = _collector()
        collector.commit_batch_size = 1
        collector.on_trade(_trade("first", sequence=1))
        collector.on_trade(_trade("second", sequence=2))
        store = Mock()
        collector._ensure_execution_store = Mock(return_value=store)
        collector.stop()

        collector._writer_loop()

        self.assertEqual(store.insert_batch.call_count, 2)
        self.assertEqual(collector._notify_committed_executions.call_count, 2)
        self.assertTrue(collector.queue.empty())
        self.assertEqual(collector.queue.unfinished_tasks, 0)

    def test_shutdown_attempts_unavailable_batch_once(self):
        collector = _collector()
        collector.shutdown_drain_seconds = 0
        collector.on_trade(_trade("shutdown-attempt"))
        batch = (collector.queue.get_nowait(),)
        store = Mock()
        store.insert_batch.side_effect = TimeoutError("unavailable")
        collector._ensure_execution_store = Mock(return_value=store)
        collector._close_clickhouse = Mock()
        collector.stop()

        self.assertFalse(collector._insert_with_retry(batch))

        store.insert_batch.assert_called_once_with(batch)

    def test_shutdown_retries_until_the_batch_is_committed_within_deadline(self):
        collector = _collector()
        collector.shutdown_drain_seconds = 1
        collector.on_trade(_trade("shutdown-retry"))
        store = Mock()
        store.insert_batch.side_effect = [TimeoutError("unavailable"), None]
        collector._ensure_execution_store = Mock(return_value=store)
        collector._close_clickhouse = Mock()
        collector.stop()

        collector._writer_loop()

        self.assertEqual(store.insert_batch.call_count, 2)
        self.assertEqual(collector._uncommitted_batch_count, 0)
        self.assertTrue(collector.queue.empty())

    def test_shutdown_deadline_records_every_uncommitted_execution(self):
        collector = _collector()
        collector.commit_batch_size = 1
        collector.on_trade(_trade("first", sequence=1))
        collector.on_trade(_trade("second", sequence=2))
        store = Mock()

        def expire_and_fail(_batch):
            collector._shutdown_deadline = 0
            raise TimeoutError("unavailable")

        store.insert_batch.side_effect = expire_and_fail
        collector._ensure_execution_store = Mock(return_value=store)
        collector._close_clickhouse = Mock()
        collector.stop()

        collector._writer_loop()

        uncommitted = collector._uncommitted_batch_count + collector.queue.qsize()
        self.assertEqual(uncommitted, 2)
        self.assertEqual(store.insert_batch.call_count, 1)

    def test_shutdown_deadline_stops_between_successful_batches(self):
        collector = _collector()
        collector.commit_batch_size = 1
        collector.on_trade(_trade("first", sequence=1))
        collector.on_trade(_trade("second", sequence=2))
        store = Mock()
        store.insert_batch.side_effect = lambda _batch: setattr(
            collector,
            "_shutdown_deadline",
            0,
        )
        collector._ensure_execution_store = Mock(return_value=store)
        collector.stop()

        collector._writer_loop()

        self.assertEqual(store.insert_batch.call_count, 1)
        collector._notify_committed_executions.assert_called_once()
        self.assertEqual(collector.queue.qsize(), 1)

    def test_retry_reuses_the_exact_rows(self):
        collector = _collector()
        collector.on_trade(_trade("retry-me"))
        batch = (collector.queue.get_nowait(),)

        store = Mock()
        store.insert_batch.side_effect = [
            TimeoutError("acknowledgement lost"),
            None,
        ]
        collector._ensure_execution_store = Mock(return_value=store)
        collector._close_clickhouse = Mock()

        self.assertTrue(collector._insert_with_retry(batch))

        first_attempt = store.insert_batch.call_args_list[0].args[0]
        second_attempt = store.insert_batch.call_args_list[1].args[0]
        self.assertIs(first_attempt, second_attempt)
        self.assertEqual(first_attempt, batch)
        self.assertTrue(collector._stream_stop_event.is_set())
        self.assertFalse(collector.stop_event.is_set())

    def test_source_epoch_advances_from_clickhouse_and_local_connection_floor(self):
        collector = _collector("hyperliquid")
        store = Mock()
        store.recent_execution_identities.return_value = {7: ()}
        store.max_source_epoch.side_effect = [12, 4]
        collector._ensure_execution_store = Mock(return_value=store)

        first = collector._allocate_source_epoch()
        collector._next_hyperliquid_sequence = 99
        second = collector._allocate_source_epoch()

        self.assertEqual((first, second), (13, 14))
        self.assertEqual(collector._connection_source_epoch, 14)
        self.assertEqual(collector._next_hyperliquid_sequence, 0)
        self.assertEqual(store.max_source_epoch.call_args_list[0].args[0], [7])
        self.assertEqual(
            store.method_calls[:2],
            [
                call.recent_execution_identities([7], per_market_limit=10),
                call.max_source_epoch([7]),
            ],
        )

    def test_seeded_database_conflict_stops_before_allocating_an_epoch(self):
        collector = _collector()
        store = Mock()
        store.recent_execution_identities.return_value = {
            7: (
                _recent_identity("database-conflict", price_atoms=100_126),
                _recent_identity("database-conflict", price_atoms=100_125),
            )
        }
        collector._ensure_execution_store = Mock(return_value=store)
        collector._close_clickhouse = Mock()

        source_epoch = collector._allocate_source_epoch_with_retry()

        self.assertIsNone(source_epoch)
        store.max_source_epoch.assert_not_called()
        self.assertIsInstance(collector._fatal_error, ExecutionIdentityConflict)
        self.assertTrue(collector.stop_event.is_set())
        self.assertTrue(collector._stream_stop_event.is_set())

    def test_close_releases_the_owned_clickhouse_client(self):
        collector = _collector()
        access = Mock()
        collector._clickhouse_access = access
        collector._execution_store = Mock()

        collector._close_clickhouse()

        access.close.assert_called_once_with()
        self.assertIsNone(collector._clickhouse_access)
        self.assertIsNone(collector._execution_store)
