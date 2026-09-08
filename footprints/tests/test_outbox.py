import io
import json
import os
import tempfile
import threading
import time
from pathlib import Path
from unittest.mock import Mock, patch

from django.core.management import call_command
from django.test import SimpleTestCase

from footprints.collector import _HotStandbyRouter
from footprints.storage.outbox import ExecutionOutbox
from footprints.tests.test_collector import (
    MARKET_KEY,
    _collector,
    _recent_identity,
    _trade,
)
from footprints.types import RuntimeInstrument


class OutboxTests(SimpleTestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.outbox = ExecutionOutbox(self.directory.name, reserve_bytes=0)
        self.addCleanup(lambda: self.outbox.close())

    def execution(self, identity="41", sequence=41):
        collector = _collector()
        collector.on_trade(_trade(identity, sequence=sequence))
        return collector.queue.get_nowait()

    def test_restart_preserves_exact_pending_batch_epoch_and_recent_identity(self):
        execution = self.execution()
        self.outbox.seed(4, {7: (_recent_identity("older"),)})
        self.outbox.set_state("epoch", 5)
        self.outbox.append([execution])
        pending = self.outbox.first()
        self.outbox.close()
        self.outbox = ExecutionOutbox(self.directory.name, reserve_bytes=0)
        self.assertEqual(self.outbox.first(), pending)
        self.assertEqual(self.outbox.state("epoch"), 5)
        self.assertEqual(self.outbox.recent()[7][0].trade_id, "41")
        self.outbox.acknowledge(pending[0])
        self.assertEqual(self.outbox.stats()["pending_rows"], 0)
        self.assertEqual(self.outbox.recent()[7][0].trade_id, "41")

    def test_second_writer_cannot_open_shared_queue(self):
        with self.assertRaises(BlockingIOError):
            ExecutionOutbox(self.directory.name, reserve_bytes=0)

    def test_restart_allocates_from_disk_without_requiring_clickhouse(self):
        self.outbox.seed(5, {7: (_recent_identity("41"),)})
        collector = _collector()
        collector._outbox = self.outbox
        collector._ensure_execution_store = Mock(
            side_effect=AssertionError("ClickHouse is offline")
        )
        self.assertEqual(collector._allocate_source_epoch(), 6)
        self.assertTrue(collector._is_recent_trade(MARKET_KEY, "41"))
        collector._ensure_execution_store.assert_not_called()

    def test_fenced_inflight_insert_leaves_batch_for_next_writer_to_resolve(self):
        execution = self.execution()
        self.outbox.append([execution])
        collector = _collector()
        collector._outbox = self.outbox
        collector._outbox_ready.set()
        store = Mock()
        store.missing_executions.return_value = (execution,)
        store.insert_batch.side_effect = lambda _: collector._writer_fenced.set()
        collector._ensure_execution_store = Mock(return_value=store)
        collector._drain_outbox()
        self.assertEqual(self.outbox.first()[1], (execution,))
        collector._notify_committed_executions.assert_not_called()

    def test_docker_health_does_not_fail_healthy_capture_for_delivery_backlog(self):
        now = time.time()
        Path(self.directory.name, "health.json").write_text(
            json.dumps(
                {
                    "at": now,
                    "last_trade_at": now,
                    "capture": True,
                    "fatal": "",
                    "initialized": True,
                    "pending_rows": 500,
                    "oldest_pending_at": now - 3600,
                }
            )
        )
        output = io.StringIO()
        with patch.dict(os.environ, {"FOOTPRINT_STATE_DIR": self.directory.name}):
            call_command(
                "footprint_health", exchange="binance", capture_only=True, stdout=output
            )
        health = json.loads(output.getvalue())
        self.assertTrue(health["capture_healthy"])
        self.assertTrue(health["delivery_delayed"])
        self.assertNotIn("verification_error", health)

    def test_storage_limit_does_not_acknowledge_or_remember_rejected_batch(self):
        self.outbox.append([self.execution()])
        before = self.outbox.first()
        self.outbox.max_bytes = 1
        with self.assertRaisesRegex(OSError, "storage limit"):
            self.outbox.append([self.execution("42", 42)])
        self.assertEqual(self.outbox.first(), before)
        self.assertEqual([row.trade_id for row in self.outbox.recent()[7]], ["41"])

    def test_lost_ack_is_resolved_by_identity_before_queue_ack(self):
        execution = self.execution()
        self.outbox.append([execution])
        collector = _collector()
        collector._outbox = self.outbox
        collector._outbox_ready.set()
        store = Mock()
        store.missing_executions.side_effect = [(execution,), ()]
        store.insert_batch.side_effect = TimeoutError("committed, response lost")
        collector._ensure_execution_store = Mock(return_value=store)
        collector._close_clickhouse = Mock()
        collector._notify_committed_executions = Mock(
            side_effect=lambda _: collector.stop_event.set()
        )
        collector._drain_outbox()
        self.assertEqual(store.missing_executions.call_count, 2)
        store.insert_batch.assert_called_once_with((execution,))
        self.assertIsNone(self.outbox.first())
        self.assertIsNone(collector._fatal_error)

    def test_capture_commits_while_clickhouse_is_blocked(self):
        collector = _collector()
        collector._outbox = self.outbox
        collector._outbox_ready.set()
        entered, release = threading.Event(), threading.Event()

        def unavailable():
            entered.set()
            release.wait(5)
            raise TimeoutError("ClickHouse unavailable")

        collector._ensure_execution_store = unavailable
        collector._close_clickhouse = Mock()
        worker = threading.Thread(target=collector._journal_loop)
        worker.start()
        try:
            collector.queue.put(self.execution())
            self.assertTrue(entered.wait(3))
            collector.queue.put(self.execution("42", 42))
            deadline = time.monotonic() + 3
            while (
                self.outbox.stats()["pending_rows"] < 2 and time.monotonic() < deadline
            ):
                time.sleep(0.01)
            self.assertEqual(self.outbox.stats()["pending_rows"], 2)
            self.assertEqual(collector.queue.unfinished_tasks, 0)
        finally:
            collector.stop_event.set()
            release.set()
            worker.join(3)
        self.assertFalse(worker.is_alive())

    @patch("footprints.recovery.recent_trades")
    def test_recent_bridge_requires_stored_and_buffered_endpoints(self, recent):
        collector = _collector()
        collector.instruments = {MARKET_KEY: RuntimeInstrument(7, 11, "BTCUSDT")}
        collector._seed_recent_trades({7: (_recent_identity("41"),)})
        router = _HotStandbyRouter(collector)
        router.ready(0)
        router.ready(1)
        router.trade(0, _trade("44", sequence=44))
        recent.return_value = [_trade(str(i), sequence=i) for i in range(41, 45)]
        collector._bridge_recent_trades(router)
        router.activate()
        self.assertEqual(
            [collector.queue.get_nowait().trade.trade_id for _ in range(3)],
            ["42", "43", "44"],
        )
        self.assertTrue(router.activation_overlap_verified)

    @patch("footprints.recovery.recent_trades")
    def test_recent_snapshot_without_overlap_does_not_admit_history(self, recent):
        collector = _collector()
        collector.instruments = {MARKET_KEY: RuntimeInstrument(7, 11, "BTCUSDT")}
        collector._seed_recent_trades({7: (_recent_identity("41"),)})
        router = _HotStandbyRouter(collector)
        router.ready(0)
        router.trade(0, _trade("44", sequence=44))
        recent.return_value = [_trade("43", sequence=43), _trade("44", sequence=44)]
        collector._bridge_recent_trades(router)
        self.assertEqual(len(router.slots[0].buffers[MARKET_KEY]), 1)
        self.assertEqual(
            collector._coverage_issue["reason"], "recent_bridge_unavailable"
        )
