from unittest.mock import Mock, call

from django.test import SimpleTestCase

from footprints.wakeups import (
    MAINTENANCE_WAKEUP_MAX_LENGTH,
    MAINTENANCE_WAKEUP_QUEUE,
    publish_build_wakeup,
    publish_execution_wakeup,
    wait_for_wakeups,
)


class MaintenanceWakeupTests(SimpleTestCase):
    def test_execution_wakeup_is_compact_deduplicated_and_bounded(self):
        redis_client = Mock()
        pipeline = redis_client.pipeline.return_value

        publish_execution_wakeup([9, 7, 9], redis_client=redis_client)

        redis_client.pipeline.assert_called_once_with()
        self.assertEqual(
            pipeline.method_calls,
            [
                call.lpush(MAINTENANCE_WAKEUP_QUEUE, "executions:7,9"),
                call.ltrim(
                    MAINTENANCE_WAKEUP_QUEUE,
                    0,
                    MAINTENANCE_WAKEUP_MAX_LENGTH - 1,
                ),
                call.execute(),
            ],
        )

    def test_build_wakeup_uses_the_same_queue(self):
        redis_client = Mock()

        publish_build_wakeup(redis_client=redis_client)

        pipeline = redis_client.pipeline.return_value
        pipeline.lpush.assert_called_once_with(
            MAINTENANCE_WAKEUP_QUEUE,
            "build",
        )

    def test_wait_coalesces_pending_build_and_market_events(self):
        redis_client = Mock()
        redis_client.brpop.return_value = (
            MAINTENANCE_WAKEUP_QUEUE.encode(),
            b"executions:7,9",
        )
        redis_client.rpop.return_value = [
            b"build",
            b"executions:9,11",
            b"invalid",
        ]

        wakeup = wait_for_wakeups(redis_client, timeout_seconds=1)

        self.assertEqual(wakeup.market_ids, frozenset({7, 9, 11}))
        self.assertTrue(wakeup.build_requested)
        redis_client.rpop.assert_called_once_with(
            MAINTENANCE_WAKEUP_QUEUE,
            count=MAINTENANCE_WAKEUP_MAX_LENGTH - 1,
        )

    def test_wait_timeout_has_no_work(self):
        redis_client = Mock()
        redis_client.brpop.return_value = None

        self.assertIsNone(wait_for_wakeups(redis_client, timeout_seconds=1))
        redis_client.rpop.assert_not_called()

    def test_invalid_market_id_is_rejected_before_redis(self):
        redis_client = Mock()

        with self.assertRaisesRegex(ValueError, "positive integers"):
            publish_execution_wakeup([0], redis_client=redis_client)

        redis_client.pipeline.assert_not_called()
