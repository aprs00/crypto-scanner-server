from unittest.mock import MagicMock, Mock, call, patch

from django.conf import settings
from django.test import SimpleTestCase

from footprints.leadership import (
    COLLECTOR_ADVISORY_LOCK_IDS,
    FootprintWriterLeadership,
)


DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": "scanner",
        "USER": "scanner-user",
        "PASSWORD": "secret",
        "HOST": "postgres",
        "PORT": "5432",
    }
}


def _connection(*rows):
    cursor = Mock()
    cursor.fetchone.side_effect = rows
    connection = MagicMock()
    connection.cursor.return_value.__enter__.return_value = cursor
    return connection, cursor


@patch.object(settings, "DATABASES", DATABASES)
class FootprintWriterLeadershipTests(SimpleTestCase):
    def test_lock_ids_are_fixed_and_distinct(self):
        self.assertEqual(
            COLLECTOR_ADVISORY_LOCK_IDS,
            {
                "binance": 0x4353465042494E41,
                "bybit": 0x4353465042594249,
                "hyperliquid": 0x4353465048595045,
            },
        )
        self.assertEqual(len(set(COLLECTOR_ADVISORY_LOCK_IDS.values())), 3)

    def test_rejects_an_unsupported_exchange(self):
        with self.assertRaisesRegex(ValueError, "unsupported footprint exchange"):
            FootprintWriterLeadership("okx")

    @patch("footprints.leadership.psycopg.connect")
    def test_acquires_on_a_dedicated_autocommit_connection(self, connect):
        connection, cursor = _connection((True,))
        connect.return_value = connection
        leadership = FootprintWriterLeadership("binance")

        self.assertTrue(leadership.try_acquire())

        self.assertTrue(leadership.acquired)
        connect.assert_called_once_with(
            dbname="scanner",
            user="scanner-user",
            password="secret",
            host="postgres",
            port="5432",
            connect_timeout=10,
            autocommit=True,
        )
        cursor.execute.assert_called_once_with(
            "SELECT pg_try_advisory_lock(%s)",
            (COLLECTOR_ADVISORY_LOCK_IDS["binance"],),
        )

    @patch("footprints.leadership.psycopg.connect")
    def test_unavailable_lock_can_be_retried_on_the_same_session(self, connect):
        connection, cursor = _connection((False,), (True,))
        connect.return_value = connection
        leadership = FootprintWriterLeadership("bybit")

        self.assertFalse(leadership.try_acquire())
        self.assertFalse(leadership.acquired)
        self.assertTrue(leadership.try_acquire())

        self.assertTrue(leadership.acquired)
        connect.assert_called_once()
        self.assertEqual(cursor.execute.call_count, 2)

    @patch("footprints.leadership.psycopg.connect")
    def test_repeated_acquire_verifies_without_stacking_the_lock(self, connect):
        connection, cursor = _connection((True,), (1,))
        connect.return_value = connection
        leadership = FootprintWriterLeadership("hyperliquid")

        self.assertTrue(leadership.try_acquire())
        self.assertTrue(leadership.try_acquire())

        self.assertEqual(
            cursor.execute.call_args_list,
            [
                call(
                    "SELECT pg_try_advisory_lock(%s)",
                    (COLLECTOR_ADVISORY_LOCK_IDS["hyperliquid"],),
                ),
                call("SELECT 1"),
            ],
        )

    def test_verify_rejects_an_unowned_lock(self):
        leadership = FootprintWriterLeadership("binance")

        with self.assertRaisesRegex(RuntimeError, "lock is not owned"):
            leadership.verify()

    @patch("footprints.leadership.psycopg.connect")
    def test_verify_failure_closes_and_clears_ownership(self, connect):
        connection, cursor = _connection((True,))
        cursor.execute.side_effect = [None, OSError("connection lost")]
        connect.return_value = connection
        leadership = FootprintWriterLeadership("binance")
        leadership.try_acquire()

        with self.assertRaisesRegex(RuntimeError, "lock connection failed"):
            leadership.verify()

        self.assertFalse(leadership.acquired)
        connection.close.assert_called_once_with()

    @patch("footprints.leadership.psycopg.connect")
    def test_close_releases_by_closing_the_session(self, connect):
        connection, _cursor = _connection((True,))
        connect.return_value = connection
        leadership = FootprintWriterLeadership("bybit")
        leadership.try_acquire()

        leadership.close()
        leadership.close()

        self.assertFalse(leadership.acquired)
        connection.close.assert_called_once_with()
