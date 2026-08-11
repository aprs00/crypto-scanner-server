from __future__ import annotations

import sys
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import Mock, patch

from django.test import SimpleTestCase

from footprints.storage.clickhouse import (
    ClickHouseAccess,
    ClickHouseConfigurationError,
    ClickHouseExecutionStore,
    ClickHouseFailureKind,
    ClickHouseSettings,
    ExecutionIdentityConflict,
    classify_clickhouse_error,
    create_clickhouse_client,
    deduplicated_insert_settings,
)
from footprints.storage.executions import StoredExecution
from footprints.types import NormalizedTrade, TakerSide


class _RowsStream:
    def __init__(self, rows):
        self.rows = rows

    def __enter__(self):
        return iter(self.rows)

    def __exit__(self, exc_type, exc, traceback):
        return False


class _RecordingClient:
    def __init__(self, *, query_rows=(), stream_rows=()):
        self.query_rows = list(query_rows)
        self.stream_result_rows = list(stream_rows)
        self.commands = []
        self.queries = []
        self.inserts = []
        self.stream_queries = []
        self.closed = False

    def command(self, sql, parameters=None, settings=None, **kwargs):
        self.commands.append((sql, parameters, settings))
        return "command-result"

    def query(self, sql, parameters=None, settings=None, **kwargs):
        self.queries.append((sql, parameters, settings))
        return SimpleNamespace(result_rows=tuple(self.query_rows))

    def insert(self, table, data, column_names=None, settings=None, **kwargs):
        self.inserts.append((table, tuple(data), tuple(column_names or ()), settings))
        return "insert-result"

    def query_rows_stream(self, sql, parameters=None, settings=None, **kwargs):
        self.stream_queries.append((sql, parameters, settings))
        return _RowsStream(self.stream_result_rows)

    def close(self):
        self.closed = True


def _settings() -> ClickHouseSettings:
    return ClickHouseSettings(
        host="clickhouse",
        port=8123,
        database="footprints",
        username="footprints",
        password="secret",
        secure=False,
    )


def _execution(
    sequence: int,
    trade_id: str,
    *,
    timestamp_us: int | None = None,
    price: str = "100.125",
) -> StoredExecution:
    event_us = (
        timestamp_us
        if timestamp_us is not None
        else 1_700_000_000_000_000 + sequence
    )
    trade = NormalizedTrade(
        market_key="binance:perpetual:BTCUSDT",
        exchange_timestamp_us=event_us,
        trade_id=trade_id,
        venue_sequence=sequence,
        price=Decimal(price),
        quantity=Decimal("0.002500"),
        taker_side=TakerSide.BUY,
    )
    return StoredExecution.from_trade(
        market_id=7,
        instrument_spec_id=11,
        source_epoch=3,
        price_scale=3,
        quantity_scale=6,
        trade=trade,
    )


def _batch(*executions: StoredExecution) -> tuple[StoredExecution, ...]:
    return executions


class ClickHouseConfigurationTests(SimpleTestCase):
    def test_environment_is_strict_and_bounded(self):
        settings = ClickHouseSettings.from_environment(
            {
                "CLICKHOUSE_HOST": "127.0.0.1",
                "CLICKHOUSE_DATABASE": "footprints_test",
                "CLICKHOUSE_USER": "writer",
                "CLICKHOUSE_PASSWORD": "secret",
                "CLICKHOUSE_PORT": "18123",
                "CLICKHOUSE_SECURE": "yes",
            }
        )
        self.assertEqual(settings.database, "footprints_test")
        self.assertEqual(settings.port, 18123)
        self.assertTrue(settings.secure)

        with self.assertRaisesRegex(ClickHouseConfigurationError, "PASSWORD"):
            ClickHouseSettings.from_environment({})
        with self.assertRaisesRegex(ClickHouseConfigurationError, "DATABASE"):
            ClickHouseSettings.from_environment(
                {
                    "CLICKHOUSE_DATABASE": "footprints; DROP DATABASE default",
                    "CLICKHOUSE_PASSWORD": "secret",
                }
            )
        with self.assertRaisesRegex(ClickHouseConfigurationError, "boolean"):
            ClickHouseSettings.from_environment(
                {"CLICKHOUSE_PASSWORD": "secret", "CLICKHOUSE_SECURE": "sometimes"}
            )

    def test_factory_uses_the_selected_database_and_timeout(self):
        get_client = Mock(return_value=object())
        with patch.dict(
            sys.modules,
            {"clickhouse_connect": SimpleNamespace(get_client=get_client)},
        ):
            create_clickhouse_client(
                _settings(),
                role="schema-bootstrap",
                bootstrap_database=True,
            )
        bootstrap_kwargs = get_client.call_args.kwargs
        self.assertEqual(bootstrap_kwargs["database"], "default")
        self.assertEqual(bootstrap_kwargs["send_receive_timeout"], 30)
        self.assertFalse(bootstrap_kwargs["autogenerate_session_id"])

        with patch.dict(
            sys.modules,
            {"clickhouse_connect": SimpleNamespace(get_client=get_client)},
        ):
            create_clickhouse_client(_settings(), role="collector", for_inserts=True)
        insert_kwargs = get_client.call_args.kwargs
        self.assertEqual(insert_kwargs["database"], "footprints")
        self.assertEqual(insert_kwargs["send_receive_timeout"], 60)
        self.assertEqual(insert_kwargs["settings"], {"max_execution_time": 60})

        with patch.dict(
            sys.modules,
            {"clickhouse_connect": SimpleNamespace(get_client=get_client)},
        ):
            create_clickhouse_client(
                _settings(),
                role="collector",
                for_inserts=True,
                operation_timeout_seconds=10,
            )
        bounded_kwargs = get_client.call_args.kwargs
        self.assertEqual(bounded_kwargs["send_receive_timeout"], 10)
        self.assertEqual(bounded_kwargs["settings"], {"max_execution_time": 10})

    def test_write_ambiguity_is_distinct_from_safe_read_retry(self):
        timeout = TimeoutError("acknowledgement lost")
        self.assertEqual(
            classify_clickhouse_error(timeout, is_write=True),
            ClickHouseFailureKind.AMBIGUOUS_WRITE,
        )
        self.assertEqual(
            classify_clickhouse_error(timeout, is_write=False),
            ClickHouseFailureKind.RETRYABLE,
        )
        overloaded = RuntimeError("too many parts")
        overloaded.code = 252
        self.assertEqual(
            classify_clickhouse_error(overloaded, is_write=True),
            ClickHouseFailureKind.RETRYABLE,
        )
        self.assertEqual(
            classify_clickhouse_error(ValueError("bad data"), is_write=True),
            ClickHouseFailureKind.FATAL,
        )

    def test_insert_settings_batch_compatibly_and_wait_for_durable_flush(self):
        self.assertEqual(
            deduplicated_insert_settings(),
            {
                "async_insert": 1,
                "wait_for_async_insert": 1,
                "async_insert_deduplicate": 1,
                "async_insert_use_adaptive_busy_timeout": 0,
                "async_insert_busy_timeout_ms": 500,
                "insert_deduplicate": 1,
            },
        )


class ClickHouseAccessTests(SimpleTestCase):
    def test_access_delegates_each_live_operation_and_closes(self):
        client = _RecordingClient(stream_rows=((1,), (2,)))
        access = ClickHouseAccess(client)

        command = access.command(
            "CREATE DATABASE x",
            parameters={"x": 1},
            operation="create",
        )
        query = access.query("SELECT 1", operation="query")
        inserted = access.insert(
            "footprints.raw_executions",
            [(1,)],
            column_names=("market_id",),
            operation="insert",
        )
        streamed = list(access.stream_rows("SELECT x", operation="stream"))
        access.close()

        self.assertEqual(command, "command-result")
        self.assertEqual(query.result_rows, ())
        self.assertEqual(inserted, "insert-result")
        self.assertEqual(streamed, [(1,), (2,)])
        self.assertTrue(client.closed)

    def test_insert_rejects_an_unsafe_table_before_calling_client(self):
        client = _RecordingClient()
        with self.assertRaises(ClickHouseConfigurationError):
            ClickHouseAccess(client).insert(
                "footprints.raw_executions;DROP",
                [(1,)],
                column_names=("market_id",),
            )
        self.assertEqual(client.inserts, [])


class ClickHouseExecutionStoreTests(SimpleTestCase):
    @staticmethod
    def _store(client):
        return ClickHouseExecutionStore(
            ClickHouseAccess(client),
            database="footprints",
        )

    def test_insert_maps_exact_atoms_to_all_raw_columns(self):
        execution = _execution(41, "trade-41")
        client = _RecordingClient()

        self._store(client).insert_batch(_batch(execution))

        table, rows, columns, settings = client.inserts[0]
        self.assertEqual(table, "footprints.raw_executions")
        self.assertEqual(len(columns), 10)
        self.assertEqual(len(rows[0]), len(columns))
        mapped = dict(zip(columns, rows[0], strict=True))
        self.assertEqual(mapped["market_id"], 7)
        self.assertEqual(mapped["source_epoch"], 3)
        self.assertEqual(mapped["source_sequence"], 41)
        self.assertEqual(mapped["price_atoms"], 100_125)
        self.assertEqual(mapped["quantity_atoms"], 2_500)
        self.assertEqual(settings["async_insert"], 1)
        self.assertEqual(settings["wait_for_async_insert"], 1)
        self.assertEqual(settings["async_insert_deduplicate"], 1)
        self.assertEqual(settings["async_insert_use_adaptive_busy_timeout"], 0)
        self.assertEqual(settings["async_insert_busy_timeout_ms"], 500)
        self.assertEqual(settings["insert_deduplicate"], 1)
        self.assertNotIn("insert_deduplication_token", settings)
        self.assertEqual(client.queries, [])

    def test_batch_duplicate_is_collapsed_and_conflict_is_fatal(self):
        execution = _execution(1, "same")
        client = _RecordingClient()

        self._store(client).insert_batch(_batch(execution, execution))

        self.assertEqual(len(client.inserts[0][1]), 1)

        conflict = _execution(1, "same", price="100.126")
        conflict_client = _RecordingClient()
        with self.assertRaisesRegex(
            ExecutionIdentityConflict,
            "conflicting immutable execution",
        ):
            self._store(conflict_client).insert_batch(_batch(execution, conflict))
        self.assertEqual(conflict_client.inserts, [])

    def test_reads_maximum_source_epoch_for_current_markets(self):
        client = _RecordingClient(query_rows=((17,),))

        epoch = self._store(client).max_source_epoch([7, 8, 7])

        self.assertEqual(epoch, 17)
        sql, parameters, _settings = client.queries[0]
        self.assertIn("maxOrNull(source_epoch)", sql)
        self.assertEqual(parameters, {"market_ids": [7, 8]})

        empty = _RecordingClient(query_rows=((None,),))
        self.assertEqual(self._store(empty).max_source_epoch([7]), 0)

    def test_reads_recent_identities_in_reverse_primary_key_order(self):
        client = _RecordingClient(
            query_rows=(
                ("newest", 1_700_000_000_000_002, 100_126, 2_501, False, 12),
                ("older", 1_700_000_000_000_001, 100_125, 2_500, True, 11),
            )
        )

        recent = self._store(client).recent_execution_identities(
            [7, 7],
            per_market_limit=250,
        )

        self.assertEqual([row.trade_id for row in recent[7]], ["newest", "older"])
        self.assertEqual(
            recent[7][0].signature,
            (1_700_000_000_000_002, 100_126, 2_501, False, 12),
        )
        self.assertEqual(len(client.queries), 1)
        sql, parameters, _settings = client.queries[0]
        self.assertIn("WHERE market_id = {market_id:UInt64}", sql)
        self.assertIn("source_epoch DESC", sql)
        self.assertIn("source_sequence DESC", sql)
        self.assertIn("batch_index DESC", sql)
        self.assertIn("timestamp_us DESC", sql)
        self.assertIn("trade_id DESC", sql)
        self.assertEqual(
            parameters,
            {"market_id": 7, "recent_limit": 250},
        )

    def test_recent_identity_read_rejects_non_native_boolean(self):
        client = _RecordingClient(
            query_rows=(("bad-side", 1_700_000_000_000_001, 100_125, 2_500, 1, 11),)
        )

        with self.assertRaisesRegex(RuntimeError, "taker side must be a boolean"):
            self._store(client).recent_execution_identities(
                [7],
                per_market_limit=10,
            )
