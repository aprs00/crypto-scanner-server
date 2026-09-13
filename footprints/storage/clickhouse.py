from __future__ import annotations

import logging
import os
import re
import time
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from enum import Enum
from typing import Any

from footprints.storage.executions import StoredExecution
from footprints.types import TakerSide


logger = logging.getLogger(__name__)

DEFAULT_CLICKHOUSE_DATABASE = "footprints"
DEFAULT_CLICKHOUSE_PORT = 8123
DEFAULT_CONNECT_TIMEOUT_SECONDS = 5
DEFAULT_QUERY_TIMEOUT_SECONDS = 30
DEFAULT_INSERT_TIMEOUT_SECONDS = 60
DEFAULT_QUERY_RETRIES = 2
RAW_ASYNC_INSERT_BUSY_TIMEOUT_MS = 100

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_NETWORK_ERROR_CODES = {159, 209, 210, 279, 319}
_OVERLOAD_ERROR_CODES = {202, 203, 252}
_NETWORK_EXCEPTION_NAMES = {
    "ConnectionError",
    "ConnectTimeoutError",
    "HTTPError",
    "InterfaceError",
    "NetworkError",
    "OperationalError",
    "ProtocolError",
    "ReadTimeoutError",
    "TimeoutError",
}


class ClickHouseConfigurationError(RuntimeError):
    """Raised before connecting when ClickHouse configuration is unsafe."""


class ClickHouseFailureKind(str, Enum):
    FATAL = "fatal"
    RETRYABLE = "retryable"
    AMBIGUOUS_WRITE = "ambiguous_write"


@dataclass(frozen=True, slots=True)
class ClickHouseSettings:
    host: str
    port: int
    database: str
    username: str
    password: str
    secure: bool

    @classmethod
    def from_environment(
        cls,
        environment: Mapping[str, str] | None = None,
    ) -> "ClickHouseSettings":
        values = os.environ if environment is None else environment
        host = values.get("CLICKHOUSE_HOST", "clickhouse").strip()
        database = values.get(
            "CLICKHOUSE_DATABASE", DEFAULT_CLICKHOUSE_DATABASE
        ).strip()
        username = values.get("CLICKHOUSE_USER", "footprints").strip()
        password = values.get("CLICKHOUSE_PASSWORD", "")
        if not host:
            raise ClickHouseConfigurationError("CLICKHOUSE_HOST cannot be empty")
        validate_identifier(database, label="CLICKHOUSE_DATABASE")
        if not username:
            raise ClickHouseConfigurationError("CLICKHOUSE_USER cannot be empty")
        if not password:
            raise ClickHouseConfigurationError("CLICKHOUSE_PASSWORD cannot be empty")
        return cls(
            host=host,
            port=_environment_int(
                values,
                "CLICKHOUSE_PORT",
                DEFAULT_CLICKHOUSE_PORT,
                minimum=1,
                maximum=65_535,
            ),
            database=database,
            username=username,
            password=password,
            secure=_environment_bool(values, "CLICKHOUSE_SECURE", False),
        )


def create_clickhouse_client(
    configuration: ClickHouseSettings | None = None,
    *,
    role: str,
    for_inserts: bool = False,
    bootstrap_database: bool = False,
    operation_timeout_seconds: int | None = None,
) -> Any:
    """Create one reusable process-level client.

    The caller owns the client lifecycle. Insert retries intentionally remain
    outside the driver so the caller can resend one unchanged batch.
    """

    if not role or not role.strip():
        raise ClickHouseConfigurationError("ClickHouse client role cannot be empty")
    config = configuration or ClickHouseSettings.from_environment()
    try:
        import clickhouse_connect
    except ImportError as exc:  # pragma: no cover - installation failure path
        raise ClickHouseConfigurationError(
            "clickhouse-connect is not installed; install requirements.txt"
        ) from exc

    default_timeout = (
        DEFAULT_INSERT_TIMEOUT_SECONDS if for_inserts else DEFAULT_QUERY_TIMEOUT_SECONDS
    )
    timeout = (
        default_timeout
        if operation_timeout_seconds is None
        else operation_timeout_seconds
    )
    if isinstance(timeout, bool) or not isinstance(timeout, int) or timeout <= 0:
        raise ClickHouseConfigurationError(
            "ClickHouse operation timeout must be a positive integer"
        )
    return clickhouse_connect.get_client(
        host=config.host,
        port=config.port,
        username=config.username,
        password=config.password,
        database="default" if bootstrap_database else config.database,
        secure=config.secure,
        connect_timeout=DEFAULT_CONNECT_TIMEOUT_SECONDS,
        send_receive_timeout=timeout,
        query_retries=DEFAULT_QUERY_RETRIES,
        autogenerate_session_id=False,
        client_name=f"crypto-scanner-footprints/{role.strip()}",
        compress=True,
        settings={"max_execution_time": timeout},
    )


class ClickHouseAccess:
    """Observable, non-retrying access to one ClickHouse client.

    ClickHouse Connect owns its bounded read-retry budget. Commands and writes
    are deliberately attempted once; their caller decides whether the same
    stable batch can be retried.
    """

    def __init__(self, client: Any):
        self.client = client

    def command(
        self,
        sql: str,
        *,
        parameters: Mapping[str, object] | Sequence[object] | None = None,
        settings: Mapping[str, object] | None = None,
        operation: str = "command",
    ) -> object:
        return self._observe(
            operation,
            lambda: self.client.command(
                sql,
                parameters=parameters,
                settings=settings,
            ),
            is_write=True,
        )

    def query(
        self,
        sql: str,
        *,
        parameters: Mapping[str, object] | Sequence[object] | None = None,
        settings: Mapping[str, object] | None = None,
        operation: str = "query",
    ) -> object:
        return self._observe(
            operation,
            lambda: self.client.query(
                sql,
                parameters=parameters,
                settings=settings,
            ),
            is_write=False,
        )

    def insert(
        self,
        table: str,
        rows: Sequence[Sequence[object]],
        *,
        column_names: Sequence[str],
        settings: Mapping[str, object] | None = None,
        operation: str = "insert",
    ) -> object:
        safe_table = validate_qualified_identifier(table)
        safe_columns = tuple(
            validate_identifier(name, label="ClickHouse column")
            for name in column_names
        )
        return self._observe(
            operation,
            lambda: self.client.insert(
                safe_table,
                rows,
                column_names=safe_columns,
                settings=settings,
            ),
            is_write=True,
        )

    def insert_native(
        self, table: str, data: bytes, *, column_names: Sequence[str],
        settings: Mapping[str, object] | None = None,
        operation: str = "insert_native",
    ) -> object:
        safe_table = validate_qualified_identifier(table)
        safe_columns = tuple(
            validate_identifier(name, label="ClickHouse column")
            for name in column_names
        )
        return self._observe(
            operation,
            lambda: self.client.raw_insert(
                safe_table, column_names=safe_columns, insert_block=data,
                settings=settings, fmt="Native",
            ),
            is_write=True,
        )

    def stream_rows(
        self,
        sql: str,
        *,
        parameters: Mapping[str, object] | Sequence[object] | None = None,
        settings: Mapping[str, object] | None = None,
        operation: str = "stream_query",
    ) -> Iterator[Sequence[object]]:
        started = time.monotonic()
        try:
            with self.client.query_rows_stream(
                sql,
                parameters=parameters,
                settings=settings,
            ) as stream:
                yield from stream
        except Exception as exc:
            elapsed_ms = round((time.monotonic() - started) * 1_000, 2)
            kind = classify_clickhouse_error(exc, is_write=False)
            logger.warning(
                "ClickHouse operation failed operation=%s elapsed_ms=%s kind=%s error=%s",
                operation,
                elapsed_ms,
                kind.value,
                type(exc).__name__,
            )
            raise
        elapsed_ms = round((time.monotonic() - started) * 1_000, 2)
        logger.debug(
            "ClickHouse operation completed operation=%s elapsed_ms=%s",
            operation,
            elapsed_ms,
        )

    def stream_rowbinary(
        self,
        sql: str,
        *,
        parameters=None,
        operation: str = "stream_projector_rowbinary",
    ) -> Iterator[bytes]:
        """Keep ClickHouse's bounded HTTP stream as bytes for native decoding."""
        started = time.monotonic()
        try:
            with self.client.raw_stream(
                sql, parameters=parameters, fmt="RowBinary"
            ) as stream:
                while block := stream.read(8 * 1024 * 1024):
                    yield block
        except Exception as exc:
            logger.warning(
                "ClickHouse operation failed operation=%s elapsed_ms=%s kind=%s error=%s",
                operation,
                round((time.monotonic() - started) * 1_000, 2),
                classify_clickhouse_error(exc, is_write=False).value,
                type(exc).__name__,
            )
            raise
        logger.debug(
            "ClickHouse operation completed operation=%s elapsed_ms=%s",
            operation,
            round((time.monotonic() - started) * 1_000, 2),
        )

    def close(self) -> None:
        self.client.close()

    @staticmethod
    def _observe(operation: str, callback, *, is_write: bool) -> object:
        started = time.monotonic()
        try:
            result = callback()
        except Exception as exc:
            elapsed_ms = round((time.monotonic() - started) * 1_000, 2)
            kind = classify_clickhouse_error(exc, is_write=is_write)
            logger.warning(
                "ClickHouse operation failed operation=%s elapsed_ms=%s kind=%s error=%s",
                operation,
                elapsed_ms,
                kind.value,
                type(exc).__name__,
            )
            raise
        elapsed_ms = round((time.monotonic() - started) * 1_000, 2)
        logger.debug(
            "ClickHouse operation completed operation=%s elapsed_ms=%s",
            operation,
            elapsed_ms,
        )
        return result


def deduplicated_insert_settings() -> dict[str, object]:
    """Batch compatible inserts while retaining durable retry acknowledgements.

    ClickHouse 26.3 preserves a per-source-insert hash inside one asynchronous
    flush.  Omitting an explicit token is intentional: a unique token is part of
    the async-buffer key and would turn every small request back into its own
    MergeTree part.  Callers retry the exact same ordered rows, so the server's
    content hash remains stable across an ambiguous acknowledgement.
    """

    return {
        "async_insert": 1,
        "wait_for_async_insert": 1,
        "async_insert_deduplicate": 1,
        "async_insert_use_adaptive_busy_timeout": 0,
        "async_insert_busy_timeout_ms": RAW_ASYNC_INSERT_BUSY_TIMEOUT_MS,
        "insert_deduplicate": 1,
    }


def deduplicated_immediate_insert_settings() -> dict[str, object]:
    """Synchronously acknowledge one already-batched, retry-safe insert.

    The chart maintenance process is the only chart-bar writer and already
    combines every bar produced by one advancement pass. Sending those rows
    through ClickHouse's asynchronous insert queue adds the configured busy
    timeout to every publication without creating a larger batch. Keep raw
    collector writes asynchronous, but let chart checkpoints advance as soon
    as their corresponding rows are durable.
    """

    return {
        "async_insert": 0,
        "insert_deduplicate": 1,
    }


class ExecutionIdentityConflict(RuntimeError):
    """The same market/trade identity has different immutable content."""


ExecutionIdentitySignature = tuple[int, int, int, bool, int]


@dataclass(frozen=True, slots=True)
class RecentExecutionIdentity:
    """One recently stored identity using ClickHouse-native exact values."""

    market_id: int
    trade_id: str
    timestamp_us: int
    price_atoms: int
    quantity_atoms: int
    is_taker_buy: bool
    instrument_spec_id: int

    def __post_init__(self) -> None:
        _validate_uint(self.market_id, 64, label="execution market ID", positive=True)
        if not isinstance(self.trade_id, str) or not self.trade_id:
            raise RuntimeError("recent execution trade ID must be a non-empty string")
        _validate_int64(
            self.timestamp_us,
            label="recent execution timestamp microseconds",
            positive=True,
        )
        _validate_int64(
            self.price_atoms,
            label="recent execution price atoms",
            positive=True,
        )
        _validate_uint(
            self.quantity_atoms,
            64,
            label="recent execution quantity atoms",
            positive=True,
        )
        if not isinstance(self.is_taker_buy, bool):
            raise RuntimeError("recent execution taker side must be a boolean")
        _validate_uint(
            self.instrument_spec_id,
            64,
            label="recent execution instrument specification ID",
            positive=True,
        )

    @property
    def signature(self) -> ExecutionIdentitySignature:
        return (
            self.timestamp_us,
            self.price_atoms,
            self.quantity_atoms,
            self.is_taker_buy,
            self.instrument_spec_id,
        )


_RAW_EXECUTION_INSERT_COLUMNS = (
    "market_id",
    "source_epoch",
    "source_sequence",
    "batch_index",
    "timestamp_us",
    "trade_id",
    "price_atoms",
    "quantity_atoms",
    "is_taker_buy",
    "instrument_spec_id",
)


class ClickHouseExecutionStore:
    """Individual-execution repository backed by the ClickHouse raw table."""

    def __init__(
        self,
        access: ClickHouseAccess,
        *,
        database: str,
        table: str = "raw_executions",
    ):
        self.access = access
        self.qualified_table = qualified_table(database, table)

    def insert_batch(self, executions: Sequence[StoredExecution]) -> None:
        seen: dict[tuple[int, str], StoredExecution] = {}
        for execution in executions:
            previous = seen.get(execution.identity)
            if previous is not None:
                if _execution_signature(previous) != _execution_signature(execution):
                    raise ExecutionIdentityConflict(
                        _identity_conflict_message(execution)
                    )
                continue
            seen[execution.identity] = execution

        rows = [self._insert_row(execution) for execution in seen.values()]
        if not rows:
            return
        self.access.insert(
            self.qualified_table,
            rows,
            column_names=_RAW_EXECUTION_INSERT_COLUMNS,
            settings=deduplicated_insert_settings(),
            operation="insert_raw_execution_batch",
        )

    def max_source_epoch(self, market_ids: Sequence[int]) -> int:
        identities = tuple(dict.fromkeys(market_ids))
        if not identities:
            return 0
        for market_id in identities:
            _validate_uint(
                market_id,
                64,
                label="execution market ID",
                positive=True,
            )
        result = self.access.query(
            f"""
            SELECT maxOrNull(source_epoch)
            FROM {self.qualified_table}
            WHERE market_id IN {{market_ids:Array(UInt64)}}
            """,
            parameters={"market_ids": list(identities)},
            operation="read_max_source_epoch",
        )
        rows = tuple(getattr(result, "result_rows", ()))
        if len(rows) != 1 or len(rows[0]) != 1:
            raise RuntimeError(
                "ClickHouse maximum source epoch query did not return exactly one value"
            )
        value = rows[0][0]
        if value is None:
            return 0
        if isinstance(value, bool):
            raise RuntimeError("ClickHouse maximum source epoch is not an integer")
        try:
            epoch = int(value)
        except (TypeError, ValueError) as exc:
            raise RuntimeError(
                "ClickHouse maximum source epoch is not an integer"
            ) from exc
        _validate_uint(epoch, 32, label="maximum execution source epoch")
        return epoch

    def missing_executions(
        self, executions: Sequence[StoredExecution]
    ) -> tuple[StoredExecution, ...]:
        """Resolve an ambiguous insert by identity, independent of block dedupe TTL."""
        found = {}
        for market_id in {e.market_id for e in executions}:
            members = [e for e in executions if e.market_id == market_id]
            result = self.access.query(
                f"""SELECT market_id,trade_id,timestamp_us,price_atoms,quantity_atoms,
                    is_taker_buy,instrument_spec_id FROM {self.qualified_table}
                    WHERE market_id={{market:UInt64}} AND trade_id IN {{ids:Array(String)}}
                    AND timestamp_us BETWEEN {{start:Int64}} AND {{end:Int64}}""",
                parameters={
                    "market": market_id,
                    "ids": [e.trade.trade_id for e in members],
                    "start": min(e.trade.exchange_timestamp_us for e in members),
                    "end": max(e.trade.exchange_timestamp_us for e in members),
                },
                operation="resolve_outbox_insert",
            )
            for row in result.result_rows:
                key, signature = (row[0], row[1]), tuple(row[2:])
                if key in found and found[key] != signature:
                    raise ExecutionIdentityConflict(
                        f"Conflicting stored identity: {key}"
                    )
                found[key] = signature
        missing = []
        for execution in executions:
            signature = (
                execution.trade.exchange_timestamp_us,
                execution.price_atoms,
                execution.quantity_atoms,
                execution.trade.taker_side == TakerSide.BUY,
                execution.instrument_spec_id,
            )
            if execution.identity not in found:
                missing.append(execution)
            elif found[execution.identity] != signature:
                raise ExecutionIdentityConflict(_identity_conflict_message(execution))
        return tuple(missing)

    def recent_execution_identities(
        self,
        market_ids: Sequence[int],
        *,
        per_market_limit: int,
    ) -> dict[int, tuple[RecentExecutionIdentity, ...]]:
        """Read each market's newest identities in reverse primary-key order."""

        identities = tuple(dict.fromkeys(market_ids))
        _validate_uint(
            per_market_limit,
            64,
            label="recent execution identity limit",
            positive=True,
        )
        recent: dict[int, tuple[RecentExecutionIdentity, ...]] = {}
        for market_id in identities:
            _validate_uint(
                market_id,
                64,
                label="execution market ID",
                positive=True,
            )
            result = self.access.query(
                f"""
                SELECT
                    trade_id,
                    timestamp_us,
                    price_atoms,
                    quantity_atoms,
                    is_taker_buy,
                    instrument_spec_id
                FROM {self.qualified_table}
                WHERE market_id = {{market_id:UInt64}}
                ORDER BY
                    source_epoch DESC,
                    source_sequence DESC,
                    batch_index DESC,
                    timestamp_us DESC,
                    trade_id DESC
                LIMIT {{recent_limit:UInt64}}
                """,
                parameters={
                    "market_id": market_id,
                    "recent_limit": per_market_limit,
                },
                operation="read_recent_execution_identities",
            )
            rows = tuple(getattr(result, "result_rows", ()))
            market_recent: list[RecentExecutionIdentity] = []
            for row in rows:
                if len(row) != 6:
                    raise RuntimeError(
                        "ClickHouse recent execution identity query returned "
                        "an unexpected row shape"
                    )
                (
                    trade_id,
                    timestamp_us,
                    price_atoms,
                    quantity_atoms,
                    is_taker_buy,
                    instrument_spec_id,
                ) = row
                market_recent.append(
                    RecentExecutionIdentity(
                        market_id=market_id,
                        trade_id=trade_id,
                        timestamp_us=timestamp_us,
                        price_atoms=price_atoms,
                        quantity_atoms=quantity_atoms,
                        is_taker_buy=is_taker_buy,
                        instrument_spec_id=instrument_spec_id,
                    )
                )
            recent[market_id] = tuple(market_recent)
        return recent

    def _insert_row(
        self,
        execution: StoredExecution,
    ) -> tuple[object, ...]:
        trade = execution.trade
        _validate_uint(
            execution.market_id, 64, label="execution market ID", positive=True
        )
        _validate_uint(
            execution.instrument_spec_id,
            64,
            label="execution instrument specification ID",
            positive=True,
        )
        _validate_uint(
            execution.source_epoch,
            32,
            label="execution source epoch",
            positive=True,
        )
        sequence = execution.source_sequence
        _validate_uint(sequence, 64, label="execution source sequence")
        _validate_uint(trade.batch_index, 32, label="execution batch index")
        _validate_int64(
            trade.exchange_timestamp_us,
            label="execution timestamp microseconds",
            positive=True,
        )
        _validate_int64(
            execution.price_atoms, label="execution price atoms", positive=True
        )
        _validate_uint(
            execution.quantity_atoms,
            64,
            label="execution quantity atoms",
            positive=True,
        )
        return (
            execution.market_id,
            execution.source_epoch,
            sequence,
            trade.batch_index,
            trade.exchange_timestamp_us,
            trade.trade_id,
            execution.price_atoms,
            execution.quantity_atoms,
            trade.taker_side == TakerSide.BUY,
            execution.instrument_spec_id,
        )


def _execution_signature(execution: StoredExecution) -> tuple[object, ...]:
    """Return every immutable field scoped by a market/trade identity."""

    return (
        execution.trade.market_key,
        execution.trade.trade_id,
        *execution.immutable_payload,
    )


def _identity_conflict_message(execution: StoredExecution) -> str:
    return (
        "conflicting immutable execution "
        f"{execution.trade.market_key}:{execution.trade.trade_id}"
    )


def _validate_uint(
    value: int,
    bits: int,
    *,
    label: str,
    positive: bool = False,
) -> None:
    minimum = 1 if positive else 0
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not minimum <= value <= 2**bits - 1
    ):
        qualifier = "positive " if positive else ""
        raise OverflowError(f"{label} must fit a {qualifier}uint{bits}")


def _validate_int64(value: int, *, label: str, positive: bool = False) -> None:
    minimum = 1 if positive else -(2**63)
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not minimum <= value <= 2**63 - 1
    ):
        qualifier = "positive " if positive else ""
        raise OverflowError(f"{label} must fit a {qualifier}int64")


def classify_clickhouse_error(
    error: BaseException,
    *,
    is_write: bool,
) -> ClickHouseFailureKind:
    """Classify failures without ever replaying a write automatically."""

    code = getattr(error, "code", None)
    try:
        numeric_code = int(code) if code is not None else None
    except (TypeError, ValueError):
        numeric_code = None
    network_failure = isinstance(
        error, (ConnectionError, TimeoutError, OSError)
    ) or any(cls.__name__ in _NETWORK_EXCEPTION_NAMES for cls in type(error).__mro__)
    if numeric_code in _NETWORK_ERROR_CODES or network_failure:
        return (
            ClickHouseFailureKind.AMBIGUOUS_WRITE
            if is_write
            else ClickHouseFailureKind.RETRYABLE
        )
    if numeric_code in _OVERLOAD_ERROR_CODES:
        return ClickHouseFailureKind.RETRYABLE
    return ClickHouseFailureKind.FATAL


def validate_identifier(value: str, *, label: str = "ClickHouse identifier") -> str:
    if not isinstance(value, str) or not _IDENTIFIER.fullmatch(value):
        raise ClickHouseConfigurationError(
            f"{label} must start with a letter or underscore and contain only letters, digits, and underscores"
        )
    return value


def validate_qualified_identifier(value: str) -> str:
    if not isinstance(value, str):
        raise ClickHouseConfigurationError("ClickHouse table must be a string")
    components = value.split(".")
    if len(components) not in {1, 2}:
        raise ClickHouseConfigurationError(
            "ClickHouse table must be a table or database.table"
        )
    return ".".join(validate_identifier(component) for component in components)


def quote_identifier(value: str) -> str:
    return f"`{validate_identifier(value)}`"


def qualified_table(database: str, table: str) -> str:
    return f"{validate_identifier(database)}.{validate_identifier(table)}"


def _environment_int(
    environment: Mapping[str, str],
    name: str,
    default: int,
    *,
    minimum: int,
    maximum: int,
) -> int:
    raw = environment.get(name)
    if raw is None or not raw.strip():
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise ClickHouseConfigurationError(f"{name} must be an integer") from exc
    if not minimum <= value <= maximum:
        raise ClickHouseConfigurationError(
            f"{name} must be between {minimum} and {maximum}"
        )
    return value


def _environment_bool(
    environment: Mapping[str, str],
    name: str,
    default: bool,
) -> bool:
    raw = environment.get(name)
    if raw is None or not raw.strip():
        return default
    normalized = raw.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ClickHouseConfigurationError(f"{name} must be a boolean")
