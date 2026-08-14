from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

from core.redis_config import get_redis_connection


MAINTENANCE_WAKEUP_QUEUE = "footprints:maintenance:wakeups"
MAINTENANCE_WAKEUP_MAX_LENGTH = 1_024
_BUILD_PAYLOAD = "build"
_EXECUTION_PREFIX = "executions:"


@dataclass(frozen=True, slots=True)
class MaintenanceWakeup:
    market_ids: frozenset[int] = frozenset()
    build_requested: bool = False


def publish_execution_wakeup(market_ids: Iterable[int], *, redis_client=None) -> None:
    identities = _market_ids(market_ids)
    if not identities:
        return
    _publish(
        _EXECUTION_PREFIX + ",".join(str(market_id) for market_id in identities),
        redis_client=redis_client,
    )


def publish_build_wakeup(*, redis_client=None) -> None:
    _publish(_BUILD_PAYLOAD, redis_client=redis_client)


def wait_for_wakeups(redis_client, *, timeout_seconds: int = 1) -> MaintenanceWakeup | None:
    if (
        isinstance(timeout_seconds, bool)
        or not isinstance(timeout_seconds, int)
        or timeout_seconds <= 0
    ):
        raise ValueError("wakeup timeout must be a positive integer")

    first = redis_client.brpop(
        MAINTENANCE_WAKEUP_QUEUE,
        timeout=timeout_seconds,
    )
    if first is None:
        return None

    payloads = [first[1]]
    remaining = redis_client.rpop(
        MAINTENANCE_WAKEUP_QUEUE,
        count=MAINTENANCE_WAKEUP_MAX_LENGTH - 1,
    )
    if remaining:
        payloads.extend(remaining)

    market_ids: set[int] = set()
    build_requested = False
    for raw_payload in payloads:
        payload = _text(raw_payload)
        if payload == _BUILD_PAYLOAD:
            build_requested = True
            continue
        if not payload.startswith(_EXECUTION_PREFIX):
            continue
        for raw_market_id in payload.removeprefix(_EXECUTION_PREFIX).split(","):
            try:
                market_id = int(raw_market_id)
            except (TypeError, ValueError):
                continue
            if market_id > 0:
                market_ids.add(market_id)
    return MaintenanceWakeup(
        market_ids=frozenset(market_ids),
        build_requested=build_requested,
    )


def _publish(payload: str, *, redis_client=None) -> None:
    owned_client = redis_client is None
    client = get_redis_connection() if owned_client else redis_client
    try:
        pipeline = client.pipeline()
        pipeline.lpush(MAINTENANCE_WAKEUP_QUEUE, payload)
        pipeline.ltrim(
            MAINTENANCE_WAKEUP_QUEUE,
            0,
            MAINTENANCE_WAKEUP_MAX_LENGTH - 1,
        )
        pipeline.execute()
    finally:
        if owned_client:
            client.close()


def _market_ids(values: Iterable[int]) -> tuple[int, ...]:
    result: set[int] = set()
    for value in values:
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise ValueError("wakeup market IDs must be positive integers")
        result.add(value)
    return tuple(sorted(result))


def _text(value: object) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)
