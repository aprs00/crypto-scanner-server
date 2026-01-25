import time
from typing import Any, cast

import msgpack
import redis
from redis.exceptions import RedisError, ResponseError

from core.redis_config import get_redis_connection


STREAM_PREFIX = "stream:market"


def get_market_stream_key(exchange: str, contract_type: str) -> str:
    return f"{STREAM_PREFIX}:{exchange}:{contract_type}"


def ensure_consumer_group(
    redis_client: redis.Redis, stream_key: str, group_name: str
) -> bool:
    try:
        # Use "$" to start from latest message, not stream beginning
        redis_client.xgroup_create(stream_key, group_name, id="$", mkstream=True)
        return True
    except ResponseError as exc:
        if "BUSYGROUP" not in str(exc):
            raise
    return False


def publish_market_event(
    exchange: str,
    contract_type: str,
    event_type: str,
    payload: dict[str, Any],
    redis_client: redis.Redis | None = None,
) -> str:
    print("PUBLISHING EVENT:", event_type, "FOR", exchange, contract_type)
    redis_client = redis_client or get_redis_connection()
    stream_key = get_market_stream_key(exchange, contract_type)
    fields: dict[str, Any] = {
        "event_type": event_type,
        "timestamp_ms": str(int(time.time() * 1000)),
        "payload": msgpack.packb(payload, use_bin_type=True),
    }
    # Retry logic with exponential backoff (3 attempts)
    for attempt in range(3):
        try:
            return cast(
                str,
                redis_client.xadd(stream_key, fields, maxlen=10000, approximate=True),  # type: ignore[arg-type]
            )
        except RedisError:
            if attempt == 2:
                raise
            time.sleep(0.1 * (2**attempt))
    raise RedisError("Failed to publish after 3 attempts")


def decode_stream_fields(fields: dict) -> dict:
    decoded = {}
    for key, value in fields.items():
        key_str = key.decode("utf-8") if isinstance(key, (bytes, bytearray)) else key
        if key_str == "payload" and value is not None:
            decoded[key_str] = msgpack.unpackb(value, raw=False, strict_map_key=False)
        else:
            decoded[key_str] = (
                value.decode("utf-8")
                if isinstance(value, (bytes, bytearray))
                else value
            )
    return decoded


# Idempotency tracking for preventing duplicate processing
IDEMPOTENCY_TTL_SECONDS = 300  # 5 minutes TTL for processed timestamps


def get_idempotency_key(service: str, exchange: str, contract_type: str) -> str:
    """Get the Redis sorted set key for idempotency tracking."""
    return f"processed_timestamps:{service}:{exchange}:{contract_type}"


def mark_timestamp_processed(
    redis_client: redis.Redis,
    service: str,
    exchange: str,
    contract_type: str,
    timestamp_ms: int,
) -> None:
    """
    Mark a timestamp as processed using a Redis sorted set.
    The score is the current time for TTL cleanup.
    """
    key = get_idempotency_key(service, exchange, contract_type)
    current_time = time.time()

    pipe = redis_client.pipeline()
    # Add the timestamp with current time as score
    pipe.zadd(key, {str(timestamp_ms): current_time})
    # Remove entries older than TTL
    pipe.zremrangebyscore(key, "-inf", current_time - IDEMPOTENCY_TTL_SECONDS)
    # Set key expiry as a safety net
    pipe.expire(key, IDEMPOTENCY_TTL_SECONDS * 2)
    pipe.execute()


def is_timestamp_processed(
    redis_client: redis.Redis,
    service: str,
    exchange: str,
    contract_type: str,
    timestamp_ms: int,
) -> bool:
    """
    Check if a timestamp has already been processed.
    Returns True if already processed, False otherwise.
    """
    key = get_idempotency_key(service, exchange, contract_type)
    return redis_client.zscore(key, str(timestamp_ms)) is not None


def get_stream_last_id(redis_client: redis.Redis, stream_key: str) -> str:
    """
    Get the last message ID in a stream, or "0" if empty/non-existent.

    Use this to capture the stream position before long-running initialization,
    then resume from that position to avoid losing messages published during init.
    """
    try:
        info = redis_client.xinfo_stream(stream_key)
        last_id = info.get("last-generated-id") or info.get(b"last-generated-id")  # type: ignore[misc]
        if last_id:
            return (
                last_id.decode("utf-8") if isinstance(last_id, bytes) else str(last_id)
            )
    except ResponseError as exc:
        if "no such key" in str(exc).lower():
            return "0"
        raise
    return "0"
