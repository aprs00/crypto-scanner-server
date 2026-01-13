"""
Redis Streams helper utilities for reliable message delivery.

Provides publishing and consuming utilities with automatic retries,
consumer group management, and message acknowledgment.
"""

import json
import os
import time
from typing import Any, Callable, Dict, List, Optional, Tuple, cast

import redis


class StreamPublisher:
    """Publishes messages to Redis Streams with automatic retries."""

    def __init__(self, redis_client: redis.Redis, max_retries: int = 3):
        self.redis = redis_client
        self.max_retries = max_retries

    def publish(
        self,
        stream_key: str,
        data: Dict[str, Any],
        maxlen: int = 10000,
        approximate: bool = True,
    ) -> Optional[str]:
        """
        Publish a message to a Redis Stream.

        Args:
            stream_key: Redis stream key
            data: Message data (will be JSON-encoded where needed)
            maxlen: Maximum stream length (old messages auto-deleted)
            approximate: Use approximate trimming for better performance

        Returns:
            Message ID if successful, None if all retries failed
        """
        # Encode complex values as JSON strings
        encoded_data = {}
        for key, value in data.items():
            if isinstance(value, (dict, list)):
                encoded_data[key] = json.dumps(value)
            else:
                encoded_data[key] = str(value)

        for attempt in range(self.max_retries):
            try:
                message_id = self.redis.xadd(
                    stream_key, encoded_data, maxlen=maxlen, approximate=approximate
                )
                if isinstance(message_id, bytes):
                    return message_id.decode("utf-8")
                elif isinstance(message_id, str):
                    return message_id
                else:
                    return str(message_id) if message_id is not None else None
            except (redis.ConnectionError, redis.TimeoutError) as e:
                if attempt == self.max_retries - 1:
                    print(f"[StreamPublisher] Failed to publish after {self.max_retries} attempts: {e}")
                    return None
                wait_time = 2 ** attempt
                print(f"[StreamPublisher] Publish failed (attempt {attempt + 1}/{self.max_retries}), retrying in {wait_time}s...")
                time.sleep(wait_time)
            except Exception as e:
                print(f"[StreamPublisher] Unexpected error publishing to {stream_key}: {e}")
                return None

        return None


class StreamConsumer:
    """
    Consumes messages from Redis Streams with consumer groups.

    Provides automatic:
    - Consumer group creation
    - Message acknowledgment
    - Pending message reclaim
    - Connection retry with exponential backoff
    """

    def __init__(
        self,
        redis_client: redis.Redis,
        stream_key: str,
        group_name: str,
        consumer_name: Optional[str] = None,
    ):
        self.redis = redis_client
        self.stream_key = stream_key
        self.group_name = group_name
        self.consumer_name = consumer_name or f"worker-{os.getpid()}"
        self.running = False

    def create_consumer_group(self, start_id: str = "0") -> bool:
        """
        Create consumer group if it doesn't exist.

        Args:
            start_id: Starting message ID ('0' for beginning, '$' for latest)

        Returns:
            True if created or already exists, False on error
        """
        try:
            self.redis.xgroup_create(self.stream_key, self.group_name, id=start_id, mkstream=True)
            print(f"[StreamConsumer] Created consumer group: {self.group_name} on {self.stream_key}")
            return True
        except redis.ResponseError as e:
            if "BUSYGROUP" in str(e):
                print(f"[StreamConsumer] Consumer group already exists: {self.group_name}")
                return True
            print(f"[StreamConsumer] Error creating consumer group: {e}")
            return False
        except Exception as e:
            print(f"[StreamConsumer] Unexpected error creating consumer group: {e}")
            return False

    def read_messages(
        self,
        count: int = 10,
        block_ms: int = 5000,
    ) -> List[Tuple[str, Dict[str, Any]]]:
        """
        Read new messages from the stream.

        Args:
            count: Maximum number of messages to read
            block_ms: Block for this many milliseconds waiting for messages

        Returns:
            List of (message_id, decoded_data) tuples
        """
        try:
            messages = cast(
                List[Tuple[Any, List[Tuple[Any, Dict[bytes, bytes]]]]],
                self.redis.xreadgroup(
                groupname=self.group_name,
                consumername=self.consumer_name,
                streams={self.stream_key: ">"},
                count=count,
                block=block_ms,
                ),
            )

            result = []
            for stream, msg_list in messages:
                for msg_id, msg_data in msg_list:
                    decoded_data = self._decode_message(msg_data)
                    msg_id_str = msg_id.decode("utf-8") if isinstance(msg_id, bytes) else msg_id
                    result.append((msg_id_str, decoded_data))

            return result
        except (redis.ConnectionError, redis.TimeoutError) as e:
            print(f"[StreamConsumer] Error reading messages: {e}")
            return []
        except Exception as e:
            print(f"[StreamConsumer] Unexpected error reading: {e}")
            return []

    def reclaim_pending(
        self,
        min_idle_ms: int = 60000,
        count: int = 10,
    ) -> List[Tuple[str, Dict[str, Any]]]:
        """
        Reclaim pending messages that have been idle too long.

        Args:
            min_idle_ms: Minimum idle time in milliseconds to reclaim
            count: Maximum number of messages to reclaim

        Returns:
            List of (message_id, decoded_data) tuples
        """
        try:
            # Get pending message info
            pending = cast(
                List[Dict[str, Any]],
                self.redis.xpending_range(
                    self.stream_key, self.group_name, min="-", max="+", count=count
                ),
            )

            if not pending:
                return []

            # Find messages idle longer than threshold
            to_reclaim = [
                msg["message_id"]
                for msg in pending
                if msg["time_since_delivered"] > min_idle_ms
            ]

            if not to_reclaim:
                return []

            # Claim the messages
            claimed = cast(
                List[Tuple[Any, Dict[bytes, bytes]]],
                self.redis.xclaim(
                    self.stream_key,
                    self.group_name,
                    self.consumer_name,
                    min_idle_time=min_idle_ms,
                    message_ids=to_reclaim,
                ),
            )

            result = []
            for msg_id, msg_data in claimed:
                decoded_data = self._decode_message(msg_data)
                msg_id_str = msg_id.decode("utf-8") if isinstance(msg_id, bytes) else msg_id
                result.append((msg_id_str, decoded_data))

            if result:
                print(f"[StreamConsumer] Reclaimed {len(result)} pending messages")

            return result

        except Exception as e:
            print(f"[StreamConsumer] Error reclaiming pending messages: {e}")
            return []

    def ack(self, *message_ids: str) -> int:
        """
        Acknowledge message(s) as processed.

        Args:
            message_ids: One or more message IDs to acknowledge

        Returns:
            Number of messages acknowledged
        """
        try:
            result = self.redis.xack(self.stream_key, self.group_name, *message_ids)
            return int(cast(Any, result))
        except Exception as e:
            print(f"[StreamConsumer] Error acknowledging messages: {e}")
            return 0

    def reset_position(self, message_id: str = "$") -> bool:
        """
        Reset consumer group position to a specific message ID.

        Args:
            message_id: Message ID to reset to ('$' for latest)

        Returns:
            True if successful, False otherwise
        """
        try:
            self.redis.xgroup_setid(self.stream_key, self.group_name, message_id)
            print(f"[StreamConsumer] Reset consumer group position to {message_id}")
            return True
        except Exception as e:
            print(f"[StreamConsumer] Error resetting position: {e}")
            return False

    def start_consuming(
        self,
        message_handler: Callable[[str, Dict[str, Any]], bool],
        count: int = 10,
        block_ms: int = 5000,
        reclaim_idle_ms: int = 60000,
        max_retries_per_message: int = 3,
    ):
        """
        Start consuming messages in a loop.

        Args:
            message_handler: Function that processes messages.
                           Should return True if processed successfully, False to retry.
            count: Number of messages to read per batch
            block_ms: Block time waiting for new messages
            reclaim_idle_ms: Reclaim messages idle longer than this
            max_retries_per_message: Max retries before giving up on a message
        """
        self.running = True
        retry_counts: Dict[str, int] = {}
        connection_retry = 0

        print(f"[StreamConsumer] Starting consumer loop for {self.stream_key}")

        while self.running:
            try:
                # Read new messages
                messages = self.read_messages(count=count, block_ms=block_ms)

                # If no new messages, check for pending
                if not messages:
                    messages = self.reclaim_pending(min_idle_ms=reclaim_idle_ms, count=count)

                # Process messages
                for msg_id, msg_data in messages:
                    try:
                        success = message_handler(msg_id, msg_data)
                    except Exception as e:
                        print(f"[StreamConsumer] Error processing message {msg_id}: {e}")
                        success = False

                    if success:
                        self.ack(msg_id)
                        retry_counts.pop(msg_id, None)
                        connection_retry = 0
                    else:
                        retry_counts[msg_id] = retry_counts.get(msg_id, 0) + 1
                        if retry_counts[msg_id] >= max_retries_per_message:
                            print(f"[StreamConsumer] Max retries reached for {msg_id}, ACKing to skip")
                            self.ack(msg_id)
                            retry_counts.pop(msg_id, None)

            except (redis.ConnectionError, redis.TimeoutError) as e:
                connection_retry += 1
                wait_time = min(2 ** connection_retry, 60)
                print(f"[StreamConsumer] Connection error: {e}, retrying in {wait_time}s...")
                time.sleep(wait_time)
            except Exception as e:
                print(f"[StreamConsumer] Unexpected error in consumer loop: {e}")
                time.sleep(5)

        print(f"[StreamConsumer] Consumer loop stopped for {self.stream_key}")

    def stop(self):
        """Stop the consumer loop."""
        self.running = False

    def _decode_message(self, msg_data: Dict[bytes, bytes]) -> Dict[str, Any]:
        """
        Decode raw Redis message data.

        Args:
            msg_data: Raw message data from Redis (bytes keys/values)

        Returns:
            Decoded message with string keys and parsed JSON values
        """
        decoded = {}
        for key, value in msg_data.items():
            key_str = key.decode("utf-8") if isinstance(key, bytes) else key
            value_str = value.decode("utf-8") if isinstance(value, bytes) else value

            # Try to parse as JSON
            try:
                decoded[key_str] = json.loads(value_str)
            except (json.JSONDecodeError, ValueError):
                # Not JSON, keep as string
                decoded[key_str] = value_str

        return decoded
