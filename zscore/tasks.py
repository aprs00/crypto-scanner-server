import redis

from zscore.utils import calculate_z_score_history
from core.constants import RedisPubMessages


r = redis.Redis(host="redis")


def calculate_history(duration):
    r.execute_command(
        "SET", f"z_score_history_{duration}", calculate_z_score_history(duration)
    )


def subscribe_to_klines_updates():
    """
    Subscribe to 'klines_fetched' Redis channel and execute z-score calculations
    when new data is available.
    """

    pubsub = r.pubsub()
    pubsub.subscribe(RedisPubMessages.KLINE_SAVED_TO_DB.value)

    # for message in pubsub.listen():
    #     if message["type"] == "message":
    #         calculate_history(duration="4h")
