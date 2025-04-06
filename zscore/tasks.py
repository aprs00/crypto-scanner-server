import redis

from crypto_scanner.constants import (
    stats_select_options_all,
)
from zscore.utils import calculate_z_score_matrix, calculate_z_score_history


r = redis.Redis(host="redis")


def calculate_options_matrix():
    for duration in stats_select_options_all:
        r.execute_command(
            "SET", f"z_score_{duration}", calculate_z_score_matrix(duration)
        )


def calculate_history(duration="12h"):
    r.execute_command(
        "SET", f"z_score_history_{duration}", calculate_z_score_history(duration)
    )


def subscribe_to_klines_updates():
    """
    Subscribe to 'klines_fetched' Redis channel and execute z-score calculations
    when new data is available.
    """

    pubsub = r.pubsub()
    pubsub.subscribe("klines_fetched")

    for message in pubsub.listen():
        if message["type"] == "message":
            calculate_options_matrix()
            calculate_history(duration="12h")
