import redis


def get_redis_connection() -> redis.Redis:
    return redis.Redis(
        host="redis",
        socket_keepalive=True,
        socket_keepalive_options={},
        socket_connect_timeout=5,
        socket_timeout=5,
        retry_on_timeout=True,
        health_check_interval=30,
    )
