import time
import redis

from django.apps import AppConfig

from exchange_connections.binance.klines import main as start_binance_klines

r = redis.Redis(host="redis", port=6379, decode_responses=True)


class ExchangeConnectionsConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "exchange_connections"

    def ready(self):
        if r.set("my_lock", "True", nx=True):
            start_binance_klines()

        # self.toggle_lock_after_delay()

    @staticmethod
    def toggle_lock_after_delay():
        time.sleep(20)  # 20 minutes
        r.set("my_lock", "False")
