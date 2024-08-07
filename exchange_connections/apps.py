import threading

from django.apps import AppConfig

from exchange_connections.binance.klines import main as start_binance_klines


class ExchangeConnectionsConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "exchange_connections"

    def ready(self):
        thread = threading.Thread(target=start_binance_klines, daemon=True)
        thread.start()
