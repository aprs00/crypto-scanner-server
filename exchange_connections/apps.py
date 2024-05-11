from django.apps import AppConfig

from exchange_connections.binance.klines import main as start_binance_klines

_is_ready_called = False


class ExchangeConnectionsConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "exchange_connections"

    def ready(self):
        global _is_ready_called

        if not _is_ready_called:
            # start_binance_klines()
            print("fewpoihfewpoih")
