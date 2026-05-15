import time
from django.core.management.base import BaseCommand

from core.constants import ACTIVE_EXCHANGES, Exchange

EXCHANGE_KLINES_MAP = {
    Exchange.BINANCE: "exchange_connections.binance.klines",
    Exchange.HYPERLIQUID: "exchange_connections.hyperliquid.klines",
    Exchange.BYBIT: "exchange_connections.bybit.klines",
    Exchange.OKX: "exchange_connections.okx.klines",
}


class Command(BaseCommand):
    help = "Start klines websocket connection for a specific exchange"

    def add_arguments(self, parser):
        parser.add_argument(
            "--exchange",
            type=str,
            required=True,
            choices=EXCHANGE_KLINES_MAP.keys(),
            help="Exchange to connect to (binance, hyperliquid, bybit, okx)",
        )

    def handle(self, *args, **options):
        exchange = options["exchange"]

        if Exchange(exchange) not in ACTIVE_EXCHANGES:
            self.stdout.write(f"[{exchange}] Exchange is disabled, skipping klines.")
            while True:
                time.sleep(3600)

        self.stdout.write(f"Starting {exchange.capitalize()} klines connection...")

        try:
            module = __import__(EXCHANGE_KLINES_MAP[exchange], fromlist=["main"])
            module.main()
        except Exception as e:
            self.stderr.write(f"Error: {e}")
