from django.core.management.base import BaseCommand

from core.constants import Exchange

EXCHANGE_KLINES_MAP = {
    Exchange.BINANCE: "exchange_connections.binance.klines",
    Exchange.HYPERLIQUID: "exchange_connections.hyperliquid.klines",
    Exchange.BYBIT: "exchange_connections.bybit.klines",
}


class Command(BaseCommand):
    help = "Start klines websocket connection for a specific exchange"

    def add_arguments(self, parser):
        parser.add_argument(
            "--exchange",
            type=str,
            required=True,
            choices=EXCHANGE_KLINES_MAP.keys(),
            help="Exchange to connect to (binance, hyperliquid, bybit)",
        )

    def handle(self, *args, **options):
        exchange = options["exchange"]

        self.stdout.write(f"Starting {exchange.capitalize()} klines connection...")

        try:
            module = __import__(EXCHANGE_KLINES_MAP[exchange], fromlist=["main"])
            module.main()
        except Exception as e:
            self.stderr.write(f"Error: {e}")
