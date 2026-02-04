from django.core.management.base import BaseCommand

from cointegration.services.scanner import CointegrationScanner
from core.constants import Exchange


class Command(BaseCommand):
    help = "Runs cointegration calculations for a specific exchange"

    def add_arguments(self, parser):
        parser.add_argument(
            "--exchange",
            type=str,
            required=True,
            help="Exchange to calculate cointegration for (binance, bybit, hyperliquid)",
        )
        parser.add_argument(
            "--contract-type",
            type=str,
            default="perpetual",
            help="Contract type (default: perpetual)",
        )
        parser.add_argument(
            "--window-minutes",
            type=int,
            default=1440,
            help="Window length in minutes (default: 1440)",
        )
        parser.add_argument(
            "--cadence-minutes",
            type=int,
            default=15,
            help="Cadence in minutes (default: 15)",
        )
        parser.add_argument(
            "--batch-size",
            type=int,
            default=5000,
            help="Pair batch size (default: 5000)",
        )

    def handle(self, *args, **options):
        exchange = Exchange(options["exchange"]).value
        contract_type = options["contract_type"]
        window_minutes = options["window_minutes"]
        cadence_minutes = options["cadence_minutes"]
        batch_size = options["batch_size"]

        self.stdout.write(
            self.style.SUCCESS(
                f"Starting cointegration scan for {exchange} ({contract_type})..."
            )
        )

        scanner = CointegrationScanner(
            exchange=exchange,
            contract_type=contract_type,
            window_minutes=window_minutes,
            cadence_minutes=cadence_minutes,
            batch_size=batch_size,
        )
        scanner.run()
