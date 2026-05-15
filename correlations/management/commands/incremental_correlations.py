import time
from django.core.management.base import BaseCommand
from correlations.services.incremental_correlations import (
    CorrelationCalculator,
)
from core.constants import ACTIVE_EXCHANGES, Exchange


class Command(BaseCommand):
    help = "Runs the incremental correlations calculations"

    def add_arguments(self, parser):
        parser.add_argument(
            "--exchange",
            type=str,
            required=True,
            help="Exchange to calculate correlations for (e.g. binance, bybit, hyperliquid, okx)",
        )
        parser.add_argument(
            "--contract-type",
            type=str,
            default="perpetual",
            help="Contract type (default: perpetual)",
        )

    def handle(self, *args, **options):
        exchange = Exchange(options["exchange"])
        contract_type = options["contract_type"]

        if exchange not in ACTIVE_EXCHANGES:
            self.stdout.write(f"[{exchange}] Exchange is disabled, skipping correlations.")
            while True:
                time.sleep(3600)

        self.stdout.write(
            self.style.SUCCESS(
                f"Starting correlations calculations for {exchange} ({contract_type})..."
            )
        )
        calculator = CorrelationCalculator(
            exchange=exchange, contract_type=contract_type
        )
        calculator.run()
