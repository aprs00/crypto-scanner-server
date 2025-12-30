from django.core.management.base import BaseCommand
from correlations.services.incremental_correlations import (
    CorrelationCalculator,
)


class Command(BaseCommand):
    help = "Runs the incremental correlations calculations"

    def add_arguments(self, parser):
        parser.add_argument(
            "--exchange",
            type=str,
            required=True,
            help="Exchange to calculate correlations for (e.g. binance, hyperliquid)",
        )
        parser.add_argument(
            "--contract-type",
            type=str,
            default="perpetual",
            help="Contract type (default: perpetual)",
        )

    def handle(self, *args, **options):
        exchange = options["exchange"]
        contract_type = options["contract_type"]
        self.stdout.write(
            self.style.SUCCESS(
                f"Starting correlations calculations for {exchange} ({contract_type})..."
            )
        )
        calculator = CorrelationCalculator(
            exchange=exchange, contract_type=contract_type
        )
        calculator.run()
