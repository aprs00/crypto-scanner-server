import time
from django.core.management.base import BaseCommand
from zscore.services.incremental_zscore import ZScoreProcessor
from core.constants import ACTIVE_EXCHANGES, Exchange


class Command(BaseCommand):
    help = "Runs the incremental zscore calculations"

    def add_arguments(self, parser):
        parser.add_argument(
            "--exchange",
            type=str,
            required=True,
            help="Exchange to calculate zscores for (e.g. binance, bybit, hyperliquid, okx)",
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

        if Exchange(exchange) not in ACTIVE_EXCHANGES:
            self.stdout.write(f"[{exchange}] Exchange is disabled, skipping zscore.")
            while True:
                time.sleep(3600)

        self.stdout.write(
            self.style.SUCCESS(
                f"Starting zscore calculations for {exchange} ({contract_type})..."
            )
        )
        processor = ZScoreProcessor(exchange=exchange, contract_type=contract_type)
        processor.run()
