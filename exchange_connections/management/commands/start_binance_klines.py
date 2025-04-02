from django.core.management.base import BaseCommand
from exchange_connections.binance.klines import main as start_binance_klines


class Command(BaseCommand):
    help = "Start Binance Klines connection"

    def handle(self, *args, **kwargs):
        self.stdout.write("Starting Binance Klines connection...")
        try:
            start_binance_klines()
        except Exception as e:
            self.stderr.write(f"Error: {e}")
