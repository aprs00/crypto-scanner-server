from django.core.management.base import BaseCommand
from exchange_connections.hyperliquid.klines import main as hyperliquid_klines


class Command(BaseCommand):
    help = "Start Hyperliquid Klines connection"

    def handle(self, *args, **kwargs):
        self.stdout.write("Starting Hyperliquid Klines connection...")
        try:
            hyperliquid_klines()
        except Exception as e:
            self.stderr.write(f"Error: {e}")
