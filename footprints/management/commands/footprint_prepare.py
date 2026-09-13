"""Prepare shared TREV input before starting an upgraded chart worker."""

import threading

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone

from footprints.chart_service import open_chart_stores
from footprints.management.commands.footprint_maintenance import (
    _install_signal_handlers,
    _restore_signal_handlers,
)
from footprints.models import FootprintMarket
from footprints.storage.prepared import prepare_market


class Command(BaseCommand):
    help = "Prepare the shared TREV input cache once for all enabled markets"

    def add_arguments(self, parser):
        parser.add_argument("--market-id", type=int)

    def handle(self, *args, **options):
        if not settings.TREV_PREPARED_DIR:
            raise CommandError("TREV_PREPARED_DIR must name the shared cache directory")
        stop = threading.Event()
        handlers = _install_signal_handlers(stop)
        stores = None
        try:
            stores = open_chart_stores(
                role="trev-input-preparation", operation_timeout_seconds=600
            )
            markets = FootprintMarket.objects.filter(enabled=True)
            if options["market_id"] is not None:
                markets = markets.filter(pk=options["market_id"])
            for market_id in markets.values_list("pk", flat=True):
                if stop.is_set():
                    break
                prepare_market(
                    stores.executions,
                    market_id=market_id,
                    directory=settings.TREV_PREPARED_DIR,
                    now_us=int(timezone.now().timestamp() * 1_000_000),
                    should_stop=stop.is_set,
                )
                if stop.is_set():
                    raise CommandError("TREV input preparation interrupted")
                self.stdout.write(f"Checked prepared TREV input for market {market_id}")
        finally:
            if stores is not None:
                stores.close()
            _restore_signal_handlers(handlers)
