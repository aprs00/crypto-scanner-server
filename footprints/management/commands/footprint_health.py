import json
import os
import time
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError

from footprints.models import RecoveryStatus, TradeRecoveryDay


class Command(BaseCommand):
    help = "JSON capture, delivery and verification health; backlog alone does not fail the liveness check"

    def add_arguments(self, parser):
        parser.add_argument("--exchange", choices=("binance", "bybit"), required=True)
        parser.add_argument(
            "--capture-only",
            action="store_true",
            help="Local Docker liveness check; no DB dependency",
        )

    def handle(self, *args, **options):
        directory = Path(os.environ.get("FOOTPRINT_STATE_DIR", "/tmp/footprint-state"))
        try:
            state = json.loads((directory / "health.json").read_text())
        except (OSError, ValueError) as exc:
            raise CommandError("capture heartbeat unavailable") from exc
        now = time.time()
        state["capture_healthy"] = bool(
            state.get("capture")
            and now - state["at"] < 30
            and not state.get("fatal")
            and (
                (state.get("initialized") and now - state.get("last_trade_at", 0) < 180)
                or (
                    not state.get("initialized")
                    and (directory / "feeds-ready").exists()
                )
            )
        )
        oldest = state.get("oldest_pending_at")
        state["delivery_lag_seconds"] = max(0, now - oldest) if oldest else 0
        state["delivery_delayed"] = state["delivery_lag_seconds"] > 30
        if not options["capture_only"]:
            try:
                state["unresolved_days"] = list(
                    TradeRecoveryDay.objects.filter(
                        market__exchange=options["exchange"]
                    )
                    .exclude(status=RecoveryStatus.VERIFIED)
                    .values("day", "status", "error", "report")
                )
            except Exception as exc:
                state["verification_error"] = str(exc)
        self.stdout.write(json.dumps(state, default=str))
        if not state["capture_healthy"]:
            raise CommandError("capture is stale, stopped, or has a fatal error")
