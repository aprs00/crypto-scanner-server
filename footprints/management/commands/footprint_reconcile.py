"""One low-frequency, credential-free recovery worker. Repairs are opt-in."""

import json
import logging
import threading
from datetime import date, datetime, timedelta, timezone

from django.core.management.base import BaseCommand, CommandError
from django.db import close_old_connections

from footprints.management.commands.footprint_maintenance import (
    _acquire_singleton,
    _install_signal_handlers,
    _restore_signal_handlers,
    _verify_singleton,
)
from footprints.models import FootprintMarket, RecoveryStatus, TradeRecoveryDay
from footprints.recovery import EXCHANGES, MINUTE_US, Recovery
from footprints.storage.clickhouse import (
    ClickHouseAccess,
    ClickHouseSettings,
    create_clickhouse_client,
)

logger = logging.getLogger(__name__)
RECOVERY_LOCK_ID = 0x435346505245434F


class Command(BaseCommand):
    help = "Audit Binance/Bybit trades against public candles and archives; --repair explicitly publishes verified history"

    def add_arguments(self, parser):
        parser.add_argument("--exchange", choices=(*EXCHANGES, "both"), default="both")
        parser.add_argument(
            "--day", type=date.fromisoformat, help="UTC archive date (YYYY-MM-DD)"
        )
        parser.add_argument(
            "--loop",
            action="store_true",
            help="Poll every five minutes and audit one archive per exchange per pass",
        )
        parser.add_argument(
            "--repair",
            action="store_true",
            help="Allow verified archive publication and chart replacement",
        )
        parser.add_argument(
            "--refresh", action="store_true", help="Recheck an already verified --day"
        )
        parser.add_argument("--bootstrap-days", type=int, default=30)

    def handle(self, *args, **options):
        if options["day"] and options["loop"]:
            raise CommandError("--day and --loop are mutually exclusive")
        if options["refresh"] and not options["day"]:
            raise CommandError("--refresh requires --day")
        if not 1 <= options["bootstrap_days"] <= 30:
            raise CommandError("--bootstrap-days must be between 1 and 30")
        stop = threading.Event()
        handlers = _install_signal_handlers(stop)
        lease = access = None
        try:
            # Covers audits too: never race two large downloads or publishers.
            lease = _acquire_singleton(RECOVERY_LOCK_ID)
            configuration = ClickHouseSettings.from_environment()
            access = ClickHouseAccess(
                create_clickhouse_client(
                    configuration, role="trade-recovery", for_inserts=True
                )
            )
            recovery = Recovery(
                access,
                configuration.database,
                verify_writer=lambda: _verify_singleton(lease),
            )
            exchanges = (
                EXCHANGES if options["exchange"] == "both" else (options["exchange"],)
            )
            while not stop.is_set():
                _verify_singleton(lease)
                close_old_connections()
                markets = list(
                    FootprintMarket.objects.filter(
                        exchange__in=exchanges, enabled=True
                    ).order_by("exchange", "pk")
                )
                archived_exchanges = set()
                for market in markets:
                    if stop.is_set():
                        break
                    try:
                        if options["day"]:
                            report = recovery.repair_day(
                                market,
                                options["day"],
                                repair=options["repair"],
                                refresh=options["refresh"],
                            )
                            self.stdout.write(json.dumps(report))
                            continue
                        now = datetime.now(timezone.utc)
                        first, _ = recovery.bounds(market)
                        if first is None:
                            continue
                        end_us = (int(now.timestamp()) // 60 - 2) * MINUTE_US
                        start_us = max(
                            end_us - 10 * MINUTE_US,
                            ((first + MINUTE_US - 1) // MINUTE_US) * MINUTE_US,
                        )
                        differences = (
                            recovery.audit_minutes(market, start_us, end_us)
                            if start_us < end_us
                            else []
                        )
                        self.stdout.write(
                            json.dumps(
                                {
                                    "exchange": market.exchange,
                                    "symbol": market.symbol,
                                    "start_us": start_us,
                                    "end_us": end_us,
                                    "minute_mismatches": differences,
                                }
                            )
                        )
                        if options["loop"]:
                            self._record_minutes(market, now, differences)
                        if (
                            options["loop"]
                            and now.hour >= 2
                            and market.exchange not in archived_exchanges
                        ):
                            day = self._next_day(
                                market, first, now, options["bootstrap_days"]
                            )
                            if day:
                                archived_exchanges.add(market.exchange)
                                report = recovery.repair_day(
                                    market,
                                    day,
                                    repair=options["repair"],
                                    record_audit=True,
                                )
                                self.stdout.write(json.dumps(report))
                    except Exception as exc:
                        if not options["loop"]:
                            raise CommandError(str(exc)) from exc
                        logger.exception(
                            "Trade reconciliation failed for %s; capture is unaffected",
                            market.market_key,
                        )
                if not options["loop"] or stop.wait(300):
                    break
        finally:
            if access is not None:
                access.close()
            if lease is not None:
                lease.close()
            _restore_signal_handlers(handlers)

    @staticmethod
    def _next_day(market, first, now, days):
        first_day = max(
            datetime.fromtimestamp(first // 1_000_000, timezone.utc).date(),
            now.date() - timedelta(days=days),
        )
        records = {
            row.day: row
            for row in TradeRecoveryDay.objects.filter(
                market=market, day__gte=first_day
            )
        }
        candidates = []
        for offset in range((now.date() - first_day).days):
            day = first_day + timedelta(days=offset)
            record = records.get(day)
            if record and (
                record.status == RecoveryStatus.VERIFIED
                or record.retry_at
                and record.retry_at > now
            ):
                continue
            # Never-audited days first, then the least recently audited day.
            candidates.append(
                (
                    record.checked_at
                    if record and record.checked_at
                    else datetime.min.replace(tzinfo=timezone.utc),
                    day,
                )
            )
        return min(candidates)[1] if candidates else None

    @staticmethod
    def _record_minutes(market, now, differences):
        days = {
            now.date(),
            *(
                datetime.fromtimestamp(
                    row["minute_us"] // 1_000_000, timezone.utc
                ).date()
                for row in differences
            ),
        }
        for day in days:
            day_us = (
                int(
                    datetime.combine(day, datetime.min.time(), timezone.utc).timestamp()
                )
                * 1_000_000
            )
            record, _ = TradeRecoveryDay.objects.get_or_create(
                market=market, day=day, defaults={"start_us": day_us}
            )
            report = dict(record.report)
            report.update(last_minute_audit=now.isoformat())
            # Keep evidence until the archive verifies the whole day.
            existing = {
                row["minute_us"]: row for row in report.get("minute_mismatches", [])
            }
            existing.update(
                {
                    row["minute_us"]: row
                    for row in differences
                    if day_us <= row["minute_us"] < day_us + 86_400_000_000
                }
            )
            report["minute_mismatches"] = list(existing.values())[-1440:]
            record.report = report
            record.save(update_fields=["report"])
