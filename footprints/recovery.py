"""Free public trade reconciliation and atomic, verified historical overrides."""

from __future__ import annotations

import csv
import gzip
import hashlib
import io
import json
import logging
import shutil
import sqlite3
import tempfile
import urllib.error
import urllib.parse
import urllib.request
import uuid
import zipfile
from bisect import bisect_right
from contextlib import closing
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from django.db import transaction
from django.utils import timezone as django_timezone

from footprints.chart_live import publish_chart_error
from footprints.models import (
    ChartSeries,
    ChartType,
    FootprintMarket,
    RecoveryStatus,
    TradeRecoveryDay,
)
from footprints.storage.clickhouse import (
    deduplicated_immediate_insert_settings,
    qualified_table,
)
from footprints.storage.clickhouse_schema import ensure_archive_schema
from footprints.storage.reconciled import DAY_US
from footprints.wakeups import publish_build_wakeup

logger = logging.getLogger(__name__)
EXCHANGES = ("binance", "bybit")
HOUR_US = 3_600_000_000
MINUTE_US = 60_000_000
IMPORT_BATCH = 100_000


def request_json(url):
    with urllib.request.urlopen(
        urllib.request.Request(
            url, headers={"User-Agent": "crypto-scanner-recovery/1"}
        ),
        timeout=10,
    ) as response:
        value = json.load(response)
    if isinstance(value, dict) and value.get("retCode", 0) != 0:
        raise ValueError(f"Exchange rejected request: {value.get('retMsg')}")
    return value


def candles(exchange, symbol, start_us, end_us, *, minute=False):
    if exchange not in EXCHANGES:
        raise ValueError("recovery supports only Binance and Bybit")
    parameters = {"symbol": symbol, "limit": 1000}
    if exchange == "binance":
        parameters.update(
            interval="1m" if minute else "1h",
            startTime=start_us // 1000,
            endTime=(end_us - 1) // 1000,
        )
        rows = request_json(
            "https://fapi.binance.com/fapi/v1/klines?"
            + urllib.parse.urlencode(parameters)
        )
    else:
        parameters.update(
            category="linear",
            interval="1" if minute else "60",
            start=start_us // 1000,
            end=(end_us - 1) // 1000,
        )
        rows = request_json(
            "https://api.bybit.com/v5/market/kline?"
            + urllib.parse.urlencode(parameters)
        )["result"]["list"]
    return {
        int(row[0]) * 1000: (
            int(row[8]) if exchange == "binance" else None,
            Decimal(row[5]),
        )
        for row in rows
    }


def recent_trades(exchange, symbol, market_key):
    """One bounded public snapshot; no credentials or historical cursor."""
    from footprints.collectors import binance, bybit

    symbol = urllib.parse.quote(symbol, safe="")
    if exchange == "binance":
        rows = request_json(
            f"https://fapi.binance.com/fapi/v1/trades?symbol={symbol}&limit=1000"
        )
        return [
            binance.normalize_trade(
                {
                    "t": r["id"],
                    "m": r["isBuyerMaker"],
                    "T": r["time"],
                    "p": r["price"],
                    "q": r["qty"],
                },
                market_key,
            )
            for r in rows
        ]
    if exchange == "bybit":
        rows = request_json(
            f"https://api.bybit.com/v5/market/recent-trade?category=linear&symbol={symbol}&limit=1000"
        )["result"]["list"]
        return [
            bybit.normalize_trade(
                {
                    "i": r["execId"],
                    "S": r["side"],
                    "seq": r["seq"],
                    "T": r["time"],
                    "p": r["price"],
                    "v": r["size"],
                },
                market_key,
            )
            for r in reversed(rows)
        ]
    raise ValueError("unsupported recovery exchange")


def replace_series(market, *, start_us=None, series_id=None):
    """Caller holds the market row lock. New IDs fence old builds and caches."""
    query = ChartSeries.objects.filter(market=market)
    if series_id is not None:
        query = query.filter(pk=series_id)
    if start_us is not None:
        query = query.filter(
            anchor_at__lt=datetime.fromtimestamp(start_us / 1e6, timezone.utc)
        )
    rows = list(query)
    old_keys = [str(row.public_key) for row in rows]
    query.delete()
    for row in rows:
        anchor = row.anchor_at
        if row.chart_type == ChartType.TREV:
            from footprints.chart_service import lookback_anchor
            from footprints.constants import CHART_MAX_LOOKBACK_DAYS

            anchor = lookback_anchor(django_timezone.now(), CHART_MAX_LOOKBACK_DAYS)
        ChartSeries.objects.create(
            market=market,
            chart_type=row.chart_type,
            parameter_a=row.parameter_a,
            parameter_b=row.parameter_b,
            lookback_days=row.lookback_days,
            anchor_at=anchor,
            last_requested_at=row.last_requested_at,
        )

    def notify():
        for key in old_keys:
            try:
                publish_chart_error(key, "CHART_SERIES_REPLACED")
            except Exception:
                logger.exception(
                    "Chart replacement notification failed; clients will rediscover it"
                )
        try:
            publish_build_wakeup()
        except Exception:
            logger.exception(
                "Chart replacement wakeup failed; periodic builder will catch up"
            )

    transaction.on_commit(notify)


def publish_import(market_id, day, start_us, import_id, fingerprint, report):
    with transaction.atomic():
        market = FootprintMarket.objects.select_for_update().get(pk=market_id)
        if market.exchange not in EXCHANGES:
            raise ValueError("unsupported recovery exchange")
        record, _ = TradeRecoveryDay.objects.get_or_create(
            market=market, day=day, defaults={"start_us": start_us}
        )
        unchanged = record.active_import == import_id
        first_activation = not market.recovery_enabled
        market.recovery_enabled = True
        market.save(update_fields=["recovery_enabled"])
        record.active_import = import_id
        record.start_us = start_us
        record.fingerprint = fingerprint
        record.report = report
        record.status = RecoveryStatus.VERIFIED
        record.error = ""
        record.retry_at = None
        record.checked_at = django_timezone.now()
        record.save()
        if unchanged:
            return False
        replace_series(
            market,
            start_us=None
            if first_activation
            else (
                int(
                    datetime.combine(day, datetime.min.time(), timezone.utc).timestamp()
                )
                * 1_000_000
                + DAY_US
            ),
        )
    return True


def download_archive(exchange, symbol, day, directory):
    symbol = urllib.parse.quote(symbol, safe="")
    if exchange == "binance":
        url = f"https://data.binance.vision/data/futures/um/daily/trades/{symbol}/{symbol}-trades-{day.isoformat()}.zip"
    else:
        url = f"https://public.bybit.com/trading/{symbol}/{symbol}{day.isoformat()}.csv.gz"
    target = Path(directory) / (
        "trades.zip" if exchange == "binance" else "trades.csv.gz"
    )
    digest = hashlib.sha256()
    total = 0
    with (
        urllib.request.urlopen(url, timeout=30) as response,
        target.open("wb") as output,
    ):
        while chunk := response.read(1024**2):
            total += len(chunk)
            if total > 1024**3 or shutil.disk_usage(directory).free < 5 * 1024**3:
                raise OSError("archive download exceeded disk budget")
            digest.update(chunk)
            output.write(chunk)
    fingerprint = digest.hexdigest()
    if exchange == "binance":
        with urllib.request.urlopen(url + ".CHECKSUM", timeout=10) as response:
            expected = response.read(4096).decode().split()[0]
        if expected != fingerprint:
            raise ValueError("Binance archive checksum mismatch")
    return target, fingerprint


def archive_rows(path, exchange):
    if exchange == "binance":
        with zipfile.ZipFile(path) as archive:
            members = [m for m in archive.infolist() if m.filename.endswith(".csv")]
            if len(members) != 1 or members[0].file_size > 4 * 1024**3:
                raise ValueError("unexpected Binance archive contents")
            with archive.open(members[0]) as stream:
                yield from csv.DictReader(io.TextIOWrapper(stream))
    else:
        with gzip.open(path, "rt", newline="") as stream:

            def bounded_lines():
                size = 0
                for line in stream:
                    size += len(line)
                    if size > 4 * 1024**3:
                        raise ValueError(
                            "Bybit archive exceeds decompressed size budget"
                        )
                    yield line

            yield from csv.DictReader(bounded_lines())


def normalize_archive_row(row, exchange, price_scale, quantity_scale):
    if exchange == "binance":
        identity = row.get("id", row.get("trade_id"))
        stamp = Decimal(row["time"]) * 1000
        price, quantity = Decimal(row["price"]), Decimal(row["qty"])
        maker = row.get("is_buyer_maker", row.get("isBuyerMaker", "")).lower()
        if maker not in ("true", "false") or identity is None:
            raise ValueError("invalid Binance archive identity or side")
        buy = maker == "false"
    else:
        identity = row["trdMatchID"]
        # Archive fractional seconds carry more precision than V5's T (ms).
        stamp = Decimal(int(Decimal(row["timestamp"]) * 1000)) * 1000
        price, quantity = Decimal(row["price"]), Decimal(row["size"])
        if row["side"] not in ("Buy", "Sell"):
            raise ValueError("invalid Bybit archive side")
        buy = row["side"] == "Buy"
    if (
        not identity
        or not stamp.is_finite()
        or stamp <= 0
        or not price.is_finite()
        or not quantity.is_finite()
        or price <= 0
        or quantity <= 0
    ):
        raise ValueError("invalid archive execution")
    p, q = price.scaleb(price_scale), quantity.scaleb(quantity_scale)
    if p != p.to_integral_value() or q != q.to_integral_value() or max(p, q) >= 2**63:
        raise ValueError("archive execution exceeds exact atom representation")
    return str(identity), int(stamp), int(p), int(q), int(buy)


class Recovery:
    def __init__(self, access, database, *, verify_writer=lambda: None):
        self.access = access
        self.database = database
        self.raw = qualified_table(database, "raw_executions")
        self.archive = qualified_table(database, "archive_executions")
        self.verify_writer = verify_writer

    def bounds(self, market):
        row = self.access.query(
            f"SELECT minOrNull(timestamp_us),maxOrNull(timestamp_us) FROM {self.raw} WHERE market_id={{market:UInt64}}",
            parameters={"market": market.pk},
            operation="recovery_bounds",
        ).result_rows[0]
        return tuple(row)

    def audit_minutes(self, market, start_us, end_us):
        expected = candles(
            market.exchange, market.symbol, start_us, end_us, minute=True
        )
        rows = self.access.query(
            f"""SELECT intDiv(timestamp_us,{MINUTE_US})*{MINUTE_US},count(),toString(sum(quantity_atoms))
            FROM {self.raw} WHERE market_id={{market:UInt64}} AND timestamp_us>={{start:Int64}} AND timestamp_us<{{end:Int64}}
            GROUP BY 1""",
            parameters={"market": market.pk, "start": start_us, "end": end_us},
            operation="reconcile_minutes",
        ).result_rows
        stored = {
            r[0]: (r[1], Decimal(r[2]).scaleb(-market.quantity_scale)) for r in rows
        }
        differences = []
        for minute in range(start_us, end_us, MINUTE_US):
            if minute not in expected:
                raise ValueError("exchange candle response is incomplete")
            count, volume = expected[minute]
            actual_count, actual_volume = stored.get(minute, (0, Decimal(0)))
            if (count is not None and count != actual_count) or volume != actual_volume:
                differences.append(
                    {
                        "minute_us": minute,
                        "missing_count": None
                        if count is None
                        else count - actual_count,
                        "missing_volume": str(volume - actual_volume),
                    }
                )
        return differences

    def _live_to_stage(self, db, market, start_us, end_us):
        duplicates = 0
        sql = f"""SELECT trade_id,timestamp_us,price_atoms,quantity_atoms,is_taker_buy,
            source_epoch,source_sequence,batch_index FROM {self.raw}
            WHERE market_id={{market:UInt64}} AND timestamp_us>={{start:Int64}} AND timestamp_us<{{end:Int64}}
            ORDER BY source_epoch,source_sequence,batch_index,timestamp_us,trade_id"""
        with closing(
            self.access.stream_rows(
                sql,
                parameters={"market": market.pk, "start": start_us, "end": end_us},
                operation="stage_live_day",
            )
        ) as rows:
            for row in rows:
                try:
                    db.execute("INSERT INTO live VALUES (?,?,?,?,?,?,?,?)", tuple(row))
                except sqlite3.IntegrityError:
                    previous = db.execute(
                        "SELECT stamp,price,quantity,buy FROM live WHERE id=?",
                        (row[0],),
                    ).fetchone()
                    if previous != tuple(row[1:5]):
                        raise ValueError("conflicting live execution identity")
                    duplicates += 1
        db.commit()
        return duplicates

    def repair_day(
        self, market, day, *, repair=False, refresh=False, record_audit=False
    ):
        if market.exchange not in EXCHANGES:
            raise ValueError("unsupported recovery exchange")
        now = django_timezone.now()
        if day >= now.date() or day < (now - timedelta(days=30)).date():
            raise ValueError(
                "only completed days within 30-day retention can be repaired"
            )
        first, _ = self.bounds(market)
        if first is None:
            return {"status": "no_live_history"}
        day_us = (
            int(datetime.combine(day, datetime.min.time(), timezone.utc).timestamp())
            * 1_000_000
        )
        start_us, end_us = max(first, day_us), day_us + DAY_US
        if start_us >= end_us:
            return {"status": "outside_collection_range"}
        record = TradeRecoveryDay.objects.filter(market=market, day=day).first()
        if record and record.status == RecoveryStatus.VERIFIED and not refresh:
            return record.report
        try:
            with tempfile.TemporaryDirectory(prefix="footprint-recovery-") as directory:
                path, fingerprint = download_archive(
                    market.exchange, market.symbol, day, directory
                )
                with sqlite3.connect(Path(directory) / "compare.sqlite3") as db:
                    db.executescript("""
                        PRAGMA journal_mode=OFF; PRAGMA synchronous=OFF;
                        CREATE TABLE live(id TEXT PRIMARY KEY,stamp INTEGER,price INTEGER,quantity INTEGER,buy INTEGER,epoch INTEGER,seq INTEGER,batch INTEGER);
                        CREATE TABLE archive(id TEXT PRIMARY KEY,stamp INTEGER,price INTEGER,quantity INTEGER,buy INTEGER,ordinal INTEGER UNIQUE,spec INTEGER);
                        CREATE INDEX archive_time ON archive(stamp);
                    """)
                    specifications = list(
                        market.instrument_specs.order_by("effective_from", "pk")
                    )
                    effective = [
                        int(s.effective_from.timestamp()) * 1_000_000
                        + s.effective_from.microsecond
                        for s in specifications
                    ]
                    divisors = [
                        (
                            s.tick_size.scaleb(market.price_scale),
                            s.quantity_step.scaleb(market.quantity_scale),
                        )
                        for s in specifications
                    ]
                    if any(
                        value <= 0 or value != value.to_integral_value()
                        for pair in divisors
                        for value in pair
                    ):
                        raise ValueError(
                            "historical specification is not exact at the storage scale"
                        )
                    previous_order = None
                    for ordinal, raw in enumerate(archive_rows(path, market.exchange)):
                        if ordinal % IMPORT_BATCH == 0:
                            self.verify_writer()
                            if shutil.disk_usage(directory).free < 5 * 1024**3:
                                raise OSError("archive staging exceeded disk reserve")
                        normalized = normalize_archive_row(
                            raw,
                            market.exchange,
                            market.price_scale,
                            market.quantity_scale,
                        )
                        identity, stamp, p, q, buy = normalized
                        if not day_us <= stamp < end_us:
                            raise ValueError(
                                "archive contains executions outside its UTC day"
                            )
                        order = int(identity) if market.exchange == "binance" else stamp
                        if previous_order is not None and order < previous_order:
                            raise ValueError("archive execution order regressed")
                        previous_order = order
                        if stamp < start_us:
                            continue
                        index = bisect_right(effective, stamp) - 1
                        if index < 0:
                            raise ValueError(
                                "no historical instrument specification for archive trade"
                            )
                        tick, step = divisors[index]
                        if p % int(tick) or q % int(step):
                            raise ValueError(
                                "archive execution is not aligned with its historical specification"
                            )
                        try:
                            db.execute(
                                "INSERT INTO archive VALUES (?,?,?,?,?,?,?)",
                                (*normalized, ordinal, specifications[index].pk),
                            )
                        except sqlite3.IntegrityError as exc:
                            raise ValueError(
                                "archive contains duplicate execution identities"
                            ) from exc
                    db.commit()
                    duplicates = self._live_to_stage(db, market, start_us, end_us)
                    conflicts = db.execute("""SELECT count(*) FROM archive a JOIN live l USING(id)
                        WHERE a.stamp!=l.stamp OR a.price!=l.price OR a.quantity!=l.quantity OR a.buy!=l.buy""").fetchone()[
                        0
                    ]
                    extras = db.execute(
                        "SELECT count(*) FROM live l LEFT JOIN archive a USING(id) WHERE a.id IS NULL"
                    ).fetchone()[0]
                    if conflicts or extras:
                        raise ValueError(
                            f"archive disagrees with live identities: conflicting={conflicts} absent_from_archive={extras}"
                        )
                    if market.exchange == "bybit":
                        previous = None
                        for order in db.execute(
                            "SELECT l.epoch,l.seq,l.batch FROM archive a JOIN live l USING(id) ORDER BY a.ordinal"
                        ):
                            if previous is not None and order < previous:
                                raise ValueError(
                                    "Bybit archive order disagrees with overlapping live sequence"
                                )
                            previous = order
                    candle_start = ((start_us + HOUR_US - 1) // HOUR_US) * HOUR_US
                    expected = (
                        candles(market.exchange, market.symbol, candle_start, end_us)
                        if candle_start < end_us
                        else {}
                    )
                    for hour in range(candle_start, end_us, HOUR_US):
                        if hour not in expected:
                            raise ValueError(
                                "exchange hourly candle response is incomplete"
                            )
                        count, atoms = db.execute(
                            "SELECT count(*),coalesce(sum(quantity),0) FROM archive WHERE stamp>=? AND stamp<?",
                            (hour, hour + HOUR_US),
                        ).fetchone()
                        expected_count, expected_volume = expected[hour]
                        if (
                            expected_count is not None and count != expected_count
                        ) or Decimal(atoms).scaleb(
                            -market.quantity_scale
                        ) != expected_volume:
                            raise ValueError(
                                f"archive disagrees with exchange candle at {hour}"
                            )
                    missing, missing_atoms = (
                        db.execute("""SELECT count(*),coalesce(sum(a.quantity),0)
                        FROM archive a LEFT JOIN live l USING(id) WHERE l.id IS NULL""").fetchone()
                    )
                    report = {
                        "exchange": market.exchange,
                        "day": str(day),
                        "missing_trades": missing,
                        "missing_volume": str(
                            Decimal(missing_atoms).scaleb(-market.quantity_scale)
                        ),
                        "duplicate_live_rows": duplicates,
                        "archive_rows": db.execute(
                            "SELECT count(*) FROM archive"
                        ).fetchone()[0],
                        "archive_verified": True,
                        "status": "repair_required"
                        if missing or duplicates
                        else "matches",
                    }
                    if repair:
                        if missing or duplicates or record and record.active_import:
                            report["status"] = "repaired"
                            ensure_archive_schema(self.access, self.database)
                            import_id = uuid.uuid5(
                                uuid.NAMESPACE_URL,
                                f"footprint-v1:{market.pk}:{day}:{fingerprint}:{start_us}:{[(s.pk, str(s.effective_from)) for s in specifications]}",
                            )
                            rows = db.execute(
                                "SELECT ordinal,stamp,id,price,quantity,buy,spec FROM archive ORDER BY ordinal"
                            )
                            while batch := rows.fetchmany(IMPORT_BATCH):
                                self.verify_writer()
                                # A deterministic import can resume after a lost acknowledgement.
                                existing = self.access.query(
                                    f"SELECT count(),uniqExact(ordinal) FROM {self.archive} WHERE market_id={{market:UInt64}} AND import_id={{import:UUID}} AND ordinal BETWEEN {{first:UInt64}} AND {{last:UInt64}}",
                                    parameters={
                                        "market": market.pk,
                                        "import": str(import_id),
                                        "first": batch[0][0],
                                        "last": batch[-1][0],
                                    },
                                    operation="resume_archive_batch",
                                ).result_rows[0]
                                if existing[0] == existing[1] == len(batch):
                                    continue
                                if existing[0]:
                                    raise ValueError(
                                        "archive import contains an incomplete or duplicate batch"
                                    )
                                self.access.insert(
                                    self.archive,
                                    [
                                        (
                                            market.pk,
                                            day,
                                            str(import_id),
                                            *row[:5],
                                            bool(row[5]),
                                            row[6],
                                        )
                                        for row in batch
                                    ],
                                    column_names=(
                                        "market_id",
                                        "day",
                                        "import_id",
                                        "ordinal",
                                        "timestamp_us",
                                        "trade_id",
                                        "price_atoms",
                                        "quantity_atoms",
                                        "is_taker_buy",
                                        "instrument_spec_id",
                                    ),
                                    settings=deduplicated_immediate_insert_settings(),
                                    operation="stage_archive_batch",
                                )
                            stored_count, unique_count = self.access.query(
                                f"SELECT count(),uniqExact(ordinal) FROM {self.archive} WHERE market_id={{market:UInt64}} AND import_id={{import:UUID}}",
                                parameters={
                                    "market": market.pk,
                                    "import": str(import_id),
                                },
                                operation="verify_archive_import",
                            ).result_rows[0]
                            if (
                                stored_count != report["archive_rows"]
                                or unique_count != stored_count
                            ):
                                raise ValueError(
                                    "staged ClickHouse archive row count disagrees"
                                )
                            self.verify_writer()
                            publish_import(
                                market.pk, day, start_us, import_id, fingerprint, report
                            )
                        else:
                            TradeRecoveryDay.objects.update_or_create(
                                market=market,
                                day=day,
                                defaults={
                                    "start_us": start_us,
                                    "status": RecoveryStatus.VERIFIED,
                                    "fingerprint": fingerprint,
                                    "report": report,
                                    "checked_at": now,
                                    "error": "",
                                    "retry_at": None,
                                },
                            )
                    elif record_audit:
                        TradeRecoveryDay.objects.update_or_create(
                            market=market,
                            day=day,
                            defaults={
                                "start_us": start_us,
                                "status": RecoveryStatus.CHECKED,
                                "fingerprint": fingerprint,
                                "report": report,
                                "checked_at": now,
                                "error": "",
                                "retry_at": now + timedelta(hours=6),
                            },
                        )
                    return report
        except Exception as exc:
            if repair or record_audit:
                pending = isinstance(exc, urllib.error.HTTPError) and exc.code == 404
                TradeRecoveryDay.objects.update_or_create(
                    market=market,
                    day=day,
                    defaults={
                        "start_us": start_us,
                        "status": RecoveryStatus.PENDING
                        if pending
                        else RecoveryStatus.FAILED,
                        "retry_at": now + timedelta(hours=6),
                        "checked_at": now,
                        "error": str(exc)[:2000],
                    },
                )
            raise
