"""Measure a cold build and snapshot handler against the configured databases.

Reads retained executions; writes only to temporary benchmark tables, removed
on exit. Run in a standalone process with the app's database environment:
python -m benchmarks.trev_full --market-id 2 > baseline.json
python -m benchmarks.trev_full --baseline baseline.json > comparison.json
"""

import argparse
import hashlib
import json
import logging
import os
import platform
import sys
import time
import uuid
from contextlib import ExitStack
from datetime import datetime
from pathlib import Path
from unittest.mock import patch


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--market-id", type=int, default=2)
    parser.add_argument("--chart-type", choices=("trev", "tick"), default="trev")
    parser.add_argument("--trades-per-bar", type=int, default=1000)
    parser.add_argument("--lookback-days", type=int, default=1)
    parser.add_argument("--prepared-dir", default="", help="use a shared prepared TREV input directory")
    parser.add_argument("--prepare", action="store_true", help="prepare input before the timed build")
    parser.add_argument("--continuation", type=int, default=500)
    parser.add_argument("--reversal", type=int, default=700)
    parser.add_argument("--backend", choices=("rust", "python"), default="rust")
    parser.add_argument("--baseline", help="replay and verify a captured benchmark JSON")
    parser.add_argument("--checkpoint-every", type=int, default=1_000_000,
                        help="native checkpoint interval; Python keeps its existing interval")
    args = parser.parse_args()
    if args.checkpoint_every <= 0 or args.checkpoint_every % 100_000:
        parser.error("checkpoint interval must be a positive multiple of 100000")
    frozen = json.loads(Path(args.baseline).read_text()) if args.baseline else None
    if frozen:
        args.market_id = frozen.get("market_id", args.market_id)
        args.continuation = frozen.get("continuation_ticks", args.continuation)
        args.reversal = frozen.get("reversal_ticks", args.reversal)
        args.chart_type = frozen.get("chart_type", "trev")
        args.trades_per_bar = frozen.get("trades_per_bar", args.trades_per_bar)
        args.lookback_days = frozen.get("lookback_days", args.lookback_days)

    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "core.settings")
    import django
    django.setup()
    from django.db import connection
    from django.test import RequestFactory, override_settings
    from django.utils import timezone
    from footprints import chart_api, chart_service as service
    from footprints.models import ChartSeries, ChartSeriesStatus
    from footprints.storage.charts import ClickHouseChartStore
    from footprints.storage.clickhouse import (
        ClickHouseAccess, ClickHouseSettings, create_clickhouse_client,
    )
    from footprints.storage.clickhouse_schema import CHART_BAR_COLUMNS
    from footprints.storage.native import NativeReplayReader
    from footprints.storage.reconciled import ReconciledExecutionStore
    from footprints.trev import TrevConfig
    from footprints.tick import TickConfig

    if args.chart_type == "trev":
        TrevConfig(args.continuation, args.reversal)
    else:
        TickConfig(args.trades_per_bar)
    config = ClickHouseSettings.from_environment()
    client = create_clickhouse_client(
        role="isolated-full-trev-benchmark", for_inserts=True,
        operation_timeout_seconds=600,
    )
    name = "trev_bench_" + uuid.uuid4().hex[:16]
    table = ChartSeries._meta.db_table
    quote = connection.ops.quote_name
    metrics = dict(insert_calls=0, insert_seconds=0.0, bars=0, levels=0,
                   checkpoint_calls=0, checkpoint_seconds=0.0)
    native_readers = []
    messages = []

    class Handler(logging.Handler):
        def emit(self, record):
            messages.append(record.getMessage())
            print(record.getMessage(), file=sys.stderr, flush=True)

    class Access(ClickHouseAccess):
        def insert(self, table, rows, **kwargs):
            started = time.perf_counter()
            result = super().insert(table, rows, **kwargs)
            metrics["insert_seconds"] += time.perf_counter() - started
            metrics["insert_calls"] += 1
            metrics["bars"] += len(rows)
            metrics["levels"] += sum(len(row[13]) for row in rows)
            return result

        def insert_native(self, *args, **kwargs):
            started = time.perf_counter()
            result = super().insert_native(*args, **kwargs)
            metrics["insert_seconds"] += time.perf_counter() - started
            metrics["insert_calls"] += 1
            return result

    class BarStore(ClickHouseChartStore):
        def append_native_batches(self, series_id, batches):
            batches = tuple(batches)
            result = super().append_native_batches(series_id, batches)
            metrics["bars"] += result
            metrics["levels"] += sum(b.level_count for b in batches)
            return result

    class Reader(ReconciledExecutionStore):
        captured = False

        def latest_cursor(self, **kwargs):
            if not self.captured:
                self.fixed = super().latest_cursor(**kwargs)
                self.imports = self._fixed.get(kwargs["market_id"])
                self.captured = True
            if self.imports is not None:
                self._fixed[kwargs["market_id"]] = self.imports
            return self.fixed

    persist = service._persist_build_state
    reader_init = NativeReplayReader.__init__

    def timed_persist(*args, **kwargs):
        started = time.perf_counter()
        result = persist(*args, **kwargs)
        metrics["checkpoint_seconds"] += time.perf_counter() - started
        metrics["checkpoint_calls"] += 1
        return result

    def track_reader(self, *args, **kwargs):
        reader_init(self, *args, **kwargs)
        native_readers.append(self)

    access = Access(client)
    original_search_path = None
    created_pg = created_ch = False
    handler = Handler()
    log_level = service.logger.level
    service.logger.addHandler(handler)
    service.logger.setLevel(logging.INFO)
    try:
        with connection.cursor() as cursor:
            cursor.execute("SHOW search_path")
            original_search_path = cursor.fetchone()[0]
            cursor.execute(f"CREATE SCHEMA {quote(name)}")
            created_pg = True
            cursor.execute(f"CREATE TABLE {quote(name)}.{quote(table)} "
                           f"(LIKE public.{quote(table)} INCLUDING ALL)")
            cursor.execute(f"SET search_path TO {quote(name)}, public")
        client.command(f"CREATE DATABASE {name}")
        created_ch = True
        client.command(f"CREATE TABLE {name}.chart_bars AS {config.database}.chart_bars")
        anchor = (datetime.fromisoformat(frozen["anchor"]) if frozen
                  else service.lookback_anchor(timezone.now(), lookback_days=30 if args.chart_type == "trev" else args.lookback_days))
        # Explicit ID avoids advancing the public table's sequence.
        series = ChartSeries.objects.create(
            pk=1, market_id=args.market_id, chart_type=args.chart_type,
            parameter_a=args.continuation if args.chart_type == "trev" else args.trades_per_bar,
            parameter_b=args.reversal if args.chart_type == "trev" else 0,
            lookback_days=None if args.chart_type == "trev" else args.lookback_days,
            status=ChartSeriesStatus.BUILDING, anchor_at=anchor,
        )
        reader = Reader(access, database=config.database,
                        spec_divisors=service._projector_spec_divisors(),
                        load_spec_divisor=service._projector_spec_divisor)
        if frozen:
            reader.fixed = tuple(frozen["cursor"]) if frozen["cursor"] else None
            imports = frozen.get("archive_imports")
            reader.imports = {int(day): value for day, value in imports.items()} if imports is not None else None
            if reader.fixed and reader.fixed[0] == -2 and reader.imports is None:
                raise ValueError("reconciled baseline must include archive_imports")
            reader.captured = True
        bars = BarStore(access, database=name)
        if args.prepare:
            if not args.prepared_dir or args.chart_type != "trev":
                raise ValueError("--prepare requires --prepared-dir and a TREV chart")
            from footprints.storage.prepared import DAY_US, prepare_market

            preparation = time.perf_counter()
            prepare_market(reader, market_id=args.market_id, directory=args.prepared_dir,
                           now_us=int(anchor.timestamp()*1_000_000) + 30*DAY_US + 2*3_600_000_000)
            metrics["preparation_seconds"] = time.perf_counter() - preparation
        with ExitStack() as stack:
            stack.enter_context(override_settings(TREV_COLD_BACKEND=args.backend))
            stack.enter_context(override_settings(TREV_PREPARED_DIR=args.prepared_dir))
            stack.enter_context(patch.object(service, "CHART_NATIVE_BUILD_EXECUTION_CHUNK", args.checkpoint_every))
            stack.enter_context(patch.object(service, "_persist_build_state", timed_persist))
            stack.enter_context(patch.object(NativeReplayReader, "__init__", track_reader))
            stack.enter_context(patch.object(service, "publish_chart_update"))
            stack.enter_context(patch.object(service, "publish_chart_error"))
            started = time.perf_counter()
            service.build_series(series.pk, execution_store=reader, bar_store=bars)
            metrics["cold_build_seconds"] = time.perf_counter() - started
            metrics["decode_seconds"] = sum(r.decode_seconds for r in native_readers)
            metrics["read_seconds"] = sum(r.fetch_seconds for r in native_readers)
            service.advance_ready_series(
                execution_store=reader, bar_store=bars, market_ids=[args.market_id],
            )
            metrics["fixed_stream_ready_seconds"] = time.perf_counter() - started
            series.refresh_from_db()
            if series.status != ChartSeriesStatus.READY:
                raise RuntimeError(f"benchmark build failed: {series.status} {series.error_code}")
            stack.enter_context(patch.object(chart_api, "_chart_bar_store", return_value=bars))
            request = RequestFactory().get("/", {"seriesKey": str(series.public_key)})
            snapshot_start = time.perf_counter()
            response = chart_api.chart_bars(request)
            metrics["snapshot_handler_seconds"] = time.perf_counter() - snapshot_start
            metrics["snapshot_ready_seconds"] = time.perf_counter() - started
            if response.status_code != 200:
                raise RuntimeError(f"snapshot failed: {response.status_code} {response.content!r}")
            metrics["snapshot_bytes"] = len(response.content)

        verification_start = time.perf_counter()
        count, levels, checksum = client.query(
            f"SELECT count(), sum(length(level_ticks)), "
            f"sum(cityHash64(tuple({', '.join(CHART_BAR_COLUMNS)}))) "
            f"FROM {name}.chart_bars FINAL WHERE series_id=1"
        ).result_rows[0]
        if (count, levels) != (metrics["bars"], metrics["levels"]):
            raise AssertionError("persisted bar/level totals differ from inserted totals")
        metrics["persisted_bar_checksum"] = str(checksum)
        metrics["verification_seconds"] = time.perf_counter() - verification_start
        engine, cursor = service._decode_checkpoint(series)
        metrics.update(
            event="result", backend=args.backend, machine=platform.machine(),
            python=platform.python_version(), market_id=args.market_id,
            chart_type=args.chart_type, trades_per_bar=args.trades_per_bar,
            lookback_days=args.lookback_days, prepared=bool(args.prepared_dir),
            continuation_ticks=args.continuation, reversal_ticks=args.reversal,
            checkpoint_every=((5_000_000 if args.prepared_dir and args.chart_type == "trev"
                               else args.checkpoint_every) if args.backend == "rust"
                              else service.CHART_BUILD_EXECUTION_CHUNK),
            anchor=anchor.isoformat(),
            cursor=cursor, archive_imports=reader.imports,
            executions=engine.state.last_order.rank + 1 if engine.state.last_order else 0,
            status=series.status, messages=messages,
            checkpoint_sha256=hashlib.sha256(json.dumps(
                series.checkpoint_state, sort_keys=True, separators=(",", ":"),
            ).encode()).hexdigest(),
            scope="fixed captured stream with isolated durable ClickHouse/PostgreSQL "
                  "writes and snapshot handler; excludes scheduling, live arrivals, "
                  "network transport, browser rendering, shared input preparation "
                  "and post-build verification",
        )
        if frozen:
            for key in ("executions", "bars", "levels", "checkpoint_sha256", "persisted_bar_checksum"):
                if key in frozen and metrics[key] != frozen[key]:
                    raise AssertionError(f"baseline mismatch for {key}: {metrics[key]} != {frozen[key]}")
        print(json.dumps(metrics), flush=True)
    finally:
        service.logger.removeHandler(handler)
        service.logger.setLevel(log_level)
        try:
            if original_search_path is not None:
                with connection.cursor() as cursor:
                    cursor.execute("SELECT set_config(%s, %s, false)", ["search_path", original_search_path])
                    if created_pg:
                        cursor.execute(f"DROP SCHEMA {quote(name)} CASCADE")
        finally:
            try:
                if created_ch:
                    client.command(f"DROP DATABASE {name} SYNC")
            finally:
                client.close()
                connection.close()


if __name__ == "__main__":
    main()
