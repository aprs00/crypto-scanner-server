from __future__ import annotations

import logging
import signal
import threading
import time

import psycopg
from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.db import close_old_connections
from django.utils import timezone
from redis.exceptions import RedisError

from core.redis_config import get_redis_connection
from footprints.chart_service import (
    TransientChartBuildError,
    advance_ready_series,
    build_next_queued_series,
    expire_stale_series,
    open_chart_stores,
    recover_interrupted_builds,
)
from footprints.wakeups import wait_for_wakeups


logger = logging.getLogger(__name__)
STORAGE_RETRY_SECONDS = 5.0
EXPIRED_SERIES_CLEANUP_SECONDS = 60.0
SINGLETON_VERIFY_SECONDS = 5.0
FULL_CATCHUP_SECONDS = 5.0
WAKEUP_WAIT_SECONDS = 1
MAINTENANCE_ADVISORY_LOCK_ID = 0x435346504D41494E


class Command(BaseCommand):
    help = "Build and incrementally advance cached Tick/TREV chart series"

    def handle(self, *args, **options):
        stop = threading.Event()
        previous_handlers = _install_signal_handlers(stop)
        stores = None
        singleton = None
        wakeup_client = None
        try:
            singleton = _acquire_singleton()
            stores = open_chart_stores(role="chart-maintenance")
            recovered = recover_interrupted_builds()
            if recovered:
                logger.info("Recovered %s interrupted chart builds", recovered)
            expired = expire_stale_series(now=timezone.now())
            if expired:
                logger.info("Expired %s idle chart series", expired)
            next_cleanup = time.monotonic() + EXPIRED_SERIES_CLEANUP_SECONDS
            next_singleton_verify = time.monotonic() + SINGLETON_VERIFY_SECONDS
            next_full_catchup = time.monotonic() + FULL_CATCHUP_SECONDS
            pending_market_ids: set[int] = set()
            full_advance_requested = True
            build_requested = True

            while not stop.is_set():
                started = time.monotonic()
                if started >= next_singleton_verify:
                    _verify_singleton(singleton)
                    next_singleton_verify = started + SINGLETON_VERIFY_SECONDS
                if started >= next_cleanup:
                    expired = expire_stale_series(now=timezone.now())
                    if expired:
                        logger.info("Expired %s idle chart series", expired)
                    next_cleanup = started + EXPIRED_SERIES_CLEANUP_SECONDS
                if started >= next_full_catchup:
                    full_advance_requested = True
                    build_requested = True
                    next_full_catchup = started + FULL_CATCHUP_SECONDS

                if full_advance_requested or pending_market_ids:
                    market_ids = None if full_advance_requested else pending_market_ids
                    full_advance_requested = False
                    pending_market_ids = set()
                    advance_ok, _advance_error = _guard(
                        "live chart advancement",
                        advance_ready_series,
                        execution_store=stores.executions,
                        bar_store=stores.bars,
                        market_ids=market_ids,
                    )
                    if not advance_ok:
                        full_advance_requested = True
                        if stop.wait(STORAGE_RETRY_SECONDS):
                            break
                        continue

                if build_requested:
                    build_requested = False
                    build_ok, build_result = _guard(
                        "queued chart build",
                        build_next_queued_series,
                        execution_store=stores.executions,
                        bar_store=stores.bars,
                    )
                    if not build_ok:
                        if isinstance(build_result, TransientChartBuildError):
                            build_requested = True
                            if stop.wait(STORAGE_RETRY_SECONDS):
                                break
                            continue
                    elif build_result:
                        # Drain another queued build without delaying live
                        # advancement for series that are already ready.
                        full_advance_requested = True
                        build_requested = True
                        continue

                try:
                    if wakeup_client is None:
                        wakeup_client = get_redis_connection()
                    wakeup = wait_for_wakeups(
                        wakeup_client,
                        timeout_seconds=WAKEUP_WAIT_SECONDS,
                    )
                except RedisError:
                    logger.warning(
                        "Chart maintenance wakeup queue is unavailable; "
                        "periodic ClickHouse catch-up remains active"
                    )
                    _close_wakeup_client(wakeup_client)
                    wakeup_client = None
                    if stop.wait(WAKEUP_WAIT_SECONDS):
                        break
                    continue
                if wakeup is not None:
                    pending_market_ids.update(wakeup.market_ids)
                    build_requested = build_requested or wakeup.build_requested
        finally:
            _close_wakeup_client(wakeup_client)
            if stores is not None:
                stores.close()
            if singleton is not None:
                singleton.close()
            close_old_connections()
            _restore_signal_handlers(previous_handlers)


def _guard(label, operation, *args, **kwargs):
    try:
        close_old_connections()
        return True, operation(*args, **kwargs)
    except Exception as exc:
        logger.exception("Chart maintenance failed during %s", label)
        return False, exc
    finally:
        close_old_connections()


def _install_signal_handlers(stop: threading.Event):
    previous = {}
    for signum in (signal.SIGINT, signal.SIGTERM):
        previous[signum] = signal.getsignal(signum)
        signal.signal(signum, lambda _signum, _frame: stop.set())
    return previous


def _restore_signal_handlers(previous) -> None:
    for signum, handler in previous.items():
        signal.signal(signum, handler)


def _close_wakeup_client(client) -> None:
    if client is None:
        return
    try:
        client.close()
    except RedisError:
        logger.warning("Failed to close the chart maintenance wakeup client")


def _acquire_singleton():
    configuration = settings.DATABASES["default"]
    connection = psycopg.connect(
        dbname=configuration["NAME"],
        user=configuration["USER"],
        password=configuration["PASSWORD"],
        host=configuration["HOST"],
        port=configuration["PORT"],
        connect_timeout=10,
        autocommit=True,
    )
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT pg_try_advisory_lock(%s)",
                (MAINTENANCE_ADVISORY_LOCK_ID,),
            )
            row = cursor.fetchone()
        if row != (True,):
            raise CommandError("another footprint maintenance process is active")
    except Exception:
        connection.close()
        raise
    return connection


def _verify_singleton(connection) -> None:
    with connection.cursor() as cursor:
        cursor.execute("SELECT 1")
        if cursor.fetchone() != (1,):
            raise RuntimeError("footprint maintenance advisory lock connection failed")
