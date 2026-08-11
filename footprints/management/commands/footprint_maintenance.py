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

from footprints.chart_service import (
    TransientChartBuildError,
    advance_ready_series,
    build_next_queued_series,
    expire_stale_series,
    open_chart_stores,
    recover_interrupted_builds,
)


logger = logging.getLogger(__name__)
MAINTENANCE_INTERVAL_SECONDS = 0.25
STORAGE_RETRY_SECONDS = 5.0
EXPIRED_SERIES_CLEANUP_SECONDS = 60.0
SINGLETON_VERIFY_SECONDS = 5.0
MAINTENANCE_ADVISORY_LOCK_ID = 0x435346504D41494E


class Command(BaseCommand):
    help = "Build and incrementally advance cached Tick/TREV chart series"

    def handle(self, *args, **options):
        stop = threading.Event()
        previous_handlers = _install_signal_handlers(stop)
        stores = None
        singleton = None
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
                advance_ok, _advance_error = _guard(
                    "live chart advancement",
                    advance_ready_series,
                    execution_store=stores.executions,
                    bar_store=stores.bars,
                )
                if not advance_ok:
                    if stop.wait(STORAGE_RETRY_SECONDS):
                        break
                    continue
                build_ok, build_error = _guard(
                    "queued chart build",
                    build_next_queued_series,
                    execution_store=stores.executions,
                    bar_store=stores.bars,
                )
                if not build_ok and isinstance(
                    build_error, TransientChartBuildError
                ):
                    if stop.wait(STORAGE_RETRY_SECONDS):
                        break
                stop.wait(
                    max(
                        0.01,
                        MAINTENANCE_INTERVAL_SECONDS - (time.monotonic() - started),
                    )
                )
        finally:
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
