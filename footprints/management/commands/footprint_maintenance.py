from __future__ import annotations

import logging
import multiprocessing
import os
import signal
import threading
import time
from concurrent.futures import Future, ProcessPoolExecutor
from concurrent.futures.process import BrokenProcessPool

import psycopg
from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.db import close_old_connections, connections
from django.utils import timezone
from redis.exceptions import RedisError

from core.redis_config import get_redis_connection
from footprints.chart_service import (
    TransientChartBuildError,
    advance_ready_series,
    build_claimed_series,
    claim_queued_market_series,
    discard_series_runtimes,
    ensure_default_chart_series,
    expire_stale_series,
    open_chart_stores,
    recover_interrupted_builds,
    requeue_claimed_series,
)
from footprints.storage.clickhouse import (
    ClickHouseFailureKind,
    classify_clickhouse_error,
)
from footprints.wakeups import wait_for_wakeups


logger = logging.getLogger(__name__)
STORAGE_RETRY_SECONDS = 5.0
EXPIRED_SERIES_CLEANUP_SECONDS = 60.0
SINGLETON_VERIFY_SECONDS = 5.0
FULL_CATCHUP_SECONDS = 5.0
WARM_SERIES_REFRESH_SECONDS = 5 * 60.0
WAKEUP_WAIT_SECONDS = 1
BUILD_WORKERS = 2
BUILD_QUERY_TIMEOUT_SECONDS = 60 * 60
MAINTENANCE_ADVISORY_LOCK_ID = 0x435346504D41494E

_BUILD_STOP = None


class ChartBuildWorkerSetupError(RuntimeError):
    """A build worker cannot safely start with its current configuration."""


class Command(BaseCommand):
    help = "Build and incrementally advance cached Tick/TREV chart series"

    def handle(self, *args, **options):
        stop = threading.Event()
        previous_handlers = _install_signal_handlers(stop)
        build_stop = None
        build_executor = None
        stores = None
        singleton = None
        wakeup_client = None
        try:
            connections.close_all()
            process_context = multiprocessing.get_context("fork")
            build_stop = process_context.Event()
            build_executor = ProcessPoolExecutor(
                max_workers=BUILD_WORKERS,
                mp_context=process_context,
                initializer=_initialize_build_worker,
                initargs=(build_stop,),
            )
            # Fork workers before the coordinator owns any live connection.
            build_executor.submit(_build_worker_ready).result()
            singleton = _acquire_singleton()
            stores = open_chart_stores(role="chart-maintenance")
            recovered = recover_interrupted_builds()
            if recovered:
                logger.info("Recovered %s interrupted chart builds", recovered)
            expired = expire_stale_series(now=timezone.now())
            if expired:
                logger.info("Expired %s idle chart series", expired)
            warmed = ensure_default_chart_series(now=timezone.now())
            if warmed:
                logger.info("Queued %s default chart series", warmed)
            next_cleanup = time.monotonic() + EXPIRED_SERIES_CLEANUP_SECONDS
            next_singleton_verify = time.monotonic() + SINGLETON_VERIFY_SECONDS
            next_full_catchup = time.monotonic() + FULL_CATCHUP_SECONDS
            next_warm_series = time.monotonic() + WARM_SERIES_REFRESH_SECONDS
            next_build_retry = 0.0
            pending_market_ids: set[int] = set()
            builds: dict[Future, tuple[int, ...]] = {}
            full_advance_requested = True
            build_requested = True

            while not stop.is_set():
                started = time.monotonic()
                for future in tuple(builds):
                    if not future.done():
                        continue
                    series_ids = builds.pop(future)
                    discard_series_runtimes(series_ids)
                    try:
                        completed = future.result()
                    except TransientChartBuildError:
                        logger.warning(
                            "Chart build storage is temporarily unavailable; "
                            "retrying series=%s",
                            series_ids,
                        )
                        next_build_retry = started + STORAGE_RETRY_SECONDS
                    except BrokenProcessPool:
                        raise
                    except ChartBuildWorkerSetupError:
                        raise
                    except Exception:
                        logger.exception("Chart build failed series=%s", series_ids)
                    else:
                        if completed:
                            full_advance_requested = True
                    build_requested = True
                if started >= next_singleton_verify:
                    _verify_singleton(singleton)
                    next_singleton_verify = started + SINGLETON_VERIFY_SECONDS
                if started >= next_cleanup:
                    expired = expire_stale_series(now=timezone.now())
                    if expired:
                        logger.info("Expired %s idle chart series", expired)
                    next_cleanup = started + EXPIRED_SERIES_CLEANUP_SECONDS
                if started >= next_warm_series:
                    warmed = ensure_default_chart_series(now=timezone.now())
                    if warmed:
                        logger.info("Queued %s default chart series", warmed)
                        build_requested = True
                    next_warm_series = started + WARM_SERIES_REFRESH_SECONDS
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

                if build_requested and started >= next_build_retry:
                    build_requested = False
                    while len(builds) < BUILD_WORKERS:
                        series_ids = claim_queued_market_series()
                        if not series_ids:
                            break
                        future = build_executor.submit(
                            _run_claimed_build,
                            series_ids,
                        )
                        builds[future] = series_ids

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
            if build_stop is not None:
                build_stop.set()
            if build_executor is not None:
                build_executor.shutdown(wait=True, cancel_futures=True)
            if stores is not None:
                stores.close()
            if singleton is not None:
                singleton.close()
            close_old_connections()
            _restore_signal_handlers(previous_handlers)


def _initialize_build_worker(stop_event) -> None:
    global _BUILD_STOP
    _BUILD_STOP = stop_event
    connections.close_all()


def _build_worker_ready() -> None:
    return None


def _run_claimed_build(series_ids: tuple[int, ...]) -> bool:
    close_old_connections()
    stores = None
    try:
        try:
            stores = open_chart_stores(
                role=f"chart-build-{os.getpid()}",
                operation_timeout_seconds=BUILD_QUERY_TIMEOUT_SECONDS,
            )
        except Exception as exc:
            requeue_claimed_series(series_ids)
            if (
                classify_clickhouse_error(exc, is_write=False)
                == ClickHouseFailureKind.RETRYABLE
            ):
                raise TransientChartBuildError(
                    "chart build storage could not be opened"
                ) from exc
            raise ChartBuildWorkerSetupError(
                "chart build worker could not initialize"
            ) from exc
        return build_claimed_series(
            series_ids,
            execution_store=stores.executions,
            bar_store=stores.bars,
            should_stop=_build_stop_requested,
        )
    finally:
        if stores is not None:
            stores.close()
        close_old_connections()


def _build_stop_requested() -> bool:
    return _BUILD_STOP is not None and _BUILD_STOP.is_set()


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


def _acquire_singleton(lock_id=MAINTENANCE_ADVISORY_LOCK_ID):
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
                (lock_id,),
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
