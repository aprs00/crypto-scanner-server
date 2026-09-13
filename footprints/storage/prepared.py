"""Shared, disposable TREV input tapes; source executions remain authoritative.

Published file prefixes are immutable so readers can map them safely. Missing,
stale or damaged cache data falls back to the fixed source query. This cache
contains no chart configuration, chart state, or user-visible candles.
"""

import fcntl
import hashlib
import json
import logging
import os
import re
import time
import uuid
from contextlib import closing
from concurrent.futures import ThreadPoolExecutor
from itertools import islice
from pathlib import Path

import lz4.frame
from django.conf import settings

from footprints.storage.native import NativeBatch

logger = logging.getLogger(__name__)
FILE_BYTES = 128 * 1024 * 1024
DAY_US = 86_400_000_000


def _json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _context(store, market_id):
    fixed = getattr(store, "_fixed", {})
    imports = fixed.get(market_id)
    return {
        "host": os.environ.get("CLICKHOUSE_HOST", "clickhouse"),
        "table": store.qualified_table,
        "imports": {str(k): v for k, v in imports.items()}
        if imports is not None
        else None,
        "specs": {
            str(k): [v.price_atoms_per_tick, v.quantity_atoms_per_unit]
            for k, v in store.spec_divisors.items()
        },
    }


def _compatible(previous, current):
    return all(
        previous.get(k) == current[k] for k in ("host", "table", "imports")
    ) and all(
        current["specs"].get(k) == v for k, v in previous.get("specs", {}).items()
    )


def _read_index(directory):
    envelope = json.loads((directory / "index.json").read_text())
    payload = envelope["payload"]
    if (
        payload["version"] != 1
        or hashlib.sha256(_json(payload).encode()).hexdigest() != envelope["sha256"]
    ):
        raise ValueError("prepared manifest checksum/version mismatch")
    return payload


def _write_index(directory, payload):
    envelope = {
        "payload": payload,
        "sha256": hashlib.sha256(_json(payload).encode()).hexdigest(),
    }
    temporary = directory / f"{uuid.uuid4().hex}.json.tmp"
    try:
        temporary.write_text(_json(envelope))
        temporary.replace(directory / "index.json")
    finally:
        temporary.unlink(missing_ok=True)


def _file(directory, name, suffix):
    if not isinstance(name, str) or not re.fullmatch(r"[a-f0-9]{32}", name):
        raise ValueError("invalid prepared file name")
    return directory / (name + suffix)


def _invalidate(directory, generation):
    try:
        _file(directory, generation, ".invalid").touch()
    except OSError:
        pass


def _cursor(value):
    return tuple(value) if value is not None else None


def _prefetch(frames, load):
    """Verify bounded groups ahead without moving source queries to a thread."""
    executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="trev-input")

    def load_group(group):
        return [load(frame) for frame in group]

    try:
        frames = iter(frames)
        group = tuple(islice(frames, 32))
        future = executor.submit(load_group, group) if group else None
        while group:
            following = tuple(islice(frames, 32))
            pending = executor.submit(load_group, following) if following else None
            for offset, frame in enumerate(group):
                yield frame, future, offset
            group, future = following, pending
    finally:
        executor.shutdown(wait=True, cancel_futures=True)


class PreparedExecutions:
    """Native runs with lazily loaded resume identities and exact raw fallback."""

    def __init__(self, native, directory, frame, store, kwargs, generation):
        self.native = native
        self.directory = directory
        self.frame = frame
        self.store = store
        self.kwargs = kwargs
        self._raw = None
        self.generation = generation

    def __len__(self):
        return len(self.native)

    def __getattr__(self, name):
        return getattr(self.native, name)

    def process(self, builder, *args):
        from _trev_native import DataError, PreparedCursorRequired, PreparedFallback

        try:
            try:
                return builder.process_prepared(self.native, *args)
            except PreparedCursorRequired:
                with _file(self.directory, self.frame["file"], ".cursors").open(
                    "rb"
                ) as source:
                    source.seek(self.frame["cursor_offset"])
                    encoded = source.read(self.frame["cursor_length"])
                self.native.load_cursors(lz4.frame.decompress(encoded))
                return builder.process_prepared(self.native, *args)
        except PreparedFallback:
            return builder.process(self.raw_batch(), *args)
        except (DataError, OSError, RuntimeError):
            _invalidate(self.directory, self.generation)
            return builder.process(self.raw_batch(), *args)

    def raw_batch(self):
        from _trev_native import ExecutionBatch

        if self._raw is None:
            kwargs = dict(
                self.kwargs,
                after_cursor=_cursor(self.frame["previous"]),
                through_cursor=_cursor(self.frame["last"]),
            )
            with closing(self.store.iter_native_batches(**kwargs)) as batches:
                self._raw = ExecutionBatch.combine([batch.data for batch in batches])
            if len(self._raw) != len(self):
                raise ValueError("prepared fallback execution count changed")
        return self._raw

    def executions(self, start):
        # Used only when the existing wide-integer fallback selects Python.
        return self.raw_batch().executions(start)


def iter_prepared_batches(store, *, directory, **kwargs):
    """Use prepared interiors and query raw data at anchors, resumes and the tail."""
    from _trev_native import PreparedTape

    directory = Path(directory) / str(kwargs["market_id"])
    current = kwargs.get("after_cursor")
    through = kwargs["through_cursor"]
    anchor = kwargs["start_timestamp_us"]
    mappings = {}

    def raw(until):
        return store.iter_native_batches(
            **dict(kwargs, after_cursor=current, through_cursor=until)
        )

    try:
        index = _read_index(directory)
        if not _compatible(index["context"], _context(store, kwargs["market_id"])):
            raise ValueError("prepared source snapshot changed")
        frames = index["frames"]
        if anchor < index["anchor"] and (
            current is None
            or not frames
            or current < _cursor(frames[0]["previous"] or frames[0]["first"])
        ):
            raise ValueError("prepared history does not cover the chart origin")
        frames = [
            frame
            for frame in frames
            if (current is None or _cursor(frame["last"]) > current)
            and through is not None
            and _cursor(frame["first"]) <= through
            and frame["maximum"] >= anchor
        ]
    except (OSError, ValueError, KeyError, TypeError):
        with closing(raw(through)) as batches:
            yield from batches
        return

    def load(frame):
        # Boundary frames use the original filtered query, including all trade
        # timestamps. Only complete interiors can use same-price runs.
        if frame["minimum"] < anchor or _cursor(frame["last"]) > through:
            return None
        name = frame["file"]
        if name not in mappings:
            mappings[name] = PreparedTape(str(_file(directory, name, ".core")))
        native = mappings[name].batch(
            frame["offset"], frame["length"], frame["checksum"]
        )
        if (
            len(native) != frame["count"]
            or native.last_cursor != _cursor(frame["last"])
            or native.previous_cursor != _cursor(frame["previous"])
            or native.first_cursor != _cursor(frame["first"])
        ):
            raise ValueError("prepared frame metadata mismatch")
        return native

    # All ClickHouse reads and writes stay on the caller's thread. The worker
    # only maps immutable file prefixes and checks their integrity.
    try:
        with closing(_prefetch(frames, load)) as prefetched:
            for frame, future, offset in prefetched:
                last = _cursor(frame["last"])
                until = min(last, through)
                if (
                    current != _cursor(frame["previous"])
                    or frame["minimum"] < anchor
                    or last > through
                ):
                    with closing(raw(until)) as batches:
                        for batch in batches:
                            current = batch.data.last_cursor
                            yield batch
                    if until == through:
                        return
                    continue
                try:
                    data = PreparedExecutions(
                        future.result()[offset],
                        directory,
                        frame,
                        store,
                        kwargs,
                        index["generation"],
                    )
                except (OSError, ValueError, KeyError, TypeError):
                    logger.warning(
                        "Prepared TREV input unavailable; resuming raw replay market_id=%s",
                        kwargs["market_id"],
                    )
                    _invalidate(directory, index["generation"])
                    with closing(raw(through)) as batches:
                        yield from batches
                    return
                yield NativeBatch(data, frame["seam"])
                current = last
                if current == through:
                    return
        if through is not None and (current is None or current < through):
            with closing(raw(through)) as batches:
                yield from batches
    finally:
        mappings.clear()


def prepare_market(store, *, market_id, directory, now_us, should_stop=lambda: False):
    """Append one fixed source snapshot, publishing only complete cache frames."""
    from _trev_native import PreparedBatch

    directory = Path(directory) / str(market_id)
    directory.mkdir(parents=True, exist_ok=True)
    with (directory / "writer.lock").open("ab") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return False
        # Prepare a small margin before the canonical hourly 30-day origin.
        anchor = (now_us // 3_600_000_000 - 2) * 3_600_000_000 - 30 * DAY_US
        through = store.latest_cursor(market_id=market_id, start_timestamp_us=anchor)
        context = _context(store, market_id)
        try:
            index = _read_index(directory)
            if (
                _file(directory, index["generation"], ".invalid").exists()
                or not _compatible(index["context"], context)
                or (through is not None and _cursor(index["through"]) > through)
            ):
                raise ValueError("prepared source changed")
        except (OSError, ValueError, KeyError, TypeError):
            index = {
                "version": 1,
                "generation": uuid.uuid4().hex,
                "anchor": anchor,
                "through": None,
                "context": context,
                "frames": [],
            }
        after = _cursor(index["through"])
        if through is None or through == after:
            return False
        frames = index["frames"]
        name = frames[-1]["file"] if frames else uuid.uuid4().hex
        core = cursors = None
        started = time.monotonic()
        count = 0
        try:
            with closing(
                store.iter_native_batches(
                    market_id=market_id,
                    start_timestamp_us=anchor,
                    after_cursor=after,
                    through_cursor=through,
                    batch_size=100_000,
                )
            ) as batches:
                for batch in batches:
                    if should_stop():
                        return False
                    if core is None or core.tell() >= FILE_BYTES:
                        if core is not None:
                            core.close()
                            cursors.close()
                            name = uuid.uuid4().hex
                        elif (
                            _file(directory, name, ".core").exists()
                            and _file(directory, name, ".core").stat().st_size
                            >= FILE_BYTES
                        ):
                            name = uuid.uuid4().hex
                        core = _file(directory, name, ".core").open("ab")
                        cursors = _file(directory, name, ".cursors").open("ab")
                    raw = PreparedBatch(batch.data.prepared_bytes()).compact_bytes()
                    parsed = PreparedBatch(raw)
                    body, identities = (
                        raw[: parsed.records_end],
                        raw[parsed.records_end :],
                    )
                    compressed = lz4.frame.compress(identities, content_checksum=True)
                    minimum, maximum = batch.data.timestamp_bounds()
                    frame = dict(
                        file=name,
                        offset=core.tell(),
                        length=len(body),
                        cursor_offset=cursors.tell(),
                        cursor_length=len(compressed),
                        checksum=PreparedBatch(body).checksum(),
                        count=len(batch),
                        previous=batch.data.previous_cursor,
                        first=batch.data.first_cursor,
                        last=batch.data.last_cursor,
                        minimum=minimum,
                        maximum=maximum,
                        seam=batch.seam_break,
                    )
                    core.write(body)
                    cursors.write(compressed)
                    frames.append(frame)
                    count += len(batch)
            if not frames or _cursor(frames[-1]["last"]) != through:
                raise ValueError("prepared source ended before its fixed cursor")
            core.flush()
            cursors.flush()
            index["through"] = through
            index["context"] = _context(store, market_id)
            # Keep the same physical retention grace as authoritative source data.
            cutoff = now_us - 31 * DAY_US
            index["frames"] = [frame for frame in frames if frame["maximum"] >= cutoff]
            index["anchor"] = max(index["anchor"], cutoff)
            _write_index(directory, index)
            retained = {frame["file"] for frame in index["frames"]}
            for path in directory.glob("*.core"):
                if (
                    path.stem not in retained
                    and time.time() - path.stat().st_mtime > 600
                ):
                    path.unlink(missing_ok=True)
                    path.with_suffix(".cursors").unlink(missing_ok=True)
            logger.log(
                logging.INFO if count >= 100_000 else logging.DEBUG,
                "Prepared TREV input market_id=%s executions=%s elapsed_seconds=%.3f",
                market_id,
                count,
                time.monotonic() - started,
            )
            return True
        finally:
            if core is not None:
                core.close()
                cursors.close()


def run_preparation(stop):
    """One coordinator thread prepares all markets independently of chart builds."""
    from django.db import close_old_connections, connections
    from django.utils import timezone
    from footprints.chart_service import open_chart_stores
    from footprints.models import FootprintMarket

    directory = getattr(settings, "TREV_PREPARED_DIR", "")
    stores = None
    try:
        while not stop.is_set():
            try:
                close_old_connections()
                if stores is None:
                    stores = open_chart_stores(
                        role="trev-input-preparation", operation_timeout_seconds=600
                    )
                for market_id in FootprintMarket.objects.filter(
                    enabled=True
                ).values_list("pk", flat=True):
                    if stop.is_set():
                        break
                    prepare_market(
                        stores.executions,
                        market_id=market_id,
                        directory=directory,
                        now_us=int(timezone.now().timestamp() * 1_000_000),
                        should_stop=stop.is_set,
                    )
            except Exception:
                logger.exception(
                    "TREV input preparation failed; raw replay remains available"
                )
                if stores is not None:
                    stores.close()
                    stores = None
                close_old_connections()
                if stop.wait(5):
                    break
            # Keep the manifest bounded instead of publishing a tiny frame for
            # every collector flush. Builds query the recent raw tail directly.
            stop.wait(300)
    finally:
        if stores is not None:
            stores.close()
        connections.close_all()
