"""Small durable execution outbox. Only the exchange's elected writer opens it."""

from __future__ import annotations

import fcntl
import json
import shutil
import sqlite3
import threading
import time
from dataclasses import asdict
from decimal import Decimal
from pathlib import Path

from footprints.storage.clickhouse import RecentExecutionIdentity
from footprints.storage.executions import StoredExecution
from footprints.types import NormalizedTrade, TakerSide


def encode_execution(execution: StoredExecution) -> dict:
    value = asdict(execution)
    value["trade"]["price"] = str(execution.trade.price)
    value["trade"]["quantity"] = str(execution.trade.quantity)
    value["trade"]["taker_side"] = execution.trade.taker_side.value
    return value


def decode_execution(value: dict) -> StoredExecution:
    value = dict(value)
    trade = dict(value.pop("trade"))
    trade["price"] = Decimal(trade["price"])
    trade["quantity"] = Decimal(trade["quantity"])
    trade["taker_side"] = TakerSide(trade["taker_side"])
    return StoredExecution(**value, trade=NormalizedTrade(**trade))


class ExecutionOutbox:
    def __init__(self, directory, *, max_bytes=2 * 1024**3, reserve_bytes=5 * 1024**3):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.path = self.directory / "outbox.sqlite3"
        # Fence a process whose PG lease was lost with a CH request in flight.
        self._lease = (self.directory / "writer.lock").open("a")
        try:
            fcntl.flock(self._lease, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            self._lease.close()
            raise
        self.max_bytes = max_bytes
        self.reserve_bytes = reserve_bytes
        self.lock = threading.RLock()
        self.db = sqlite3.connect(self.path, timeout=10, check_same_thread=False)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS batches (
                id INTEGER PRIMARY KEY AUTOINCREMENT, payload TEXT NOT NULL,
                rows INTEGER NOT NULL, bytes INTEGER NOT NULL, created REAL NOT NULL);
            CREATE TABLE IF NOT EXISTS recent (
                id INTEGER PRIMARY KEY AUTOINCREMENT, market_id INTEGER NOT NULL,
                trade_id TEXT NOT NULL, payload TEXT NOT NULL,
                UNIQUE(market_id, trade_id));
            CREATE TABLE IF NOT EXISTS state (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE INDEX IF NOT EXISTS recent_market_order ON recent(market_id,id);
        """)

    def state(self, key, default=None):
        with self.lock:
            row = self.db.execute(
                "SELECT value FROM state WHERE key=?", (key,)
            ).fetchone()
            return json.loads(row[0]) if row else default

    def set_state(self, key, value):
        with self.lock, self.db:
            self.db.execute(
                "INSERT OR REPLACE INTO state VALUES (?, ?)", (key, json.dumps(value))
            )

    def seed(self, epoch, recent):
        with self.lock, self.db:
            for rows in recent.values():
                for row in reversed(rows):
                    self._remember(row)
            self.db.execute(
                "INSERT OR REPLACE INTO state VALUES ('epoch', ?)", (json.dumps(epoch),)
            )

    def _remember(self, row):
        self.db.execute(
            "INSERT OR REPLACE INTO recent(market_id,trade_id,payload) VALUES (?,?,?)",
            (row.market_id, row.trade_id, json.dumps(asdict(row))),
        )

    def recent(self):
        result = {}
        with self.lock:
            for (payload,) in self.db.execute(
                "SELECT payload FROM recent ORDER BY id DESC"
            ):
                row = RecentExecutionIdentity(**json.loads(payload))
                result.setdefault(row.market_id, []).append(row)
        return {key: tuple(rows) for key, rows in result.items()}

    def append(self, executions):
        payload = json.dumps(
            [encode_execution(row) for row in executions], separators=(",", ":")
        )
        with self.lock:
            size = sum(p.stat().st_size for p in self.directory.glob("outbox.sqlite3*"))
            # SQLite reuses freed pages, so limit the outstanding payload, too.
            pending = self.db.execute(
                "SELECT coalesce(sum(bytes),0) FROM batches"
            ).fetchone()[0]
            if (
                pending + len(payload) > self.max_bytes
                or shutil.disk_usage(self.directory).free < self.reserve_bytes
            ):
                raise OSError(
                    "footprint outbox storage limit reached; capture must stop"
                )
            if size > self.max_bytes * 2:
                self.db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            with self.db:
                self.db.execute(
                    "INSERT INTO batches(payload,rows,bytes,created) VALUES (?,?,?,?)",
                    (payload, len(executions), len(payload), time.time()),
                )
                for execution in executions:
                    self._remember(
                        RecentExecutionIdentity(
                            execution.market_id,
                            execution.trade.trade_id,
                            execution.trade.exchange_timestamp_us,
                            execution.price_atoms,
                            execution.quantity_atoms,
                            execution.trade.taker_side == TakerSide.BUY,
                            execution.instrument_spec_id,
                        )
                    )
                for market_id in {e.market_id for e in executions}:
                    self.db.execute(
                        """DELETE FROM recent WHERE market_id=? AND id <
                        (SELECT id FROM recent WHERE market_id=? ORDER BY id DESC LIMIT 1 OFFSET 49999)""",
                        (market_id, market_id),
                    )

    def first(self):
        with self.lock:
            row = self.db.execute(
                "SELECT id,payload FROM batches ORDER BY id LIMIT 1"
            ).fetchone()
            return (
                (row[0], tuple(decode_execution(v) for v in json.loads(row[1])))
                if row
                else None
            )

    def acknowledge(self, batch_id):
        with self.lock, self.db:
            self.db.execute("DELETE FROM batches WHERE id=?", (batch_id,))
            self.db.execute(
                "INSERT OR REPLACE INTO state VALUES ('last_ack',?)",
                (json.dumps(time.time()),),
            )

    def stats(self):
        with self.lock:
            rows, size, oldest = self.db.execute(
                "SELECT coalesce(sum(rows),0),coalesce(sum(bytes),0),min(created) FROM batches"
            ).fetchone()
        return {
            "pending_rows": rows,
            "pending_bytes": size,
            "oldest_pending_at": oldest,
        }

    def close(self):
        with self.lock:
            self.db.close()
            self._lease.close()
