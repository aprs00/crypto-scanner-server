"""Contain malformed public events, retaining bounded diagnostic evidence."""

import json
import logging
import logging.handlers
import os
import threading
import time
from pathlib import Path

logger = logging.getLogger(__name__)
_lock = threading.Lock()
_handlers = {}
_counts = {}


def rejection_stats(exchange):
    with _lock:
        return dict(
            _counts.get(exchange, {"rejected_events": 0, "last_rejected_at": None})
        )


def reject(exchange, payload, error):
    encoded = json.dumps(payload, default=str)
    if len(encoded) > 16384:
        payload = {"truncated": True, "prefix": encoded[:16384]}
    record = {
        "exchange": exchange,
        "at": time.time(),
        "reason": str(error),
        "payload": payload,
    }
    logger.warning("Rejected %s execution event: %s", exchange, error)
    with _lock:
        previous = _counts.get(exchange, {})
        _counts[exchange] = {
            "rejected_events": previous.get("rejected_events", 0) + 1,
            "last_rejected_at": record["at"],
        }
    directory = os.environ.get("FOOTPRINT_OUTBOX_DIR")
    if directory:
        path = Path(directory)
        path.mkdir(parents=True, exist_ok=True)
        with _lock:
            handler = _handlers.get(str(path))
            if handler is None:
                handler = logging.handlers.RotatingFileHandler(
                    path / "rejected.jsonl", maxBytes=10 * 1024**2, backupCount=2
                )
                _handlers[str(path)] = handler
            handler.emit(
                logging.LogRecord(
                    "rejected", logging.WARNING, "", 0, json.dumps(record), (), None
                )
            )
    # Counts remain observable even if the logging handler cannot retain evidence.
