from __future__ import annotations

import logging
import threading


logger = logging.getLogger(__name__)


def close_stream_on_error(stop_event: threading.Event):
    def handle_error(websocket_app, error) -> None:
        if not stop_event.is_set():
            logger.warning("Execution WebSocket stream error: %s", error)
        stop_event.set()
        websocket_app.close()

    return handle_error


def run_websocket(websocket_app, stop_event: threading.Event, **options) -> None:
    finished = threading.Event()

    def close_when_stopped() -> None:
        while not finished.wait(0.25):
            if stop_event.is_set():
                websocket_app.close()
                return

    closer = threading.Thread(target=close_when_stopped, daemon=True)
    closer.start()
    try:
        websocket_app.run_forever(**options)
    finally:
        finished.set()
        closer.join(timeout=1)
