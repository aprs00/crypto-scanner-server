from __future__ import annotations

from channels.db import database_sync_to_async
from channels.generic.websocket import AsyncJsonWebsocketConsumer
from django.core.exceptions import ValidationError
from django.utils import timezone

from footprints.chart_live import chart_group_name
from footprints.chart_service import refresh_series_lease


MAX_CHART_SUBSCRIPTIONS = 16
MAX_CHART_FRAME_BYTES = 2 * 1_024


class ChartConsumer(AsyncJsonWebsocketConsumer):
    """Small absolute-state WebSocket for tick and TRev series."""

    async def connect(self) -> None:
        self.subscriptions: dict[str, str] = {}
        await self.accept()

    async def receive(self, text_data=None, bytes_data=None) -> None:
        if bytes_data is not None or text_data is None:
            await self.close(code=4400)
            return
        if (
            len(text_data) > MAX_CHART_FRAME_BYTES
            or len(text_data.encode("utf-8")) > MAX_CHART_FRAME_BYTES
        ):
            await self.close(code=4400)
            return
        await super().receive(text_data=text_data)

    async def disconnect(self, _code: int) -> None:
        for group in self.subscriptions.values():
            await self.channel_layer.group_discard(group, self.channel_name)
        self.subscriptions.clear()

    async def receive_json(self, content, **_kwargs) -> None:
        if not isinstance(content, dict):
            await self.close(code=4400)
            return
        operation = content.get("op")
        series_key = content.get("seriesKey")
        if not isinstance(series_key, str) or not series_key:
            await self.close(code=4400)
            return
        if operation == "subscribe":
            await self._subscribe(series_key)
        elif operation == "unsubscribe":
            await self._unsubscribe(series_key)
        else:
            await self.send_json(
                {
                    "type": "error",
                    "code": "UNKNOWN_OPERATION",
                    "seriesKey": series_key,
                }
            )

    async def _subscribe(self, series_key: str) -> None:
        series = await database_sync_to_async(_series_status)(series_key)
        if series is None:
            await self.send_json(
                {
                    "type": "error",
                    "code": "UNKNOWN_CHART_SERIES",
                    "seriesKey": series_key,
                }
            )
            return
        status = series
        # Another panel can request catch-up between HTTP and subscription.
        # The group still publishes only committed updates during catch-up.
        if status not in ("ready", "catching_up"):
            await self.send_json(
                {
                    "type": "error",
                    "code": "CHART_SERIES_NOT_READY",
                    "seriesKey": series_key,
                }
            )
            return
        if series_key in self.subscriptions:
            await self._ack(series_key)
            return
        if len(self.subscriptions) >= MAX_CHART_SUBSCRIPTIONS:
            await self.send_json(
                {
                    "type": "error",
                    "code": "TOO_MANY_CHART_SUBSCRIPTIONS",
                    "seriesKey": series_key,
                }
            )
            return
        group = chart_group_name(series_key)
        # Register before acknowledging so the client can refetch HTTP after
        # the ack without an HTTP-to-WebSocket loss window.
        await self.channel_layer.group_add(group, self.channel_name)
        self.subscriptions[series_key] = group
        await self._ack(series_key)

    async def _unsubscribe(self, series_key: str) -> None:
        group = self.subscriptions.pop(series_key, None)
        if group is not None:
            await self.channel_layer.group_discard(group, self.channel_name)

    async def chart_update(self, event: dict) -> None:
        series_key = str(event.get("series_key", ""))
        if series_key not in self.subscriptions:
            return
        await self.send_json(
            {
                "type": "update",
                "seriesKey": series_key,
                "bars": event.get("bars", []),
                "activeBar": event.get("active_bar"),
            }
        )

    async def chart_error(self, event: dict) -> None:
        series_key = str(event.get("series_key", ""))
        if series_key not in self.subscriptions:
            return
        await self.send_json(
            {
                "type": "error",
                "code": str(event.get("code", "CHART_SERIES_FAILED")),
                "seriesKey": series_key,
            }
        )
        if event.get("code") == "CHART_SERIES_REPLACED":
            # A late old-worker publication must not resurrect the retired feed.
            await self._unsubscribe(series_key)

    async def _ack(self, series_key: str) -> None:
        await self.send_json(
            {
                "type": "subscribed",
                "seriesKey": series_key,
            }
        )


def _series_status(series_key: str) -> str | None:
    try:
        return refresh_series_lease(series_key, now=timezone.now())
    except (ValidationError, ValueError):
        return None
