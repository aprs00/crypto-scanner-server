from __future__ import annotations

from asgiref.sync import async_to_sync
from channels.layers import get_channel_layer


def chart_group_name(series_key: str) -> str:
    if not series_key:
        raise ValueError("chart series key is required")
    # UUID-backed public keys keep this below the Channels group-name limit.
    return f"chart.{series_key}"


def publish_chart_update(
    series_key: str,
    *,
    bars: list[dict],
    active_bar: dict | None,
) -> None:
    """Publish immutable closed bars and an absolute active-bar replacement."""

    if not bars and active_bar is None:
        return
    channel_layer = get_channel_layer()
    if channel_layer is None:
        raise RuntimeError("chart channel layer is unavailable")
    async_to_sync(channel_layer.group_send)(
        chart_group_name(series_key),
        {
            "type": "chart.update",
            "series_key": series_key,
            "bars": bars,
            "active_bar": active_bar,
        },
    )


def publish_chart_error(series_key: str, code: str) -> None:
    channel_layer = get_channel_layer()
    if channel_layer is None:
        raise RuntimeError("chart channel layer is unavailable")
    async_to_sync(channel_layer.group_send)(
        chart_group_name(series_key),
        {
            "type": "chart.error",
            "series_key": series_key,
            "code": code,
        },
    )
