from django.urls import re_path

from footprints.chart_consumers import ChartConsumer

from . import consumers


websocket_urlpatterns = [
    re_path(r"ws/notifications/$", consumers.NotificationConsumer.as_asgi()),  # type: ignore[arg-type]
    re_path(r"ws/v1/charts/$", ChartConsumer.as_asgi()),  # type: ignore[arg-type]
]
