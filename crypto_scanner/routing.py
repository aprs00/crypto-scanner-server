from django.urls import re_path

from . import consumers

websocket_urlpatterns = [
    re_path(r"ws/crypto_scanner/$", consumers.BinanceConsumer.as_asgi()),
]
