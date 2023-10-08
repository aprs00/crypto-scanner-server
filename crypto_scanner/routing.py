from django.urls import re_path

from . import consumers

websocket_urlpatterns = [
    re_path(r"ws/crypto_scanner/table$", consumers.TableConsumer.as_asgi()),
]

# ws://127.0.0.1:8000/ws/crypto_scanner/table?param1=value1&param2=value2&jsonObj={'property1':'val1', 'property2':54}
