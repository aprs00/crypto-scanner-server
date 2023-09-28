# import os

# from channels.auth import AuthMiddlewareStack
# from channels.routing import ProtocolTypeRouter, URLRouter
# from channels.security.websocket import AllowedHostsOriginValidator
# from django.core.asgi import get_asgi_application
# from django.urls import path

# os.environ.setdefault("DJANGO_SETTINGS_MODULE", "mysite.settings")
# # Initialize Django ASGI application early to ensure the AppRegistry
# # is populated before importing code that may import ORM models.
# django_asgi_app = get_asgi_application()

# from crypto_scanner.consumers import BinanceConsumer

# application = ProtocolTypeRouter(
#     {
#         "http": django_asgi_app,
#         "websocket": URLRouter(
#             [
#                 path("nesto/", BinanceConsumer.as_asgi()),
#             ]
#         ),
#     }
# )
