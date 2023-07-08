from django.urls import path, include

urlpatterns = [
    path("", include("crypto_scanner.urls")),
]
