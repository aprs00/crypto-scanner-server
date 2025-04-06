from django.urls import path, include

urlpatterns = [
    path("", include("crypto_scanner.urls")),
    path("", include("correlations.urls")),
    path("", include("zscore.urls")),
]
