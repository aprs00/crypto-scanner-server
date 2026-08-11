from django.urls import path, include

from core.api import bootstrap

urlpatterns = [
    path("api/v1/footprints/", include("footprints.urls")),
    path("", include("correlations.urls")),
    path("", include("zscore.urls")),
    path("", include("averages.urls")),
    path("", include("cointegration.urls")),
    path("bootstrap", bootstrap),
]
