from django.urls import path, include

from core.api import bootstrap

urlpatterns = [
    path("", include("correlations.urls")),
    path("", include("zscore.urls")),
    path("", include("averages.urls")),
    path("bootstrap", bootstrap),
]
