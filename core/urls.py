from django.urls import path, include

urlpatterns = [
    path("", include("correlations.urls")),
    path("", include("zscore.urls")),
    path("", include("filters.urls")),
    path("", include("averages.urls")),
]
