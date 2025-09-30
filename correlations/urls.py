from django.urls import path

from correlations.api import get_pearson_correlation, get_correlation_history


urlpatterns = [
    path(
        "pearson-correlation",
        get_pearson_correlation,
        name="pearson-correlation",
    ),
    path(
        "correlation-history",
        get_correlation_history,
        name="correlation-history",
    ),
]
