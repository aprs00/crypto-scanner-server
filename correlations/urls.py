from django.urls import path

from correlations.api import get_pearson_correlation, get_correlation_pair_history


urlpatterns = [
    path(
        "pearson-correlation",
        get_pearson_correlation,
        name="pearson-correlation",
    ),
    path(
        "correlation-pair-history",
        get_correlation_pair_history,
        name="correlation-pair-history",
    ),
]
