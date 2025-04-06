from django.urls import path

from correlations.api import get_pearson_correlation, get_large_pearson_correlation


urlpatterns = [
    path(
        "pearson-correlation",
        get_pearson_correlation,
        name="pearson-correlation",
    ),
    path(
        "large-pearson-correlation",
        get_large_pearson_correlation,
        name="large-pearson-correlation",
    ),
]
