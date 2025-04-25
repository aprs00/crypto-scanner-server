from django.urls import path

from filters.api import (
    get_stats_select_options,
    get_tickers_options,
    get_large_pearson_types,
)


urlpatterns = [
    path("stats-select-options", get_stats_select_options),
    path("tickers-options", get_tickers_options),
    path("pearson-type-options", get_large_pearson_types),
]
