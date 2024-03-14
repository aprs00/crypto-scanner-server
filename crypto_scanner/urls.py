from django.urls import path

from crypto_scanner.api import (
    average_price,
    pearson,
    z_score,
    options,
)


urlpatterns = [
    path("stats-select-options", options.get_stats_select_options),
    path("tickers-options", options.get_tickers_options),
    path(
        "average-prices",
        average_price.get_average_prices,
        name="average-price-change-per-week",
    ),
    path(
        "pearson-correlation",
        pearson.get_pearson_correlation,
        name="pearson-correlation",
    ),
    path(
        "z-score-matrix",
        z_score.get_z_score_matrix,
        name="z-score-matrix",
    ),
    path(
        "z-score-history",
        z_score.get_z_score_history,
        name="z-score-history",
    ),
    path(
        "test-redis-data",
        pearson.get_last_15_minutes_of_data,
        name="test-redis-data",
    ),
]
