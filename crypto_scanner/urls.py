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
        "average-price-day/<str:symbol>/<str:duration>",
        average_price.average_price_change_per_day_of_week,
        name="average-price-change-per-week",
    ),
    path(
        "average-price-hour/<str:symbol>/<str:duration>",
        average_price.average_price_change_per_hour_of_day,
        name="average-price-change-per-hour",
    ),
    path(
        "pearson-correlation/<str:duration>",
        pearson.get_pearson_correlation,
        name="pearson-correlation",
    ),
    path(
        "z-score-matrix/<str:duration>",
        z_score.get_z_score_matrix,
        name="z-score-matrix",
    ),
    path(
        "z-score-history/<str:duration>/<str:type>",
        z_score.get_z_score_history,
        name="z-score-history",
    ),
]
