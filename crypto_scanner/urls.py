from django.urls import path

from crypto_scanner.api import (
    pearson_correlation,
    z_score,
    average_price_per_day,
    options,
)


urlpatterns = [
    path("stats-select-options", options.get_stats_select_options),
    path("tickers-options", options.get_tickers_options),
    path(
        "average-price/<str:symbol>/<str:duration>",
        average_price_per_day.average_price_change_per_day_of_week,
        name="average-price-change-per-week",
    ),
    path(
        "pearson-correlation/<str:duration>",
        pearson_correlation.get_pearson_correlation,
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
