from django.urls import path

from crypto_scanner.api import (
    average_price,
    correlations,
    z_score,
    options,
)


urlpatterns = [
    path("stats-select-options", options.get_stats_select_options),
    path("tickers-options", options.get_tickers_options),
    path("pearson-type-options", options.get_large_pearson_types),
    path("pearson-time-frame-options", options.get_large_pearson_timeframes),
    path(
        "average-prices",
        average_price.get_average_prices,
        name="average-price-change-per-week",
    ),
    path(
        "pearson-correlation",
        correlations.get_pearson_correlation,
        name="pearson-correlation",
    ),
    path(
        "z-score-matrix",
        z_score.get_z_score_matrix,
        name="z-score-matrix",
    ),
    path(
        "z-score-matrix-large",
        z_score.get_large_z_score_matrix,
        name="z-score-matrix-large",
    ),
    path(
        "z-score-history",
        z_score.get_z_score_history,
        name="z-score-history",
    ),
    path(
        "z-score-heatmap",
        z_score.get_z_score_heatmap,
        name="z-score-heatmap",
    ),
    path(
        "large-pearson-correlation",
        correlations.get_large_pearson_correlation,
        name="large-pearson-correlation",
    ),
]
