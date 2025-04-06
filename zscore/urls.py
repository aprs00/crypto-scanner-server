from django.urls import path

from zscore.api import (
    get_z_score_matrix,
    get_large_z_score_matrix,
    get_z_score_history,
    get_z_score_heatmap,
)


urlpatterns = [
    path(
        "z-score-matrix",
        get_z_score_matrix,
        name="z-score-matrix",
    ),
    path(
        "z-score-matrix-large",
        get_large_z_score_matrix,
        name="z-score-matrix-large",
    ),
    path(
        "z-score-history",
        get_z_score_history,
        name="z-score-history",
    ),
    path(
        "z-score-heatmap",
        get_z_score_heatmap,
        name="z-score-heatmap",
    ),
]
