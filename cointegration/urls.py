from django.urls import path

from cointegration.api import get_cointegration_live_table, get_cointegration_pair_history


urlpatterns = [
    path(
        "cointegration-live-table",
        get_cointegration_live_table,
        name="cointegration-live-table",
    ),
    path(
        "cointegration-pair-history",
        get_cointegration_pair_history,
        name="cointegration-pair-history",
    ),
]
