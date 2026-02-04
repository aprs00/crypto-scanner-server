from django.urls import path

from cointegration.api import get_cointegration_live_table


urlpatterns = [
    path(
        "cointegration-live-table",
        get_cointegration_live_table,
        name="cointegration-live-table",
    ),
]
