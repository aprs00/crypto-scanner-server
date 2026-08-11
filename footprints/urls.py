from django.urls import path

from footprints.api import markets
from footprints.chart_api import chart_bars, chart_series

urlpatterns = [
    path("markets", markets, name="footprint-markets"),
    path("chart-series", chart_series, name="chart-series"),
    path("chart-bars", chart_bars, name="chart-bars"),
]
