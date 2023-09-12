from django.urls import path
from crypto_scanner import views

urlpatterns = [
    path("stats-select-options", views.get_stats_select_options),
    path(
        "average-price/<str:symbol>/<str:duration>",
        views.average_price_change_per_day_of_week,
        name="average-price-change-per-week",
    ),
    path(
        "pearson-correlation/<str:duration>",
        views.get_pearson_correlation,
        name="pearson-correlation",
    ),
]
