from django.urls import path
from crypto_scanner import views

urlpatterns = [
    path("snippets", views.snippet_list),
    path("snippets/<int:pk>", views.snippet_detail),
    path(
        "average-price/<str:symbol>/<str:duration>",
        views.average_price_change_per_day_of_week,
        name="average-price-change-per-week",
    ),
    path("stats-select-options", views.get_stats_select_options),
    path(
        "pearson-correlation/<str:duration>",
        views.get_pearson_correlation,
        name="pearson-correlation",
    ),
]
