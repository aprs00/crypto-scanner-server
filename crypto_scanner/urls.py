from django.urls import path
from crypto_scanner import views

urlpatterns = [
    path("snippets/", views.snippet_list),
    path("snippets/<int:pk>/", views.snippet_detail),
    path(
        "average-price/<str:symbol>/<str:duration>/",
        views.average_price_change_per_day_of_week,
        name="average-price-change-per-week",
    ),
    path("average-price/select/", views.average_price_change_per_day_of_week_select),
]
