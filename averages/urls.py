from django.urls import path

from averages.api import (
    get_average_prices,
)


urlpatterns = [
    path(
        "average-prices",
        get_average_prices,
        name="average-price-change-per-week",
    ),
]
