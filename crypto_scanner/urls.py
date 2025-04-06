from django.urls import path

from crypto_scanner.api import (
    average_price,
)


urlpatterns = [
    path(
        "average-prices",
        average_price.get_average_prices,
        name="average-price-change-per-week",
    ),
]
