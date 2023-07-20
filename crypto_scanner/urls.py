from django.urls import path
from crypto_scanner import views

urlpatterns = [
    path("snippets/", views.snippet_list),
    path("snippets/<int:pk>/", views.snippet_detail),
    path("average-price/<str:symbol>/<str:duration>/", views.average_price),
]
