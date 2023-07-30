from rest_framework import serializers
from crypto_scanner.models import Snippet, BtcPrice, BinanceSpotKline5m


class SnippetSerializer(serializers.ModelSerializer):
    class Meta:
        model = Snippet
        fields = ["id", "title", "code", "linenos", "language", "style"]


class BtcPriceSerializer(serializers.ModelSerializer):
    class Meta:
        model = BtcPrice
        fields = ["id", "created", "price"]


class BinanceSpotKline5mSerializer(serializers.ModelSerializer):
    class Meta:
        model = BinanceSpotKline5m
        fields = "__all__"
