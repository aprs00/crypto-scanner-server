from rest_framework import serializers
from crypto_scanner.models import BtcPrice, BinanceSpotKline5m


class BtcPriceSerializer(serializers.ModelSerializer):
    class Meta:
        model = BtcPrice
        fields = ["id", "created", "price"]


class BinanceSpotKline5mSerializer(serializers.ModelSerializer):
    class Meta:
        model = BinanceSpotKline5m
        fields = "__all__"
