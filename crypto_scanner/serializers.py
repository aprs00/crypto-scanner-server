from rest_framework import serializers
from crypto_scanner.models import BinanceSpotKline5m


class BinanceSpotKline5mSerializer(serializers.ModelSerializer):
    class Meta:
        model = BinanceSpotKline5m
        fields = "__all__"
