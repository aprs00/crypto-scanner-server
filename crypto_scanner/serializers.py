from rest_framework import serializers
from crypto_scanner.models import Snippet, BtcPrice


class SnippetSerializer(serializers.ModelSerializer):
    class Meta:
        model = Snippet
        fields = ["id", "title", "code", "linenos", "language", "style"]


class BtcPriceSerializer(serializers.ModelSerializer):
    class Meta:
        model = BtcPrice
        fields = ["id", "created", "price"]
