from datetime import timedelta
from django.db import models
from django.db.models import F, Func, DateTimeField
from django.db.models.functions import ExtractWeekDay
from rest_framework import serializers
from crypto_scanner.models import Snippet, BtcPrice, BinanceSpotKline1m
import numpy as np


class SnippetSerializer(serializers.ModelSerializer):
    class Meta:
        model = Snippet
        fields = ["id", "title", "code", "linenos", "language", "style"]


class BtcPriceSerializer(serializers.ModelSerializer):
    class Meta:
        model = BtcPrice
        fields = ["id", "created", "price"]


class BinanceSpotKline1mSerializer(serializers.ModelSerializer):
    class Meta:
        model = BinanceSpotKline1m
        fields = ("ticker",)

    def to_representation(self, instance):
        six_months_ago = instance.start_time - timedelta(days=180)

        # get data from database
        # data = (
        #     BinanceSpotKline1m.objects.filter(
        #         ticker=instance.ticker, start_time__gte=six_months_ago
        #     )
        #     .annotate(day_of_week=DayOfWeek("start_time"))
        #     .values("day_of_week")
        #     .annotate(
        #         open=models.Avg("open"),
        #         close=models.Avg("close"),
        #         high=models.Avg("high"),
        #         low=models.Avg("low"),
        #     )
        #     .order_by("day_of_week")
        # )

        data = BinanceSpotKline1m.objects.annotate(
            day_of_week=ExtractWeekDay("start_time"),
        )

        # weekly_sell = Sell.objects.annotate(
        #     weekday=ExtractWeekDay('date'),
        # ).values(
        #     'weekday',
        # ).annotate(
        #      total=Sum('total_sell')
        # ).values(
        #      'weekday',
        #      'total'
        # )

        print(data.__dict__)
        # convert to numpy array
        data = np.array(list(data))

        # convert to list of lists
        data = data.tolist()

        # convert to json
        return data
