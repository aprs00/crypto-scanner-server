from django.db import models
from pygments.lexers import get_all_lexers
from pygments.styles import get_all_styles

LEXERS = [item for item in get_all_lexers() if item[1]]
LANGUAGE_CHOICES = sorted([(item[1][0], item[0]) for item in LEXERS])
STYLE_CHOICES = sorted([(item, item) for item in get_all_styles()])


class BtcPrice(models.Model):
    id = models.AutoField(primary_key=True)
    created = models.DateTimeField(auto_now_add=True)
    price = models.DecimalField(max_digits=10, decimal_places=2)

    class Meta:
        ordering = ["created"]


class BinanceSpotKline1m(models.Model):
    ticker = models.CharField(max_length=10)
    start_time = models.DateTimeField()
    end_time = models.DateTimeField()
    open = models.DecimalField(max_digits=24, decimal_places=10)
    close = models.DecimalField(max_digits=24, decimal_places=10)
    high = models.DecimalField(max_digits=24, decimal_places=10)
    low = models.DecimalField(max_digits=24, decimal_places=10)
    base_volume = models.DecimalField(max_digits=24, decimal_places=10)
    number_of_trades = models.IntegerField()
    quote_asset_volume = models.DecimalField(max_digits=24, decimal_places=10)
    taker_buy_base_asset_volume = models.DecimalField(max_digits=24, decimal_places=10)
    taker_buy_quote_asset_volume = models.DecimalField(max_digits=24, decimal_places=10)

    class Meta:
        ordering = ["start_time"]
        db_table = "crypto_scanner_binance_spot_kline_1m"


class BinanceSpotKline5m(models.Model):
    ticker = models.CharField(max_length=10)
    start_time = models.DateTimeField()
    end_time = models.DateTimeField()
    open = models.DecimalField(max_digits=24, decimal_places=10)
    close = models.DecimalField(max_digits=24, decimal_places=10)
    high = models.DecimalField(max_digits=24, decimal_places=10)
    low = models.DecimalField(max_digits=24, decimal_places=10)
    base_volume = models.DecimalField(max_digits=24, decimal_places=10)
    number_of_trades = models.IntegerField()
    quote_asset_volume = models.DecimalField(max_digits=24, decimal_places=10)
    taker_buy_base_asset_volume = models.DecimalField(max_digits=24, decimal_places=10)
    taker_buy_quote_asset_volume = models.DecimalField(max_digits=24, decimal_places=10)

    class Meta:
        ordering = ["start_time"]
        db_table = "crypto_scanner_binance_spot_kline_5m"
