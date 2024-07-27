from django.db import models
from django.db.models import UniqueConstraint


class Ticker(models.Model):
    name = models.CharField(max_length=20, null=True)
    color = models.CharField(max_length=20, default="#000000")

    def __str__(self):
        return self.name

    class Meta:
        db_table = "crypto_scanner_binance_spot_tickers"


class ZScoreHistorical(models.Model):
    ticker_name = models.ForeignKey(Ticker, on_delete=models.CASCADE, default=1)
    ticker_quote = models.ForeignKey(
        Ticker, on_delete=models.CASCADE, related_name="zscore_quote_ticker", default=1
    )
    volume_z_score = models.FloatField()
    price_z_score = models.FloatField()
    trades_z_score = models.FloatField()
    calculated_at = models.DateTimeField(auto_now_add=True, db_index=True)

    class Meta:
        ordering = ["calculated_at"]
        db_table = "crypto_scanner_z_score_historical"
        constraints = [
            UniqueConstraint(
                fields=["ticker_name", "ticker_quote", "calculated_at"],
                name="unique_zscore_ticker_start_time",
            ),
        ]

    def __str__(self):
        return f"{self.ticker_name} - {self.calculated_at}"


class BinanceSpotKline5m(models.Model):
    ticker = models.CharField(max_length=10)
    ticker_name = models.ForeignKey(Ticker, on_delete=models.CASCADE, default=1)
    ticker_quote = models.ForeignKey(
        Ticker, on_delete=models.CASCADE, related_name="quote_ticker", default=1
    )
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
        constraints = [
            UniqueConstraint(
                fields=["ticker", "start_time"], name="unique_ticker_start_time"
            ),
        ]


class ErrorLog(models.Model):
    created = models.DateTimeField(auto_now_add=True)
    message = models.TextField()

    class Meta:
        ordering = ["-created"]
        db_table = "crypto_scanner_error_log"
