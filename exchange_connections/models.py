from django.db import models
from django.db.models import UniqueConstraint


class Ticker(models.Model):
    name = models.CharField(max_length=20, null=True)
    color = models.CharField(max_length=20, default="#000000")

    def __str__(self):
        return self.name

    class Meta:
        db_table = "crypto_scanner_binance_spot_tickers"


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
        indexes = [
            models.Index(fields=["ticker", "-end_time"]),
        ]
        constraints = [
            UniqueConstraint(
                fields=["ticker", "start_time"], name="unique_ticker_start_time"
            ),
        ]

    def __str__(self):
        return (
            f"BinanceSpotKline5m("
            f"ticker={self.ticker}, "
            f"ticker_name={self.ticker_name}, "
            f"ticker_quote={self.ticker_quote}, "
            f"start_time={self.start_time}, "
            f"end_time={self.end_time}, "
            f"open={self.open}, "
            f"close={self.close}, "
            f"high={self.high}, "
            f"low={self.low}, "
            f"base_volume={self.base_volume}, "
            f"number_of_trades={self.number_of_trades}, "
            f"quote_asset_volume={self.quote_asset_volume}, "
            f"taker_buy_base_asset_volume={self.taker_buy_base_asset_volume}, "
            f"taker_buy_quote_asset_volume={self.taker_buy_quote_asset_volume}"
            f")"
        )
