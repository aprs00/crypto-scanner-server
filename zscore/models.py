from django.db import models
from django.db.models import UniqueConstraint

from exchange_connections.models import Ticker


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
