from django.db import models
from django.db.models import UniqueConstraint

from exchange_connections.models import Symbol


class ZScoreHistory(models.Model):
    symbol = models.ForeignKey(Symbol, on_delete=models.CASCADE, db_index=True)
    volume = models.FloatField()
    price = models.FloatField()
    trades = models.FloatField()
    hours = models.IntegerField()
    calculated_at = models.DateTimeField(auto_now_add=True, db_index=True)

    class Meta:
        ordering = ["calculated_at"]
        db_table = "cs_zscore_history"
        constraints = [
            UniqueConstraint(
                fields=["symbol", "calculated_at"],
                name="unique_zscore_symbol_start_time",
            ),
        ]
        indexes = [
            models.Index(
                fields=["calculated_at"],
                name="idx_zscore_calculated_covering",
                include=["price", "volume", "trades", "hours", "symbol_id"],
            )
        ]
