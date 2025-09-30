from django.db import models
from django.db.models import UniqueConstraint

from exchange_connections.models import Symbol


class CorrelationPairHistory(models.Model):
    """Store correlation values for individual symbol pairs over time"""

    symbol_a = models.ForeignKey(
        Symbol, on_delete=models.CASCADE, related_name="correlation_as_a", db_index=True
    )
    symbol_b = models.ForeignKey(
        Symbol, on_delete=models.CASCADE, related_name="correlation_as_b", db_index=True
    )
    correlation_value = models.FloatField()
    data_type = models.CharField(max_length=20, db_index=True)
    hours = models.IntegerField(db_index=True)
    calculated_at = models.DateTimeField(auto_now_add=True, db_index=True)

    class Meta:
        ordering = ["calculated_at"]
        db_table = "cs_correlation_pair_history"
        constraints = [
            UniqueConstraint(
                fields=["symbol_a", "symbol_b", "data_type", "hours", "calculated_at"],
                name="unique_correlation_pair_time",
            ),
        ]
        indexes = [
            models.Index(
                fields=["symbol_a", "symbol_b", "data_type", "hours", "-calculated_at"],
                name="idx_correlation_pair_lookup",
            ),
        ]

    def __str__(self):
        return f"{self.symbol_a.name}-{self.symbol_b.name} {self.data_type} @ {self.hours}h: {self.correlation_value:.2f}"
