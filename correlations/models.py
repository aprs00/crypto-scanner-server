from django.db import models

from exchange_connections.models import Symbol


class CorrelationPairHistory(models.Model):
    """
    Stores historical correlation values between two symbols.
    For a given pair (e.g., BTCUSDT-ETHUSDT), we store the correlation
    coefficient calculated at specific time intervals.
    """

    symbol1 = models.ForeignKey(
        Symbol,
        on_delete=models.CASCADE,
        related_name="correlations_as_symbol1",
    )
    symbol2 = models.ForeignKey(
        Symbol,
        on_delete=models.CASCADE,
        related_name="correlations_as_symbol2",
    )
    correlation_value = models.FloatField(
        help_text="Pearson correlation coefficient (-1 to 1)"
    )
    data_type = models.CharField(
        max_length=20,
        help_text="Type of data used for correlation (e.g., 'close', 'volume', 'trades')",
    )
    hours = models.IntegerField(
        help_text="Time window in hours used for correlation calculation"
    )
    calculated_at = models.DateTimeField(
        auto_now_add=True,
        help_text="Timestamp when this correlation was calculated",
    )

    class Meta:
        managed = False  # Table is partitioned, managed via raw SQL
        ordering = ["-calculated_at"]
        db_table = "cs_correlation_pair_history"

    def __str__(self):
        return f"{self.symbol1.name}-{self.symbol2.name} ({self.data_type}, {self.hours}h): {self.correlation_value:.2f} @ {self.calculated_at}"
