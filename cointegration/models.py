from django.db import models
from django.db.models import UniqueConstraint

from exchange_connections.models import Exchange, ContractType, Symbol


class CointegrationPair(models.Model):
    exchange = models.ForeignKey(
        Exchange,
        on_delete=models.CASCADE,
        db_constraint=False,
        related_name="cointegration_pairs",
    )
    contract_type = models.ForeignKey(
        ContractType,
        on_delete=models.CASCADE,
        db_constraint=False,
        related_name="cointegration_pairs",
    )
    symbol1 = models.ForeignKey(
        Symbol,
        on_delete=models.CASCADE,
        db_constraint=False,
        related_name="cointegration_as_symbol1",
    )
    symbol2 = models.ForeignKey(
        Symbol,
        on_delete=models.CASCADE,
        db_constraint=False,
        related_name="cointegration_as_symbol2",
    )
    window_minutes = models.IntegerField()
    hedge_ratio = models.FloatField()
    intercept = models.FloatField()
    spread_mean = models.FloatField()
    spread_std = models.FloatField()
    spread_z = models.FloatField()
    half_life = models.FloatField(null=True)
    adf_t = models.FloatField()
    calculated_at = models.DateTimeField(db_index=True)

    class Meta:
        db_table = "cs_cointegration_pair"
        ordering = ["-calculated_at"]
        constraints = [
            UniqueConstraint(
                fields=[
                    "exchange",
                    "contract_type",
                    "symbol1",
                    "symbol2",
                    "window_minutes",
                ],
                name="unique_cointegration_pair",
            )
        ]
        indexes = [
            models.Index(
                fields=[
                    "exchange",
                    "contract_type",
                    "window_minutes",
                    "calculated_at",
                ],
                name="cointegration_lookup_idx",
            ),
            models.Index(
                fields=[
                    "exchange",
                    "contract_type",
                    "symbol1",
                    "symbol2",
                    "window_minutes",
                ],
                name="cointegration_pair_idx",
            ),
        ]


class CointegrationPairHistory(models.Model):
    exchange = models.ForeignKey(
        Exchange,
        on_delete=models.CASCADE,
        db_constraint=False,
        related_name="cointegration_pair_history",
    )
    contract_type = models.ForeignKey(
        ContractType,
        on_delete=models.CASCADE,
        db_constraint=False,
        related_name="cointegration_pair_history",
    )
    symbol1 = models.ForeignKey(
        Symbol,
        on_delete=models.CASCADE,
        db_constraint=False,
        related_name="cointegration_history_as_symbol1",
    )
    symbol2 = models.ForeignKey(
        Symbol,
        on_delete=models.CASCADE,
        db_constraint=False,
        related_name="cointegration_history_as_symbol2",
    )
    window_minutes = models.IntegerField()
    hedge_ratio = models.FloatField()
    intercept = models.FloatField()
    spread_mean = models.FloatField()
    spread_std = models.FloatField()
    spread_z = models.FloatField()
    half_life = models.FloatField(null=True)
    adf_t = models.FloatField()
    calculated_at = models.DateTimeField()

    class Meta:
        managed = False
        db_table = "cs_cointegration_pair_history"
        ordering = ["-calculated_at"]
