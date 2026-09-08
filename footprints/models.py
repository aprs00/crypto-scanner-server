from __future__ import annotations

import uuid

from django.db import models
from django.utils import timezone

from footprints.constants import (
    CHART_MAX_LOOKBACK_DAYS,
    TICK_MAX_TRADES_PER_BAR,
    TREV_MAX_THRESHOLD_TICKS,
    TREV_MIN_THRESHOLD_TICKS,
)


class ChartType(models.TextChoices):
    TICK = "tick", "tick"
    TREV = "trev", "trev"


class RecoveryStatus(models.TextChoices):
    PENDING = "pending", "pending"
    CHECKED = "checked", "checked"
    VERIFIED = "verified", "verified"
    FAILED = "failed", "failed"


class ChartSeriesStatus(models.TextChoices):
    QUEUED = "queued", "queued"
    BUILDING = "building", "building"
    READY = "ready", "ready"
    FAILED = "failed", "failed"


class FootprintMarket(models.Model):
    exchange = models.CharField(max_length=24)
    contract_type = models.CharField(max_length=24, default="perpetual")
    symbol = models.CharField(max_length=40)
    base_asset = models.CharField(max_length=20)
    enabled = models.BooleanField(default=True)
    price_scale = models.PositiveSmallIntegerField(default=12)
    quantity_scale = models.PositiveSmallIntegerField(default=12)
    recovery_enabled = models.BooleanField(default=False)

    class Meta:
        db_table = "cs_footprint_markets"
        ordering = ["exchange", "symbol"]
        constraints = [
            models.UniqueConstraint(
                fields=["exchange", "contract_type", "symbol"],
                name="unique_footprint_market",
            )
        ]

    @property
    def market_key(self) -> str:
        return f"{self.exchange}:{self.contract_type}:{self.symbol}"

    def __str__(self) -> str:
        return self.market_key


class TradeRecoveryDay(models.Model):
    market = models.ForeignKey(FootprintMarket, on_delete=models.CASCADE)
    day = models.DateField()
    start_us = models.BigIntegerField()
    status = models.CharField(
        max_length=16, choices=RecoveryStatus.choices, default=RecoveryStatus.PENDING
    )
    report = models.JSONField(default=dict)
    fingerprint = models.CharField(max_length=64, blank=True)
    active_import = models.UUIDField(null=True, blank=True)
    checked_at = models.DateTimeField(null=True, blank=True)
    retry_at = models.DateTimeField(null=True, blank=True)
    error = models.TextField(blank=True)

    class Meta:
        db_table = "cs_trade_recovery_days"
        constraints = [
            models.UniqueConstraint(
                fields=("market", "day"), name="unique_trade_recovery_day"
            )
        ]


class InstrumentSpec(models.Model):
    market = models.ForeignKey(
        FootprintMarket,
        on_delete=models.CASCADE,
        related_name="instrument_specs",
    )
    effective_from = models.DateTimeField()
    tick_size = models.DecimalField(max_digits=30, decimal_places=12)
    quantity_step = models.DecimalField(max_digits=30, decimal_places=12)

    class Meta:
        db_table = "cs_instrument_specs"
        ordering = ["market", "-effective_from"]
        constraints = [
            models.UniqueConstraint(
                fields=["market", "effective_from"],
                name="unique_instrument_spec_effective_from",
            )
        ]

    def __str__(self) -> str:
        return f"{self.market.market_key} @ {self.effective_from.isoformat()}"


class ChartSeries(models.Model):
    """Configuration, lifecycle, and live checkpoint for one derived chart."""

    public_key = models.UUIDField(default=uuid.uuid4, unique=True, editable=False)
    market = models.ForeignKey(
        FootprintMarket,
        on_delete=models.CASCADE,
        related_name="chart_series",
    )
    chart_type = models.CharField(max_length=8, choices=ChartType.choices)
    parameter_a = models.PositiveIntegerField()
    parameter_b = models.PositiveIntegerField(default=0)
    lookback_days = models.PositiveSmallIntegerField(default=2)
    status = models.CharField(
        max_length=16,
        choices=ChartSeriesStatus.choices,
        default=ChartSeriesStatus.QUEUED,
    )
    anchor_at = models.DateTimeField()
    checkpoint_state = models.JSONField(null=True, blank=True)
    error_code = models.CharField(max_length=64, blank=True, default="")
    last_requested_at = models.DateTimeField(default=timezone.now)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "cs_chart_series"
        constraints = [
            models.UniqueConstraint(
                fields=[
                    "market",
                    "chart_type",
                    "parameter_a",
                    "parameter_b",
                    "lookback_days",
                ],
                name="unique_chart_series_key",
            ),
            models.CheckConstraint(
                condition=models.Q(
                    lookback_days__gte=1,
                    lookback_days__lte=CHART_MAX_LOOKBACK_DAYS,
                ),
                name="valid_chart_series_lookback",
            ),
            models.CheckConstraint(
                condition=(
                    models.Q(
                        chart_type=ChartType.TICK,
                        parameter_a__gte=1,
                        parameter_a__lte=TICK_MAX_TRADES_PER_BAR,
                        parameter_b=0,
                    )
                    | models.Q(
                        chart_type=ChartType.TREV,
                        parameter_a__gte=TREV_MIN_THRESHOLD_TICKS,
                        parameter_a__lte=TREV_MAX_THRESHOLD_TICKS,
                        parameter_b__gte=TREV_MIN_THRESHOLD_TICKS,
                        parameter_b__lte=TREV_MAX_THRESHOLD_TICKS,
                    )
                ),
                name="valid_chart_series_parameters",
            ),
        ]
        indexes = [
            models.Index(
                fields=["status", "last_requested_at"],
                name="cs_series_live_idx",
            )
        ]
