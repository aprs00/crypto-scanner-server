import uuid
from datetime import datetime, timezone
from decimal import Decimal

import django.db.models.deletion
import django.utils.timezone
from django.db import migrations, models


def seed_btc_markets(apps, schema_editor):
    market_model = apps.get_model("footprints", "FootprintMarket")
    spec_model = apps.get_model("footprints", "InstrumentSpec")
    effective_from = datetime(1970, 1, 1, tzinfo=timezone.utc)
    defaults = (
        ("binance", "BTCUSDT", "BTC", Decimal("0.1"), Decimal("0.001")),
        ("bybit", "BTCUSDT", "BTC", Decimal("0.1"), Decimal("0.001")),
        ("hyperliquid", "BTC", "BTC", Decimal("0.1"), Decimal("0.00001")),
    )
    for exchange, symbol, base_asset, tick_size, quantity_step in defaults:
        market = market_model.objects.create(
            exchange=exchange,
            contract_type="perpetual",
            symbol=symbol,
            base_asset=base_asset,
        )
        spec_model.objects.create(
            market=market,
            effective_from=effective_from,
            tick_size=tick_size,
            quantity_step=quantity_step,
        )


class Migration(migrations.Migration):
    initial = True
    dependencies = []
    operations = [
        migrations.CreateModel(
            name="FootprintMarket",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("exchange", models.CharField(max_length=24)),
                ("contract_type", models.CharField(default="perpetual", max_length=24)),
                ("symbol", models.CharField(max_length=40)),
                ("base_asset", models.CharField(max_length=20)),
                ("enabled", models.BooleanField(default=True)),
                ("price_scale", models.PositiveSmallIntegerField(default=12)),
                ("quantity_scale", models.PositiveSmallIntegerField(default=12)),
            ],
            options={
                "db_table": "cs_footprint_markets",
                "ordering": ["exchange", "symbol"],
                "constraints": [
                    models.UniqueConstraint(
                        fields=("exchange", "contract_type", "symbol"),
                        name="unique_footprint_market",
                    )
                ],
            },
        ),
        migrations.CreateModel(
            name="InstrumentSpec",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("effective_from", models.DateTimeField()),
                ("tick_size", models.DecimalField(decimal_places=12, max_digits=30)),
                ("quantity_step", models.DecimalField(decimal_places=12, max_digits=30)),
                (
                    "market",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="instrument_specs",
                        to="footprints.footprintmarket",
                    ),
                ),
            ],
            options={
                "db_table": "cs_instrument_specs",
                "ordering": ["market", "-effective_from"],
                "constraints": [
                    models.UniqueConstraint(
                        fields=("market", "effective_from"),
                        name="unique_instrument_spec_effective_from",
                    )
                ],
            },
        ),
        migrations.CreateModel(
            name="ChartSeries",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                (
                    "public_key",
                    models.UUIDField(default=uuid.uuid4, editable=False, unique=True),
                ),
                (
                    "chart_type",
                    models.CharField(
                        choices=[("tick", "tick"), ("trev", "trev")],
                        max_length=8,
                    ),
                ),
                ("parameter_a", models.PositiveIntegerField()),
                ("parameter_b", models.PositiveIntegerField(default=0)),
                (
                    "status",
                    models.CharField(
                        choices=[
                            ("queued", "queued"),
                            ("building", "building"),
                            ("ready", "ready"),
                            ("failed", "failed"),
                        ],
                        default="queued",
                        max_length=16,
                    ),
                ),
                ("anchor_at", models.DateTimeField()),
                ("checkpoint_state", models.JSONField(blank=True, null=True)),
                ("error_code", models.CharField(blank=True, default="", max_length=64)),
                (
                    "last_requested_at",
                    models.DateTimeField(default=django.utils.timezone.now),
                ),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                (
                    "market",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="chart_series",
                        to="footprints.footprintmarket",
                    ),
                ),
            ],
            options={
                "db_table": "cs_chart_series",
                "constraints": [
                    models.UniqueConstraint(
                        fields=(
                            "market",
                            "chart_type",
                            "parameter_a",
                            "parameter_b",
                        ),
                        name="unique_chart_series_key",
                    ),
                    models.CheckConstraint(
                        condition=(
                            models.Q(
                                chart_type="tick",
                                parameter_a__gte=1,
                                parameter_a__lte=4_000,
                                parameter_b=0,
                            )
                            | models.Q(
                                chart_type="trev",
                                parameter_a__gte=1,
                                parameter_a__lte=4_000,
                                parameter_b__gte=1,
                                parameter_b__lte=4_000,
                            )
                        ),
                        name="valid_chart_series_parameters",
                    ),
                ],
                "indexes": [
                    models.Index(
                        fields=["status", "last_requested_at"],
                        name="cs_series_live_idx",
                    )
                ],
            },
        ),
        migrations.RunPython(seed_btc_markets, migrations.RunPython.noop),
    ]
