from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [("footprints", "0004_continuous_trev")]

    # Keep indexes/checks in a separate transaction from the data migration:
    # PostgreSQL must finish deferred foreign-key triggers before creating them.
    operations = [
        migrations.AddConstraint(
            model_name="chartseries",
            constraint=models.UniqueConstraint(
                fields=(
                    "market",
                    "chart_type",
                    "parameter_a",
                    "parameter_b",
                    "lookback_days",
                ),
                condition=models.Q(chart_type="tick"),
                name="unique_chart_series_key",
            ),
        ),
        migrations.AddConstraint(
            model_name="chartseries",
            constraint=models.UniqueConstraint(
                fields=("market", "chart_type", "parameter_a", "parameter_b"),
                condition=models.Q(chart_type="trev"),
                name="unique_trev_series_key",
            ),
        ),
        migrations.AddConstraint(
            model_name="chartseries",
            constraint=models.CheckConstraint(
                condition=(
                    models.Q(chart_type="trev", lookback_days__isnull=True)
                    | models.Q(
                        chart_type="tick",
                        lookback_days__isnull=False,
                        lookback_days__gte=1,
                        lookback_days__lte=30,
                    )
                ),
                name="valid_chart_series_lookback",
            ),
        ),
    ]
