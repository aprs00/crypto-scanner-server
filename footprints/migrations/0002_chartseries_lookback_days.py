from django.db import migrations, models


def rebuild_truncated_series(apps, schema_editor):
    chart_series = apps.get_model("footprints", "ChartSeries")
    chart_series.objects.update(
        status="queued",
        checkpoint_state=None,
        error_code="",
    )


class Migration(migrations.Migration):
    dependencies = [("footprints", "0001_initial")]

    operations = [
        migrations.RemoveConstraint(
            model_name="chartseries",
            name="unique_chart_series_key",
        ),
        migrations.AddField(
            model_name="chartseries",
            name="lookback_days",
            field=models.PositiveSmallIntegerField(default=2),
        ),
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
                name="unique_chart_series_key",
            ),
        ),
        migrations.AddConstraint(
            model_name="chartseries",
            constraint=models.CheckConstraint(
                condition=models.Q(lookback_days__gte=1, lookback_days__lte=30),
                name="valid_chart_series_lookback",
            ),
        ),
        migrations.RunPython(rebuild_truncated_series, migrations.RunPython.noop),
    ]
