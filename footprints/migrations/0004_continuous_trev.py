from datetime import timedelta

from django.db import migrations, models
from django.db.models import Max
from django.utils import timezone


def rebuild_trev(apps, schema_editor):
    series = apps.get_model("footprints", "ChartSeries")
    rows = series.objects.using(schema_editor.connection.alias)
    configurations = list(
        rows.filter(chart_type="trev")
        .values("market_id", "parameter_a", "parameter_b")
        .annotate(last_requested=Max("last_requested_at"))
    )
    # Different old lookbacks have incompatible origins. New IDs also fence
    # their ClickHouse rows and client caches; those rows expire under the TTL.
    rows.filter(chart_type="trev").delete()
    anchor = (timezone.now() - timedelta(days=30)).replace(
        minute=0, second=0, microsecond=0
    )
    for configuration in configurations:
        rows.create(
            market_id=configuration["market_id"],
            chart_type="trev",
            parameter_a=configuration["parameter_a"],
            parameter_b=configuration["parameter_b"],
            lookback_days=None,
            anchor_at=anchor,
            last_requested_at=configuration["last_requested"],
        )


def restore_lookback(apps, schema_editor):
    series = apps.get_model("footprints", "ChartSeries")
    rows = series.objects.using(schema_editor.connection.alias)
    rows.filter(chart_type="trev").update(lookback_days=30)
    rows.filter(status="catching_up").update(status="queued")


class Migration(migrations.Migration):
    dependencies = [("footprints", "0003_trade_recovery")]

    operations = [
        migrations.RemoveConstraint(
            model_name="chartseries", name="unique_chart_series_key"
        ),
        migrations.RemoveConstraint(
            model_name="chartseries", name="valid_chart_series_lookback"
        ),
        migrations.AlterField(
            model_name="chartseries",
            name="lookback_days",
            field=models.PositiveSmallIntegerField(blank=True, default=2, null=True),
        ),
        migrations.AlterField(
            model_name="chartseries",
            name="status",
            field=models.CharField(
                choices=[
                    ("queued", "queued"),
                    ("building", "building"),
                    ("catching_up", "catching_up"),
                    ("ready", "ready"),
                    ("failed", "failed"),
                ],
                default="queued",
                max_length=16,
            ),
        ),
        migrations.RunPython(rebuild_trev, restore_lookback),
    ]
