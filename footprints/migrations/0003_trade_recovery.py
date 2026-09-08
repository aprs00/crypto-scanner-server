from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):
    dependencies = [("footprints", "0002_chartseries_lookback_days")]
    operations = [
        migrations.AddField(
            model_name="footprintmarket",
            name="recovery_enabled",
            field=models.BooleanField(default=False),
        ),
        migrations.CreateModel(
            name="TradeRecoveryDay",
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
                ("day", models.DateField()),
                ("start_us", models.BigIntegerField()),
                (
                    "status",
                    models.CharField(
                        choices=[
                            ("pending", "pending"),
                            ("checked", "checked"),
                            ("verified", "verified"),
                            ("failed", "failed"),
                        ],
                        default="pending",
                        max_length=16,
                    ),
                ),
                ("report", models.JSONField(default=dict)),
                ("fingerprint", models.CharField(blank=True, max_length=64)),
                ("active_import", models.UUIDField(blank=True, null=True)),
                ("checked_at", models.DateTimeField(blank=True, null=True)),
                ("retry_at", models.DateTimeField(blank=True, null=True)),
                ("error", models.TextField(blank=True)),
                (
                    "market",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        to="footprints.footprintmarket",
                    ),
                ),
            ],
            options={
                "db_table": "cs_trade_recovery_days",
                "constraints": [
                    models.UniqueConstraint(
                        fields=("market", "day"), name="unique_trade_recovery_day"
                    )
                ],
            },
        ),
    ]
