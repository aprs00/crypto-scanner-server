from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    initial = True

    dependencies = [
        ("exchange_connections", "0003_delete_binancespotkline5m_delete_ticker"),
    ]

    operations = [
        migrations.CreateModel(
            name="CointegrationPair",
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
                ("window_minutes", models.IntegerField()),
                ("hedge_ratio", models.FloatField()),
                ("intercept", models.FloatField()),
                ("spread_mean", models.FloatField()),
                ("spread_std", models.FloatField()),
                ("spread_z", models.FloatField()),
                ("half_life", models.FloatField(null=True)),
                ("adf_t", models.FloatField()),
                ("calculated_at", models.DateTimeField(db_index=True)),
                (
                    "contract_type",
                    models.ForeignKey(
                        db_constraint=False,
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="cointegration_pairs",
                        to="exchange_connections.contracttype",
                    ),
                ),
                (
                    "exchange",
                    models.ForeignKey(
                        db_constraint=False,
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="cointegration_pairs",
                        to="exchange_connections.exchange",
                    ),
                ),
                (
                    "symbol1",
                    models.ForeignKey(
                        db_constraint=False,
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="cointegration_as_symbol1",
                        to="exchange_connections.symbol",
                    ),
                ),
                (
                    "symbol2",
                    models.ForeignKey(
                        db_constraint=False,
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="cointegration_as_symbol2",
                        to="exchange_connections.symbol",
                    ),
                ),
            ],
            options={
                "db_table": "cs_cointegration_pair",
                "ordering": ["-calculated_at"],
            },
        ),
        migrations.AddConstraint(
            model_name="cointegrationpair",
            constraint=models.UniqueConstraint(
                fields=(
                    "exchange",
                    "contract_type",
                    "symbol1",
                    "symbol2",
                    "window_minutes",
                ),
                name="unique_cointegration_pair",
            ),
        ),
        migrations.AddIndex(
            model_name="cointegrationpair",
            index=models.Index(
                fields=(
                    "exchange",
                    "contract_type",
                    "window_minutes",
                    "calculated_at",
                ),
                name="cointegration_lookup_idx",
            ),
        ),
        migrations.AddIndex(
            model_name="cointegrationpair",
            index=models.Index(
                fields=(
                    "exchange",
                    "contract_type",
                    "symbol1",
                    "symbol2",
                    "window_minutes",
                ),
                name="cointegration_pair_idx",
            ),
        ),
    ]
