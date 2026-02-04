from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ("cointegration", "0001_initial"),
    ]

    operations = [
        migrations.SeparateDatabaseAndState(
            database_operations=[
                migrations.RunSQL(
                    sql="""
                    DROP TABLE IF EXISTS cs_cointegration_pair_history CASCADE;

                    CREATE TABLE cs_cointegration_pair_history (
                        id BIGSERIAL,
                        exchange_id BIGINT NOT NULL,
                        contract_type_id BIGINT NOT NULL,
                        symbol1_id BIGINT NOT NULL,
                        symbol2_id BIGINT NOT NULL,
                        window_minutes INTEGER NOT NULL,
                        hedge_ratio DOUBLE PRECISION NOT NULL,
                        intercept DOUBLE PRECISION NOT NULL,
                        spread_mean DOUBLE PRECISION NOT NULL,
                        spread_std DOUBLE PRECISION NOT NULL,
                        spread_z DOUBLE PRECISION NOT NULL,
                        half_life DOUBLE PRECISION,
                        adf_t DOUBLE PRECISION NOT NULL,
                        calculated_at TIMESTAMPTZ NOT NULL,
                        PRIMARY KEY (id, calculated_at)
                    ) PARTITION BY RANGE (calculated_at);

                    CREATE INDEX cs_cointegration_pair_hist_lookup
                    ON cs_cointegration_pair_history (exchange_id, contract_type_id, window_minutes, calculated_at);

                    CREATE INDEX cs_cointegration_pair_hist_pair
                    ON cs_cointegration_pair_history (exchange_id, contract_type_id, symbol1_id, symbol2_id, window_minutes, calculated_at);
                    """,
                    reverse_sql="""
                    DROP TABLE IF EXISTS cs_cointegration_pair_history CASCADE;
                    """,
                ),
            ],
            state_operations=[
                migrations.CreateModel(
                    name="CointegrationPairHistory",
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
                        ("calculated_at", models.DateTimeField()),
                        (
                            "contract_type",
                            models.ForeignKey(
                                db_constraint=False,
                                on_delete=django.db.models.deletion.CASCADE,
                                related_name="cointegration_pair_history",
                                to="exchange_connections.contracttype",
                            ),
                        ),
                        (
                            "exchange",
                            models.ForeignKey(
                                db_constraint=False,
                                on_delete=django.db.models.deletion.CASCADE,
                                related_name="cointegration_pair_history",
                                to="exchange_connections.exchange",
                            ),
                        ),
                        (
                            "symbol1",
                            models.ForeignKey(
                                db_constraint=False,
                                on_delete=django.db.models.deletion.CASCADE,
                                related_name="cointegration_history_as_symbol1",
                                to="exchange_connections.symbol",
                            ),
                        ),
                        (
                            "symbol2",
                            models.ForeignKey(
                                db_constraint=False,
                                on_delete=django.db.models.deletion.CASCADE,
                                related_name="cointegration_history_as_symbol2",
                                to="exchange_connections.symbol",
                            ),
                        ),
                    ],
                    options={
                        "db_table": "cs_cointegration_pair_history",
                        "managed": False,
                        "ordering": ["-calculated_at"],
                    },
                ),
            ],
        )
    ]
