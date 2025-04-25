from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ("exchange_connections", "0003_remove_kline1m_unique_klines_1m_fields"),
    ]

    operations = [
        migrations.RunSQL(
            """
            CREATE EXTENSION IF NOT EXISTS timescaledb;

            SELECT create_hypertable(
                'cs_klines_1m',
                'start_time',
                partitioning_column => 'symbol',
                number_partitions => 100,
                chunk_time_interval => INTERVAL '1 hour',
                if_not_exists => TRUE
            );
            """,
            reverse_sql=migrations.RunSQL.noop,
        ),
    ]
