from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ("correlations", "0009_correlationpairhistory_cs_corr_symbol_lookup"),
    ]

    operations = [
        migrations.RunSQL(
            # Forward: Convert to partitioned table
            sql="""
            -- Drop the old table (data is ephemeral, will be regenerated)
            DROP TABLE IF EXISTS cs_correlation_pair_history CASCADE;

            -- Create partitioned table
            CREATE TABLE cs_correlation_pair_history (
                id BIGSERIAL,
                symbol1_id BIGINT NOT NULL,
                symbol2_id BIGINT NOT NULL,
                correlation_value DOUBLE PRECISION NOT NULL,
                data_type VARCHAR(20) NOT NULL,
                hours INTEGER NOT NULL,
                calculated_at TIMESTAMPTZ NOT NULL,
                PRIMARY KEY (id, calculated_at)
            ) PARTITION BY RANGE (calculated_at);

            -- Create index for lookups (will be created on each partition)
            CREATE INDEX cs_corr_symbol_lookup
            ON cs_correlation_pair_history (data_type, hours, symbol1_id, symbol2_id);

            -- Create foreign keys (not enforced on partitioned tables by default, but documented)
            -- Note: FK constraints on partitioned tables require the partition key in the constraint
            -- We skip FKs here since they add overhead and we validate in application code
            """,
            # Reverse: Recreate original table structure
            reverse_sql="""
            DROP TABLE IF EXISTS cs_correlation_pair_history CASCADE;

            CREATE TABLE cs_correlation_pair_history (
                id BIGSERIAL PRIMARY KEY,
                symbol1_id BIGINT NOT NULL REFERENCES cs_symbol(id) ON DELETE CASCADE,
                symbol2_id BIGINT NOT NULL REFERENCES cs_symbol(id) ON DELETE CASCADE,
                correlation_value DOUBLE PRECISION NOT NULL,
                data_type VARCHAR(20) NOT NULL,
                hours INTEGER NOT NULL,
                calculated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE INDEX cs_corr_symbol_lookup
            ON cs_correlation_pair_history (data_type, hours, symbol1_id, symbol2_id);
            """,
        ),
    ]
