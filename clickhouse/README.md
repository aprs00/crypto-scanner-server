# Chart ClickHouse storage

ClickHouse owns two 31-day tables on one persistent volume:

- `raw_executions`: the exact ordered execution tape used by both projectors;
- `chart_bars`: sparse closed Tick/TRev bars.

The footprint maintenance service creates the database and tables
idempotently at startup. There is no migration ledger or manual migration
command in V0. Collectors write directly to `raw_executions`; there is no
SQLite, Parquet, shadow-write, or relational-bar path.

Collectors form a batch for at most 250 ms (or 500 executions). ClickHouse
coalesces compatible writes for up to 500 ms before acknowledgement, keeping
updates sub-second without creating a part per handful of trades.

Run the opt-in real-server test inside the app container:

```sh
FOOTPRINT_CLICKHOUSE_INTEGRATION=1 \
python manage.py test footprints.tests.test_clickhouse_integration
```

This is a pre-production setup. Add off-host backups before the execution tape
becomes irreplaceable production data.
