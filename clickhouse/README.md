# Footprint ClickHouse storage

Rust uses a fresh `footprints_rust` database with two tables retaining 31 days:

- `executions`: captured executions and verified archive revisions, ordered by market, partition, revision, and ordinal;
- `bars`: closed Tick/TRev bars, addressed by generation and bar index.

`footprints migrate` creates the schema idempotently; `footprints serve` also runs it at startup. The schema lives in `native/footprints/migrations/001_clickhouse.sql`. Delete legacy footprint storage manually only after the new release passes production checks.

Collectors first commit executions into their SQLite WAL journals. A separate task sends RowBinary batches with `wait_for_async_insert=1`; compatible ClickHouse writes coalesce for 100 ms. Bar writes use Native format with synchronous acknowledgement. Both tables enable fsync. SQLite chart checkpoints advance only after bars are acknowledged, and readers hide uncheckpointed output.

Retry deduplication in ClickHouse is bounded. Readers also remove repeated ordinals/bar indices and reject conflicting contents, so retries outside that window cannot double-count chart trades. See [architecture](../docs/footprints.md) and [operations](../docs/footprints-production.md) for persistence and backup requirements.

Diagnostic tables are separate from footprint data. `config.d/system-logs.xml` retains diagnostics for three days and reduces text logging to warnings. The existing `apply-system-log-retention.sql` concerns those disposable diagnostics only.

Run the Rust integration test against a disposable ClickHouse server:

```sh
cd native/footprints
FOOTPRINT_TEST_CLICKHOUSE=http://127.0.0.1:18123 \
  cargo test --locked --test integration -- --ignored
```

It creates and drops its own uniquely named database. Never use a production server for fault injection.
