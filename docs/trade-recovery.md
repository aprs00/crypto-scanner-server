# Binance and Bybit trade recovery

No API keys or paid data subscriptions are needed. Hyperliquid keeps its existing collector and replay path.

## What runs

- Each Binance/Bybit collector commits normalized trades and their source ordering to its own SQLite outbox (WAL, `synchronous=FULL`, up to 500 trades / 100 ms per commit). ClickHouse delivery runs separately. A restart resumes the same pending batches; an ambiguous insert is checked by immutable trade identity before acknowledgement.
- PostgreSQL elects the writer; a local file lock also protects the shared outbox during handoff. Canonical and candidate containers mount the same exchange-specific named volume. Never delete these volumes or run two hosts against the same collector lease with separate local outboxes.
- Bad public events are rejected individually and recorded in bounded `rejected.jsonl` logs. Valid Bybit batch members continue. Warm buffers retain their newest overlap and report evictions rather than crashing both feeds.
- On activation, one free recent-trades request can bridge a short interruption only when the stored endpoint and buffered endpoint both overlap. Binance additionally requires consecutive IDs. Older gaps remain unverified until archive reconciliation.
- `footprint-recovery` polls closed minutes every five minutes, excluding the newest two minutes. It compares Binance count and base volume, and Bybit base volume, using exact decimals. Minute checks are a coverage alarm, not proof of individual trade completeness.
- After 02:00 UTC it processes at most one completed archive day per exchange per pass, back through existing retained history (maximum 30 days). Failed/unpublished files retry after six hours. The first collection day is clipped to the first stored timestamp.
- Archives must pass checksum (Binance) / decompression CRC, unique identity, exact atom/specification, overlapping immutable-field and order checks, and full-hour exchange candle checks. Bybit archive fractional-second timestamps are normalized to the live API's millisecond precision; archive row order supplies the missing live cross-sequence ordering.

Verified repairs go into an immutable ClickHouse `archive_executions` side table. A PostgreSQL transaction activates the day and replaces affected chart-series IDs while preserving settings, anchors and leases. Builds freeze their source choices; deleted IDs fence old checkpoints. Clients clear retired caches and rediscover their series via WebSocket, HTTP 404, or periodic lease renewal. Original raw trades and old bars are not deleted by recovery; normal 31-day TTLs still apply.

## Audit first

The Compose worker defaults to `--loop` **without** `--repair`. It records audit reports but does not publish history or replace charts. One-shot audits only print reports:

```sh
python manage.py footprint_reconcile --exchange both
python manage.py footprint_reconcile --exchange binance --day 2026-09-04
python manage.py footprint_reconcile --exchange bybit --day 2026-09-02
```

Use dates within the current retained 30-day window. A singleton lock prevents a one-shot command racing the loop; stop just `footprint-recovery` before a manual archive audit or repair.

After reviewing the audit, explicitly publish a selected day:

```sh
python manage.py footprint_reconcile --exchange binance --day 2026-09-04 --repair
```

Use `--refresh` with `--day` to recheck a verified day. Identical imports resume without duplicating rows or repeatedly replacing charts. Conflicting IDs, unverifiable ordering or partial imports fail closed and retain the previously active source. Reports distinguish `repair_required`, `matches` and `repaired`; missing counts in a repaired report describe the original raw tape, which remains unchanged.

To automate publication after review, change only the recovery service command to:

```sh
python manage.py footprint_reconcile --exchange both --loop --repair
```

Do not edit `recovery_enabled` or `active_import` manually: switching source inputs also requires chart replacement.

## Health and disk limits

```sh
docker exec cs-binance-footprints python manage.py footprint_health --exchange binance
docker exec cs-bybit-footprints python manage.py footprint_health --exchange bybit
docker logs --tail 100 cs-footprint-recovery
```

Health JSON separates capture heartbeat/fatal state, delivery backlog age/size, rejected events (since process start), coverage incidents and unresolved day reports. Docker's `--capture-only` check has no database dependency. A delivery backlog alone does not fail capture health or trigger restarts. A healthy process is not a claim of complete historical data.

Defaults: 2 GiB pending payload per outbox and 5 GiB free disk reserve (`FOOTPRINT_OUTBOX_MAX_BYTES`, `FOOTPRINT_DISK_RESERVE_BYTES`). SQLite indexes/recent identities and reusable pages need additional headroom. Archive work is one temporary download/staging database at a time, capped at 1 GiB compressed / 4 GiB decompressed, with the same 5 GiB reserve checked during staging. Exhaustion explicitly stops capture; do not clear the queue to make the alert disappear. Restore space/ClickHouse and restart the same volume-backed collector.

This protects committed local batches from process/ClickHouse outages, not destruction of the host/disk. A hard kill can lose the current uncommitted in-memory batch; recent REST or the next published daily archive reconciles it. Free daily files may be delayed or unavailable, and Bybit does not expose a complete historical REST cursor. Unverified days remain visible rather than being labelled complete.

## Rollout order

This change is additive and is not a production deployment by itself.

1. Build an immutable backend image; keep a backup of the existing database and volume configuration. Apply migration `footprints.0003_trade_recovery` before starting any updated Django processes.
2. Deploy the matching API, chart maintenance and frontend before allowing archive publication. Start the new recovery service in its default audit mode. Inspect its reports and disk usage.
3. Roll only the two collectors, one at a time, using the existing warm handoff script:

   ```sh
   scripts/rollout-footprints.sh --exchange binance --image YOUR_IMMUTABLE_IMAGE
   scripts/rollout-footprints.sh --exchange bybit --image YOUR_IMMUTABLE_IMAGE
   ```

   The script persists `BINANCE_FOOTPRINT_IMAGE` / `BYBIT_FOOTPRINT_IMAGE` independently. Compose v2.24.4+ is required for production volume overrides. Verify `writer-active`, `handoff-verified`, health JSON and a decreasing outbox backlog after each rollout. A collector may keep capturing while handoff verification fails; inspect the failure rather than rerunning blindly.
4. Review a retained problematic day with an audit, then publish it explicitly. Confirm new series reach `ready`, minute checks resume, and no identity/order conflicts are reported. Enable loop repair only after this check.

Do not use `--exchange all`, recreate Hyperliquid, run a blanket `docker compose up`, delete ClickHouse rows, or remove the outbox volumes as part of this rollout. Once archives are active, rolling back to a reader that does not understand the new cursors is unsafe; keep the compatible reader and disable further repair if investigation is needed.

## Local verification

```sh
docker exec -e FOOTPRINT_CLICKHOUSE_INTEGRATION=1 cs-app python manage.py test footprints.tests --noinput --keepdb
docker exec cs-app python manage.py makemigrations --check --dry-run
bash -n scripts/rollout-footprints.sh
```

Real ClickHouse tests use a test-owned UUID database, not the collector's database. Frontend checks are `pnpm test`, `pnpm lint`, and `pnpm build`; no browser automation is needed.
