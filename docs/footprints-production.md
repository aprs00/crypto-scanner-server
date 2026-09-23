# Footprint operations and cutover

The Rust collectors and chart server use a separate image (`Dockerfile.footprints`). Configure `FOOTPRINT_IMAGE` with an immutable tag in production. The scanner's existing `APP_IMAGE` remains separate. Build and verify both images before changing the running application.

The initial production cutover and manual legacy-data deletion are recorded in [the September 2026 release](footprints-release-2026-09-15.md).

## Configuration

| Variable | Default / purpose |
| --- | --- |
| `FOOTPRINT_DATABASE` | `footprints_rust`; fresh ClickHouse database |
| `FOOTPRINT_CLICKHOUSE_URL` | `http://clickhouse:8123` |
| `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD` | Existing ClickHouse credentials |
| `FOOTPRINT_DATA_DIR` | `/var/lib/footprints`; persistent local volume |
| `FOOTPRINT_OUTBOX_MAX_BYTES` | `2147483648` per collector |
| `FOOTPRINT_DISK_RESERVE_BYTES` | `5368709120` |
| `FOOTPRINT_ALLOWED_ORIGINS` | Explicit comma-separated browser origins |
| `FOOTPRINT_BIND` | `0.0.0.0:8010`; private container port |
| `FOOTPRINT_COLLECTOR_URLS` | Binance, Bybit, Hyperliquid health URLs in that order |
| `FOOTPRINT_INSTANCE` | Unique standby instance; promotion persists across restarts |

Four persistent volumes hold the collectors and server: `binance_footprint_rust`, `bybit_footprint_rust`, `hyperliquid_footprint_rust`, and `footprint_rust_control`. The production override preserves all four. Never substitute Python outbox volumes or reuse chart IDs. Loss of a journal/control volume requires an explicit new storage generation, not an empty checkpoint against old tables. Startup/delivery reject stored counters ahead of local journal/control history, preventing a fresh volume from reusing existing ordinals or bar IDs.

ClickHouse and the collectors start independently. Collectors journal through ClickHouse outages. The chart server retries startup if migration/storage is unavailable. Docker restarts exited processes; each process watches its critical loop so a stalled task cannot remain green indefinitely. Disk exhaustion fails readiness rather than overwriting raw data.

## Verification before cutover

1. Run Rust unit/oracle tests, Clippy, the isolated ClickHouse integration test, and frontend footprint tests/build (`native/footprints/README.md`).
2. Build the footprint image and check its CLI and health commands. Its final runtime must contain no Python executable, package, or wheel. For an existing deployment, run `footprints warm-prepared --days 5` from the candidate image against the existing ClickHouse and control volume before switching the chart API; verified archives then have shared TRev tapes ready for new settings.
3. Start an isolated Rust stack with fresh volumes/database, then inspect counts and duplicates. Exercise ClickHouse outages, collector SIGTERM/restart, standby handoff, and archive verification. Confirm queued journals drain afterward.
4. Run `footprints soak --hours 72 --output /var/lib/footprints/soak.jsonl` inside the API container. Preserve readiness, queue growth, delivery/chart lag, reconnects, gaps, and archive errors. A short smoke test does not establish long-term uptime; record explicitly when an authorized release precedes this observation period.
5. Measure public request-to-ready-snapshot time for real five-day and 30-day TRev and one-day Tick charts, with both cold and prepared input. Record the market and chart settings. The CLI scalar/prepared benchmark isolates engine speed and is not an end-to-end improvement claim.

The [local cold and prepared HTTP measurements](../benchmarks/results/footprint-cold-http-2026-09-23.json) establish a server-side baseline; production and browser latency still require measurement after deployment.

## Initial cutover from Python

After validation, stop legacy `footprint-maintenance` and `footprint-recovery` containers. Replace the three Python collectors with Rust using fresh volumes. Start `footprint-api`; it bootstraps 30 days of Binance/Bybit archives automatically. Hyperliquid history begins with Rust capture.

Recreate `app` from the new scanner image without its host port binding, then start `gateway` on the original port 8000. Footprint HTTP/WebSocket routes go to Rust; other routes go to Django. The old app must release port 8000 first. Keep the external TLS proxy and configure the real frontend origins.

Deploy the coverage changes from the sibling `crypto-scanner` repository together with the gateway change. It understands repaired generation IDs, resnapshot requests, and coverage messages.

After the new release passes production checks, manually delete the legacy footprint tables and legacy outbox/prepared-cache volumes, as requested for this release. There is no automatic cleanup code. Inventory dependencies before dropping PostgreSQL tables; preserve all unrelated scanner tables, the shared PostgreSQL/ClickHouse volumes, and the new Rust database/volumes. Once legacy history is deleted, rollback cannot restore that history from the old image alone.

## Subsequent collector updates

```sh
scripts/rollout-footprints.sh REGISTRY/footprints:COMMIT all
```

The script warms a candidate sharing the exchange journal volume with a unique instance ID. It requires socket overlap, disables the old container's restart policy, drains the old writer, promotes the candidate, and waits for verified writer readiness. It then renames that candidate to the canonical container name. There is one handoff.

A failed candidate is removed before cutover. Once cutover starts, failures retain the candidate for inspection and do not force-kill a writer. SQLite persists promotion through restarts. Image pins in `.env` update after each successful exchange.

The promoted container retains its Compose `run` one-off label. Use this script for subsequent collector updates and `docker restart cs-EXCHANGE-footprints` for a direct restart. Do not blindly run `compose up --force-recreate` for collectors: that bypasses the staged handoff. The promoted container retains its service network alias.

## Monitoring

Private endpoints are `/health/live`, `/health/ready`, `/health/status`, `/metrics`, `/admin/promote`, and `/admin/drain`. The gateway excludes footprint administration. `footprints health --writer --overlap` checks promotion readiness.

Track both socket legs, pending journal rows/bytes and oldest age, delivery/chart lag, reconnects, and explicit gaps. Inspect `archives.status`, `attempted`, and `error` in `control.sqlite3` for unavailable or conflicting history. Never relabel pending intervals as verified merely to clear an alert.

Back up SQLite with its online backup facility or while the writer is stopped; copying only its main file during WAL writes is unsafe. Preserve volumes across container replacement. One Docker host cannot guarantee 100% uptime during host, disk, network, or exchange failures.
