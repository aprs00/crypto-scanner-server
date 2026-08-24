# Footprint V0 production deployment

This release is designed for one Docker host. It deliberately has no account
system, WAL/backfill service, cross-host collector, Prometheus stack, or bundled
reverse proxy. Each execution collector keeps a second subscribed WebSocket as
a same-process hot standby for isolated connection failures.

## Before the first release

Footprints have not shipped yet, so initialize PostgreSQL and ClickHouse from
the final baseline on fresh production volumes. After this release, never edit
`footprints/migrations/0001_initial.py` or the baseline ClickHouse DDL; add
forward migrations instead.

Install Docker Compose 2.24.4 or newer. The production override uses Compose's
`!reset` merge tag, and the rollout script checks this version before changing
any containers.

Build and tag the application with the exact commit, for example:

```sh
docker build --target runtime -t crypto-scanner:$(git rev-parse --short=12 HEAD) .
export APP_IMAGE=crypto-scanner:$(git rev-parse --short=12 HEAD)
```

Keep the deployment environment in the repository's ignored `.env` file. At
minimum use production values for
`SECRET_KEY`, PostgreSQL credentials, a non-empty `CLICKHOUSE_PASSWORD`,
`DJANGO_ALLOWED_HOSTS`, the currently deployed immutable `APP_IMAGE`, and:

```dotenv
DEBUG=false
STORE_TO_DB=true
POSTGRES_HOST=db
POSTGRES_PORT=5432
```

`DJANGO_ALLOWED_HOSTS` contains hostnames only, without schemes or paths. Include
the API hostname and every frontend hostname allowed to open chart WebSockets.

## Initial deployment

The production override requires an immutable `APP_IMAGE`, removes application
source mounts, enables Django's proxy/TLS cookie settings, binds Daphne to
localhost, and does not publish Redis.

```sh
docker compose -f docker-compose.yml -f docker-compose.prod.yml run --rm app python manage.py migrate
docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d
```

Use that broad `up -d` command only for the initial deployment. For later
releases and routine restarts, use the rolling commands below.

## Routine footprint restarts

From the production repository directory, restart all footprint collectors
with:

```sh
./scripts/rollout-footprints.sh --exchange all
```

Restart only one exchange when needed:

```sh
./scripts/rollout-footprints.sh --exchange binance
./scripts/rollout-footprints.sh --exchange bybit
./scripts/rollout-footprints.sh --exchange hyperliquid
```

Do not use `docker restart` or `docker compose restart` for footprint
collectors. Those commands close both exchange sockets at once and create an
ingestion gap.

## Release rollout

Build an immutable image, run migrations if required, then roll the three
collectors to it:

```sh
./scripts/rollout-footprints.sh \
  --image "crypto-scanner:$(git rev-parse --short=12 HEAD)" \
  --exchange all
```

The script starts a warm candidate, waits for both of its feeds, transfers the
exchange's PostgreSQL advisory-lock ownership, and requires a shared recent
trade identity after each handoff. Only then does it remove the candidate and
continue to the next exchange. A successful explicit all-exchange rollout
also updates `APP_IMAGE` in `.env` atomically.

For the first rollout from a collector version that does not expose leadership
state, add `--bootstrap-legacy`. Do not use this option again after all three
collectors run the lock-aware version:

```sh
./scripts/rollout-footprints.sh --image crypto-scanner:<commit> \
  --exchange all --bootstrap-legacy
```

The rollout script serializes runs with a host lock. Before cutover it removes
a failed candidate automatically; after cutover it leaves the current
containers in place and prints recovery context so it cannot stop the active
writer by assumption.
If a replacement becomes active without verified feed overlap, collection
continues in a new source epoch, but the script stops and reports a possible
gap. It leaves that active collector running for inspection.

The `db-maintenance` service keeps one-minute klines for 30 days using bounded
50,000-row deletes, with no more than four batches per five-minute cycle.
Cleanup defaults to enabled; set `KLINE_CLEANUP_ENABLED=false` only as a
temporary operational override. ClickHouse system diagnostics retain three
days. On the first release with that policy, apply the existing-table update
before recreating ClickHouse:

```sh
docker exec -i cs-clickhouse sh -c \
  'exec clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --multiquery' \
  < clickhouse/apply-system-log-retention.sql
```

Use the host's existing TLS reverse proxy to forward to `127.0.0.1:8000` and
preserve `Host`, set `X-Forwarded-Proto: https`, and pass WebSocket
`Upgrade`/`Connection` headers. Do not expose Daphne, PostgreSQL, ClickHouse,
or Redis directly to the Internet. The initial HSTS duration is five minutes;
only increase it after HTTPS has been verified.

Start the collectors and the single `footprint-maintenance` container before
exposing the frontend. A second maintenance process exits instead of competing
for chart checkpoints.

## Release checks

- Confirm all three footprint collectors and maintenance are healthy, and that
  collector logs show both execution feeds becoming subscription-ready. After
  an hour, confirm one planned rotation completes, the closed leg reconnects,
  and both legs return to subscription-ready without a new source epoch.
- Request the market catalog, create one Tick and one TRev series, wait for
  `ready`, and confirm both snapshots are non-empty.
- Manually smoke-test rendering, zoom, crosshair, resize, two panels sharing a
  series, settings persistence, and disconnect/reconnect behavior.
- Alert on unhealthy/restarting containers and host disk usage above 80%.
- Keep daily off-host snapshots of the PostgreSQL and ClickHouse volumes, and
  periodically test restoration.

Rollback the footprint collectors with the same handoff using a retained
previous lock-aware immutable image:

```sh
./scripts/rollout-footprints.sh --image crypto-scanner:<previous-commit> \
  --exchange all
```

Roll back the other backend and frontend services through their normal image
deployment path. Keep the PostgreSQL and ClickHouse volumes; never reset
production data as a rollback. The legacy image from before lock-aware
collectors cannot use this rolling path; recover from an initial deployment
problem by fixing forward unless a deliberate collector restart gap is
acceptable.

Rolling handoff avoids a planned socket gap when both versions become ready and
the old process drains cleanly. It cannot guarantee uninterrupted collection
during a host, Docker daemon, network, storage, or exchange outage. Common
failures still require historical reconciliation.
