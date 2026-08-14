# Footprint V0 production deployment

This release is designed for one Docker host. It deliberately has no account
system, WAL/backfill service, LRU cache, Prometheus stack, or bundled reverse
proxy. Reconnect/source changes are rendered as explicit chart gaps.

## Before the first release

Footprints have not shipped yet, so initialize PostgreSQL and ClickHouse from
the final baseline on fresh production volumes. After this release, never edit
`footprints/migrations/0001_initial.py` or the baseline ClickHouse DDL; add
forward migrations instead.

Build and tag the application with the exact commit, for example:

```sh
docker build --target runtime -t crypto-scanner:$(git rev-parse --short=12 HEAD) .
export APP_IMAGE=crypto-scanner:$(git rev-parse --short=12 HEAD)
```

Keep the deployment environment in the repository's ignored `.env` file. At
minimum use production values for
`SECRET_KEY`, PostgreSQL credentials, a non-empty `CLICKHOUSE_PASSWORD`,
`DJANGO_ALLOWED_HOSTS`, and:

```dotenv
DEBUG=false
STORE_TO_DB=true
POSTGRES_HOST=db
POSTGRES_PORT=5432
```

`DJANGO_ALLOWED_HOSTS` contains hostnames only, without schemes or paths. Include
the API hostname and every frontend hostname allowed to open chart WebSockets.

## Deploy

The production override requires an immutable `APP_IMAGE`, removes application
source mounts, enables Django's proxy/TLS cookie settings, binds Daphne to
localhost, and does not publish Redis.

```sh
docker compose -f docker-compose.yml -f docker-compose.prod.yml run --rm app python manage.py migrate
docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d
```

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

- Confirm all three footprint collectors and maintenance are healthy.
- Request the market catalog, create one Tick and one TRev series, wait for
  `ready`, and confirm both snapshots are non-empty.
- Manually smoke-test rendering, zoom, crosshair, resize, two panels sharing a
  series, settings persistence, and disconnect/reconnect behavior.
- Alert on unhealthy/restarting containers and host disk usage above 80%.
- Keep daily off-host snapshots of the PostgreSQL and ClickHouse volumes, and
  periodically test restoration.

Rollback by restoring the previous backend and frontend image tags. Keep the
PostgreSQL and ClickHouse volumes; never reset production data as a rollback.
