# Tick and TRev charts

V0 supports two execution-driven chart types:

- **Tick:** close after exactly `tradesPerBar` executions.
- **TRev:** close on `continuationTicks` and `reversalTicks` price moves.

There are no time, range, or volume chart abstractions in this version.

## Runtime

```text
exchange WebSocket
    -> bounded in-memory queue
    -> ClickHouse raw_executions
    -> Tick/TRev engine
    -> ClickHouse chart_bars + PostgreSQL checkpoint
    -> HTTP history + WebSocket live updates
```

ClickHouse owns the exact execution tape and closed chart bars. PostgreSQL owns
market/specification metadata, the requested series configuration, lifecycle
status, and the latest engine checkpoint. Redis/Channels carries replaceable
live updates only.

A chart request uses the internal `CHART_DEFAULT_LOOKBACK_DAYS` window. The
client chooses only the Tick trade count or the two TRev thresholds. Ready
series are advanced incrementally instead of being replayed for each update.
Tick trade counts and TRev thresholds are integers from 1 through 4,000. One
market may have at most 16 requested configurations with a live 30-minute
lease. The client renews a lease by repeating the idempotent series request or
subscribing to its WebSocket; idle rows are removed and their orphaned
ClickHouse bars age out under the existing table TTL.

The maintenance process recovers interrupted builds at startup. On each loop
it advances ready series, then claims and builds at most one queued series.
The maintenance process holds a dedicated PostgreSQL advisory lock and exits
if another copy already owns it or its lock connection is lost.
Failed builds remain failed and later requests only observe that state, so
readiness polling cannot start a replay loop. V0 retry is an explicit operator
action such as deleting the failed series row. A build interrupted by a
transient ClickHouse network or overload error is the exception: it returns to
the existing queue and retries after a fixed five-second delay.

## Storage

`raw_executions` contains integer price and quantity atoms, numeric source
order, side, and instrument specification. It does not duplicate market
strings or collector bookkeeping already held by the running process.

`chart_bars` is a `ReplacingMergeTree` keyed by `(series_id, bar_index)`.
Sparse footprint levels are stored as parallel tick, buy-unit, and sell-unit
arrays; empty price levels are not materialized. Both tables retain 31 days.

Collectors and maintenance create both tables with idempotent `CREATE ... IF
NOT EXISTS` statements. V0 has no ClickHouse migration ledger or manual
migration command.

Because the initial schema is intentionally rewritten during V0 development,
an already-applied local schema is not upgraded in place. Stop the footprint
services, run `python manage.py migrate footprints zero` followed by
`python manage.py migrate footprints`, and drop only the configured ClickHouse
footprint database before restarting. This deletes local footprint data; do not
remove the shared PostgreSQL volume.

## HTTP API

List markets:

```http
GET /api/v1/footprints/markets
```

Each market contains `marketKey`, `exchange`, `symbol`, `baseAsset`, and
`currentTickSize`.

Create or reuse a Tick series:

```http
POST /api/v1/footprints/chart-series
Content-Type: application/json

{
  "marketKey": "binance:perpetual:BTCUSDT",
  "chartType": "tick",
  "tradesPerBar": 500
}
```

Create or reuse a TRev series:

```json
{
  "marketKey": "binance:perpetual:BTCUSDT",
  "chartType": "trev",
  "continuationTicks": 20,
  "reversalTicks": 30
}
```

The response is `{ "seriesKey", "status", "errorCode" }`. Queued and
building series return HTTP 202. A seventeenth live configuration for one
market returns HTTP 429 with `SERIES_CAPACITY_REACHED`. Request bodies are
limited to 4 KiB.

Read the latest 500 closed bars and current active bar:

```http
GET /api/v1/footprints/chart-bars?seriesKey=...
```

The response is `{ "seriesKey", "bars", "activeBar" }`. It contains the
newest suffix of at most 500 closed bars and at most 40,000 native levels in
total, including the active bar. V0 has no pagination or price aggregation.

## WebSocket

Connect to `/ws/v1/charts/` and subscribe with:

```json
{"op":"subscribe","seriesKey":"..."}
```

Inbound subscription frames are text-only and limited to 2 KiB.

`subscribed`, `update`, and `error` messages are scoped by `seriesKey`.
Updates contain newly closed bars and an absolute active-bar replacement.
Catch-up updates retain the latest 500 bars and send at most 50 bars and 8,000
native levels per message, including the repeated active bar. A native bar is
terminally rejected with `CHART_BAR_TOO_DENSE` above 4,000 levels. One TRev
execution may expand through at most 500 synthetic boundaries. The client
refetches the bounded HTTP snapshot if bar indexes reveal a missed live
publication.

## Failure behavior

Collectors keep retrying the same admitted rows while ClickHouse is
temporarily unavailable. They stop their exchange stream while retrying so the
bounded queue cannot silently overflow. On SIGTERM they retry and drain for up
to 45 seconds, then log the exact number of unconfirmed executions. A forced
process stop during an outage can still lose in-memory rows because V0
intentionally has no WAL.

A reconnect receives a new source epoch, which creates a chart segment
boundary. Recent exact venue identities are loaded from ClickHouse before that
epoch is allocated so replayed trades are not counted twice after a process
restart.
