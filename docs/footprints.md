# Tick and TRev charts

V0 supports two execution-driven chart types:

- **Tick:** close after exactly `tradesPerBar` executions.
- **TRev:** close on `continuationTicks` and `reversalTicks` price moves.

There are no time, range, or volume chart abstractions in this version.

## Runtime

```text
two exchange WebSockets (leader + hot standby)
    -> bounded handoff buffer + shared execution-identity dedupe
    -> bounded in-memory queue
    -> ClickHouse raw_executions
    -> bounded Redis maintenance wakeup
    -> Tick/TRev engine
    -> ClickHouse chart_bars + PostgreSQL checkpoint
    -> HTTP history + WebSocket live updates
```

ClickHouse owns the exact execution tape and closed chart bars. PostgreSQL owns
market/specification metadata, the requested series configuration, lifecycle
status, and the latest engine checkpoint. Redis carries bounded maintenance
wakeups and replaceable live updates; ClickHouse cursors remain the durable
catch-up source if a wakeup is missed.

Each collector keeps a leader WebSocket and a continuously consumed hot
standby. If the leader disconnects, the standby replays its bounded buffer
through the shared trade-identity dedupe, becomes leader, and the failed socket
reconnects independently. Both connections share the existing identity cache
and ClickHouse writer. The collector also rotates the oldest ready connection
once per hour, so each leg normally lives for about two hours. A planned
rotation is postponed until both legs are subscription-ready and uses the same
handoff and dedupe path as an isolated disconnect.

HTTP chart snapshots use response gzip when the client accepts it so dense
footprint-level JSON does not dominate transfer time. The snapshot reader keeps
validated ClickHouse level arrays in their native layout until JSON encoding,
avoiding one temporary Python object per footprint level.

A chart request supplies an integer `lookbackDays` from 1 through 30. Tick
construction starts at that lookback's anchor. TRev has one continuous series
per market and continuation/reversal threshold pair; its lookback only filters
the displayed history. A new TRev configuration bootstraps forward from a fixed
30-day anchor, then keeps that origin and its candle indexes. Changing lookback
does not rebuild TRev or change its recent candle boundaries.

Engine state and the raw cursor are checkpointed every 100,000 executions after
durable chart-bar writes, so interrupted builds resume without starting over.
Opening a saved TRev series requests `catching_up`: the maintenance coordinator
restores its checkpoint and reads only the subsequent trade tail, sharing
market reads with other warm configurations. It becomes `ready` after reaching
the latest cursor observed for a catch-up pass. New arrivals continue through
live updates. A completed cold TRev bootstrap also catches up before becoming
ready. Cold builds run separately in the existing worker pool.
Tick trade counts and TRev thresholds are integers from 1 through 4,000. One
market may have at most 16 requested configurations with a live 30-minute
lease. The client renews a lease by repeating the idempotent series request or
subscribing to its WebSocket. Ready Tick series remain cached for 24 hours.
Retained TRev series keep advancing even without viewers and do not expire
after 24 hours. Viewing leases still determine which idle ready configuration
can be evicted when market capacity is needed. Orphaned ClickHouse bars age out
under the existing table TTL. The default TRev 314/471 configuration is kept
warm for every enabled market.

The maintenance process recovers interrupted builds at startup. A durable raw
insert wakes it immediately for the affected markets, and a series request
wakes the queued builder. A five-second full catch-up covers lost Redis
wakeups. Each build claim includes queued configurations with the same market
and lookback anchor. Two worker processes run independent replay batches while
the coordinator continues advancing ready series. The maintenance process holds
a dedicated PostgreSQL advisory lock and exits if another copy already owns it
or its lock connection is lost.
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

For the continuous TRev upgrade, stop chart API and maintenance processes, run
`python manage.py migrate footprints`, and deploy the matching backend and
frontend before restarting chart services. Migrations `0004` and `0005` replace
old lookback-specific TRev series with one queued 30-day generation per threshold
pair. This causes a one-time forward bootstrap. Tick series and raw executions
are preserved; no ClickHouse reset is required. Fresh series keys invalidate
old client caches. The migration must run before the updated chart services.

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
  "lookbackDays": 2,
  "tradesPerBar": 500
}
```

Create or reuse a TRev series:

```json
{
  "marketKey": "binance:perpetual:BTCUSDT",
  "chartType": "trev",
  "lookbackDays": 7,
  "continuationTicks": 20,
  "reversalTicks": 30
}
```

The response is `{ "seriesKey", "status", "errorCode" }`. Queued, building,
and catching-up series return HTTP 202. A seventeenth live configuration for one
market returns HTTP 429 with `SERIES_CAPACITY_REACHED`. Request bodies are
limited to 4 KiB.

Read the latest 500 closed bars and current active bar:

```http
GET /api/v1/footprints/chart-bars?seriesKey=...&lookbackDays=5
```

The response is `{ "seriesKey", "bars", "activeBar", "hasMore",
"nextBeforeBarIndex", "historyStartBarIndex" }`. It contains at most 500
closed bars and 40,000 native levels, including the active bar. TRev snapshots
default to five display days when `lookbackDays` is omitted. Tick reads keep
their existing series-defined history window.

While the series is queued, building, or catching up, the latest snapshot
returns HTTP 409 with `CHART_SERIES_NOT_READY` and its status. The frontend
retries this GET once per second using the same series key; it does not repeat
POST to poll readiness. Initial rendering waits for a successful snapshot,
including when IndexedDB or query caches contain old candles. Failed series
remain terminal. Regular five-minute POST lease renewal also discovers replaced
series and requests catch-up without hiding an already visible chart.

Closed snapshot bars always have indexes below the checkpoint's `nextBarIndex`,
and the active bar is restored from that same checkpoint. Bar writes not yet
covered by the checkpoint remain invisible. `historyStartBarIndex` bounds
retained history and the requested lookback, including candles straddling the
cutoff; subsequent indexes remain contiguous despite timestamp regressions.
Read older pages with the returned cursor:

```http
GET /api/v1/footprints/chart-bars?seriesKey=...&lookbackDays=5&beforeBarIndex=1200&limit=500
```

Historical pages omit the active bar and can be read during catch-up. They
read persisted candles and never start a trade replay. The browser caches downloaded closed
bars in IndexedDB and requests another page when the user scrolls near the
left edge. IndexedDB is a disposable per-device cache; ClickHouse remains the
authoritative complete history.

## WebSocket

Connect to `/ws/v1/charts/` and subscribe with:

```json
{"op":"subscribe","seriesKey":"..."}
```

Inbound subscription frames are text-only and limited to 2 KiB.

`subscribed`, `update`, and `error` messages are scoped by `seriesKey`.
Updates contain newly closed bars and an absolute active-bar replacement.
Catch-up publications retain the latest 500 bars for WebSocket delivery while
all closed bars are persisted. Messages contain at most 50 bars and 8,000
native levels, including the repeated active bar. A native bar is
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

An individual socket reconnect does not change the source epoch when the
standby buffer overlaps the leader's latest trades. An unverified handoff or
loss of both sockets advances the source epoch, giving the chart an explicit
segment boundary rather than claiming continuity; buffered observations still
pass through the identity dedupe. Recent exact venue identities are loaded from
ClickHouse before a restarted generation so replayed trades are not counted
twice.

Each exchange collector also holds a dedicated PostgreSQL advisory lock before
it allocates a source epoch or writes executions. A second process can connect
both sockets as a bounded warm standby, then take ownership and replay its
overlap only after the active writer drains and releases the lock. Before
replay, it prefers the ready socket whose buffer shares a recent persisted
trade identity. Production rollouts require that overlap at both handoffs and
use this path instead of restarting both sockets at once. A crash recovery is
still allowed to start without overlap, but its new source epoch explicitly
marks continuity as unverified.

Leadership is checked before activation, while collecting, and while draining
on shutdown. An insert already in flight when the PostgreSQL lock session fails
cannot be recalled, so a duplicate remains possible in that narrow failure
window and must be detected by reconciliation.

The two sockets share the collector process, host, network path, storage, and
exchange endpoint. They protect against an isolated WebSocket failure; they do
not protect against a host failure, a common upstream outage, or an outage
longer than the venue's available overlap. A warm process protects planned
collector replacement only while both versions are healthy and subscribed. It
does not replace historical reconciliation or backfill. If both connections
are lost, the collector exposes the break with a new source epoch instead of
claiming a seamless tape.
