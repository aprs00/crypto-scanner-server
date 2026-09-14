# Tick and TRev chart data

Footprint is a standalone Rust service in `native/footprints`. It captures Binance BTCUSDT perpetual, Bybit BTCUSDT perpetual, and Hyperliquid BTC perpetual. Other scanner services are outside this rewrite.

Three collector processes each keep two live sockets, normalize exact decimal prices and quantities to integers, and group-commit accepted executions into a SQLite WAL journal with `synchronous=FULL`. The normal commit interval is 10 ms, with earlier flushes for large batches. A separate delivery task drains batches to ClickHouse. A ClickHouse outage therefore accumulates durable local work without closing exchange connections.

The collector journal defaults to 2 GiB and protects 5 GiB of free disk. Exhaustion stops admission, retains the pending batch, reports unhealthy readiness, and records an unverified interval when capture resumes. It never overwrites queued executions. Size this buffer for the host's measured BTC traffic and expected outages. A single host, finite disk, process crashes before a group commit, and exchange history availability cannot provide an absolute zero-loss guarantee.

Binance and Bybit reconnect gaps first try the free latest-1,000-trade REST window with matching endpoints. Larger or uncertain gaps remain visible while free daily archives are verified and replayed. Hyperliquid uses socket overlap and reconnects; it never requests paid historical data. Uncovered Hyperliquid intervals remain marked as gaps.

A local filesystem lock permits one writer per exchange volume. No PostgreSQL advisory lease, Redis channel, Django process, Python fallback, or PyO3 extension participates in footprint capture or serving. Locks and journals require persistent local filesystems; network filesystems are unsupported.

## Connection lifecycle

Both sockets are subscribed and independently monitored. Planned promotion requires an identical execution in the peer's buffer. Capture switches to that peer before closing the retiring socket. An unavailable peer postpones a planned rotation. An unexpected disconnect uses buffered overlap or recent REST before recording a gap.

Binance rotates the oldest connection approximately every 11.5 hours, keeping normal socket ages below its documented 24-hour connection lifetime. A 23-hour age check also handles changes in leadership after failures. Bybit and Hyperliquid have no artificial two-hour disconnect. Binance uses protocol ping/pong, Bybit JSON ping every 20 seconds, and Hyperliquid JSON ping every 30 seconds; unanswered client heartbeats expire after 10 seconds. Retry delays use jitter and cap at 30 seconds. A quiet leading feed switches only after its peer has demonstrably advanced for five seconds. Normal differences in arrival time after a quiet market do not trigger a reconnect.

## Persistence and charts

`footprints_rust.executions` stores executions with stable source ordinals. Retries retain those ordinals; readers remove repeated records before projection even after ClickHouse's finite deduplication window expires. Conflicting records fail visibly. Bybit UUIDs identify executions but never determine order; its sequence is not treated as a gap counter. Timestamp regressions do not reorder the source tape.

The chart server uses its own SQLite WAL database for configurations, checkpoints, leases, recovery jobs, and archive revisions. Raw trades and bars use separate ClickHouse tables with fsync enabled. Bars are acknowledged before advancing a checkpoint. Reads are bounded by the committed checkpoint, so a crash cannot expose partially committed chart output.

Tick consumes individual executions. TRev reuses the existing Rust mathematical engine and prepared runs of unchanged prices. Immutable chunks have checksums and an 8 GiB disposable disk cache; live charts share reads during each projection pass. One live worker and one background worker prevent historical builds from monopolizing chart projection. Wire arithmetic checks JavaScript-safe integer bounds.

Limits remain 16 leased configurations per market, 1–4,000 trades/ticks per parameter, 4,000 price levels per bar, and 500 synthetic boundary expansions per execution. Snapshots cap at 500 bars/40,000 levels; live updates cap at 50 bars/8,000 levels. Larger updates request an HTTP resnapshot. A chart configuration failure does not stop raw capture.

TRev maintains a continuous generation with up to 30 days of visible history; lookback changes only the visible range. Tick's initial anchor depends on its requested lookback. Data retention is 31 days. A checkpoint beyond raw retention triggers a fresh generation instead of joining across expired history. Delivery rejects a journal whose stored position conflicts with ClickHouse. New storage and public series IDs deliberately replace the old Python history.

## API

The gateway retains port 8000 and routes these paths directly to Rust:

- `GET /api/v1/footprints/markets`
- `POST /api/v1/footprints/chart-series`
- `GET /api/v1/footprints/chart-bars`
- `/ws/v1/charts/`

Bar and series fields retain their existing contract. Responses add `coverage: {status, verifiedThroughMs}`. Status is `live`, `repair_pending`, or `gap_recorded`; verification is only claimed for the contiguous archive history in that chart's generation. A separate scoped WebSocket coverage message updates status even while a source has stopped producing trades.

A repair builds a separate generation while the old chart remains available. Once rebuilt history catches up, one SQLite transaction switches the active generation. Subscribers receive `CHART_SERIES_REPLACED` and request the new ID. Public IDs and old cached bars are never reused for different history.

The API enforces configured browser origins and bounded requests. `/health/*`, `/metrics`, and `/admin/*` are private container endpoints, excluded from the gateway's footprint routes.
