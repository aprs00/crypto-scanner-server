# Free execution recovery

Recovery is automatic for Binance and Bybit. Hyperliquid has no historical downloader or paid S3 path.

The collector first bridges a reconnect using the latest 1,000 public executions. Both the durable tail and the first new socket execution must occur in that response with identical fields. Binance IDs must increase, but need not be consecutive: its public API excludes insurance-fund and ADL trades. Bybit preserves response order and allows repeated sequence values across distinct executions. A window that cannot prove continuity creates an explicit gap rather than pretending the feed is complete.

An independent worker compares the last ten closed minutes, excluding a two-minute delivery grace period, with exchange candles every five minutes. Binance supplies trade count and volume; Bybit supplies volume. Candle mismatches are evidence of a possible gap, never replacement trade data.

The archive worker durably queues every closed day in the last 30 days, including days with no captured data. It polls missing or failed archives at 15-minute intervals and prioritizes recent history. It downloads only these public sources:

- Binance individual futures trades: `https://data.binance.vision/data/futures/um/daily/trades/BTCUSDT/`
- Bybit public trades: `https://public.bybit.com/trading/BTCUSDT/`

Compressed downloads cap at 2 GiB, decompression at 16 GiB, and parsing at 50 million rows per day. Cancellation is checked while parsing. Parsing uses a disposable SQLite file with a unique identity constraint, rather than retaining millions of strings in memory.

Before publishing an archive revision, recovery verifies:

1. Binance's published SHA-256 checksum; both venues also get a stored content digest.
2. Required columns, exact decimals, positive values, market, date, sides, and unique execution IDs.
3. Increasing Binance IDs or preserved Bybit CSV order; every captured overlap must match immutable values and appear in the same order.
4. Full-day volume, and Binance trade count, against the public daily candle.
5. Successful durable upload of the entire revision to ClickHouse, followed by another overlap check against capture.

Conflicting archives, unavailable files, specification changes that cannot be established, download failures, and mismatched evidence remain pending with their error in the control database. Existing charts continue using their pinned history. Revision publication and chart publication are separate: a corrected chart gets a new ID only after rebuilding.

Public archives are delayed and the latest-trades endpoints are short windows. Consequently, Binance/Bybit completeness is an eventual repair target, conditional on the exchange making the missing executions available. `verifiedThroughMs` states the actual verified boundary; it is not a claim that every current live trade has already been independently audited. Hyperliquid gaps remain permanently marked within the retention window.

Authoritative references: [Binance connection lifecycle](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Connect), [Binance public archives](https://github.com/binance/binance-public-data), [Bybit public trades](https://bybit-exchange.github.io/docs/v5/websocket/public/trade), [Bybit connection heartbeat](https://bybit-exchange.github.io/docs/v5/ws/connect), [Hyperliquid WebSockets](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket), and [Hyperliquid historical data](https://hyperliquid.gitbook.io/hyperliquid-docs/historical-data).
