# Rust footprint production release

The Rust footprint services and frontend were released on 14 September 2026. Production checks and the requested manual deletion of legacy footprint storage completed on 15 September. No automatic cleanup code was added.

## Deployed versions

| Component | Version |
| --- | --- |
| Backend source | `5325c99369e3b4742c9e5538558b9a67719f067d` |
| Rust collector/API image | `crypto-scanner-footprints:5325c99` |
| Scanner application image | `crypto-scanner:5325c99` |
| Frontend source | `f20a81d8cb9cc749555d8077b2dc0cb80c32c6bd` |
| Frontend production deployment | GitHub deployment `6440822461`, Vercel status `success` |
| Footprint storage | Fresh `footprints_rust` database and four Rust volumes |

Both backend images were built natively for the production ARM64 host. The footprint runtime has no Python executable. Image IDs and observations are in [the production artifact](../benchmarks/results/footprint-rust-production-2026-09-15.json).

The three Rust collectors established both socket connections and durable delivery before the old collectors stopped. The running Rust containers were then renamed to their canonical names without restarting capture. The gateway routes footprint HTTP and WebSocket requests to Rust and other requests to the scanner application. The external TLS proxy was preserved.

## Production verification

At 15:00 UTC on 15 September, all three collectors were ready, owned their journals, and had both socket connections available. They and the API had zero container restarts since startup about 23 hours earlier. Small active journal queues continued draining into ClickHouse.

Public checks passed after legacy deletion:

- `https://api.crypto-scanner.xyz/api/v1/footprints/markets` returned all three markets with the expected frontend CORS origin.
- Binance and Bybit one-day Tick charts and the Hyperliquid TRev chart returned ready series, historical bars, and active bars.
- `wss://api.crypto-scanner.xyz/ws/v1/charts/` acknowledged subscriptions and delivered live updates for every exchange. An earlier Bybit check also exercised HTTP resnapshot after `CHART_RESYNC_REQUIRED`, followed by live updates and eventual replacement with the repaired generation.
- The scanner bootstrap endpoint and `https://www.crypto-scanner.xyz/` returned HTTP 200.
- Stored live-capture ordinals were unique and contiguous: 3,530,561 Binance, 1,996,518 Bybit, and 339,720 Hyperliquid executions at the query snapshot. This checks journal delivery, not completeness of exchange data.

## Historical coverage

All 30 closed UTC days from **16 August through 14 September 2026** were downloaded and verified through free public archives. There were no pending closed-day jobs at inspection.

| Exchange | Verified days | Executions |
| --- | ---: | ---: |
| Binance | 30 | 104,721,931 |
| Bybit | 30 | 59,620,293 |
| Total | 60 exchange-days | 164,342,224 |

Binance rejected exchange frames around 13:35 UTC on 15 September, and the independent minute audit marked that minute for repair. Current-day chart coverage correctly remains `repair_pending`; the free daily archive must be published before that interval can be verified. Bybit coverage was `live` after its repaired chart generation became ready. Hyperliquid recorded a connection gap and reports `gap_recorded`; historical recovery remains disabled as requested.

## Manual legacy deletion

After public checks passed, the legacy maintenance and recovery containers were stopped and removed. The temporary scanner verification container was also removed. Cleanup then removed only:

- ClickHouse database `footprints`, containing the old `raw_executions` and `chart_bars` tables.
- PostgreSQL tables `cs_chart_series`, `cs_instrument_specs`, `cs_trade_recovery_days`, and `cs_footprint_markets`, plus five `footprints` migration records. Foreign keys were inspected first; the drop used no `CASCADE`.
- Docker volumes `crypto-scanner-server_binance_footprint_outbox`, `crypto-scanner-server_bybit_footprint_outbox`, and `crypto-scanner-server_trev_prepared_tapes`.

Their absence was verified. The shared ClickHouse/PostgreSQL volumes, all four Rust volumes, and 34 unrelated PostgreSQL tables remain. Reverting an old image cannot restore the deleted footprint history. Future collector updates must use the staged procedure in [operations](footprints-production.md), because the promoted containers retain their Compose one-off labels.

## Continuing observation

A 72-hour observation process started at 15:01 UTC on 15 September inside `cs-footprint-api`, writing `/var/lib/footprints/soak-release-5325c99.jsonl` on its persistent volume. Initial samples show all four services ready. This run is not yet complete; an API container replacement would interrupt the process, while preserving the observation file.

No 100% uptime result or end-to-end speedup is claimed. The same-hardware comparison of full real-history workloads with the previous implementation remains unmeasured. The implementation and failure-test results are recorded in [validation](footprints-validation.md).
