# Rust rewrite validation — 14 September 2026

The rewrite is implemented in `native/footprints`. Footprint capture, recovery, chart projection, HTTP/WebSocket serving, tests, benchmarks, and operational commands no longer depend on Python. Other scanner applications retain their separate runtime.

This records implementation validation before release. The user subsequently authorized deployment: production cutover completed on 14 September, and production verification plus manual legacy-data deletion completed on 15 September. See [the release record](footprints-release-2026-09-15.md). The 72-hour observation is running; a comparison of full real-history workloads remains unmeasured.

The local development host's legacy footprint containers still invoke the removed `footprint_trades` Python command and are failing. They have not been replaced by the validation containers. The source checkout and old bind-mounted runtime are therefore incompatible until the coordinated rollout; retaining the old runtime requires its matching immutable image/source checkout. Production uses a separate host and immutable images.

## Completed checks

- 18 Rust tests pass, including 24 frozen Tick/TRev oracle tapes with 7,200 executions, checkpoint replay, exact decimals, duplicate/conflict handling, storage pressure, abrupt process exit, writer ownership, and compacted tape resumption.
- The real ClickHouse integration test passes. It checks retry deduplication beyond the database window, hidden uncheckpointed bars, HTTP errors, a real WebSocket subscription, atomic replacement after repair, expired checkpoint rebasing, irrelevant archive updates, and rejection of lost control storage.
- Clippy passes with warnings denied. The release binary and separate Docker image build with Rust 1.94 and locked dependencies.
- The Docker runtime uses UID 10001 and has no Python executable. Startup, readiness, the markets endpoint, archive publication, and clean shutdown also pass with a read-only root and dropped capabilities. Production Compose and nginx configuration checks pass.
- The sibling frontend's 34 footprint tests, build, types, and footprint lint checks pass. Its chart status now distinguishes pending repair from recorded gaps and responds to coverage and resnapshot messages.

## Live and failure testing

An isolated ClickHouse 26.3.17.56 container was limited to two CPUs and 2 GiB. All three release collectors captured live BTC trades through two sockets each. The final run recorded no gaps or reconnects before intentional shutdown.

While ClickHouse was paused, the collectors remained ready and retained 3,299 Binance, 2,217 Bybit, and 183 Hyperliquid executions in their journals. The outage backlog drained after ClickHouse resumed. Final stored counts exactly match journal positions: 17,718 Binance, 12,064 Bybit, and 1,154 Hyperliquid executions, with no duplicates, missing ordinals, or remaining queued records.

A warm Bybit candidate acquired the journal after the old writer drained, with proven overlap and zero recorded gaps. Restarting that promoted process with `--standby` and the same instance ID resumed writer ownership without another promotion request.

Automatic recovery downloaded, validated, uploaded, and published nine real daily archives containing **18,622,460 executions**: Binance 9–13 September and Bybit 10–13 September. Binance checksums and count/volume evidence, Bybit volume evidence, and archive identities/order all passed. This exercises the complete recovery path rather than only the CSV parser. Hyperliquid has no archive recovery path.

The HTTP API produced ready Tick charts for Binance and Bybit and a ready TRev chart for Hyperliquid using captured and repaired data. Coverage stayed explicit during bootstrap.

## Performance and remaining gates

On an AMD Ryzen 5 7600X, one deterministic one-million-execution tape took 13.42 ms to prepare, 13.98 ms through the scalar Rust engine, and 7.09 ms through prepared projection. The old implementation already used a Rust TRev core: these numbers are **not** a Python-to-Rust or end-to-end speedup measurement. They exclude network, ClickHouse, and cold-history loading.

The 72-hour soak and a comparison of full 30-day TRev and one-day Tick workloads against the previous implementation on the same hardware/history have not been completed. The user subsequently authorized production release and manual deletion of legacy footprint storage after verification. See [operations](footprints-production.md); this authorization does not change the measurements above into a completed soak.

Detailed counts, digests, health observations, and benchmark scope are stored in [the validation artifact](../benchmarks/results/footprint-rust-validation-2026-09-14.json).
