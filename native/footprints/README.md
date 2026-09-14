# Footprints in Rust

Standalone BTC execution capture, free historical recovery, and Tick/TRev chart serving. The runtime, build, tests, and operational commands have no Python dependency.

```sh
cd native/footprints
cargo test --locked
cargo clippy --all-targets -- -D warnings
cargo build --locked --release
./target/release/footprints bench --rows 1000000
```

The Rust 1.94 toolchain and dependency lockfile are pinned. `Dockerfile.footprints` builds a Debian runtime with the executable and CA certificates. The rest of the scanner has its separate image.

Commands:

- `collect --exchange binance|bybit|hyperliquid`: two exchange sockets and one durable journal writer.
- `serve`: HTTP/WebSocket API, live projection, one background projection worker, archive recovery, and independent five-minute audits.
- `migrate`: create the fresh ClickHouse schema. `serve` also runs this idempotently.
- `health [--ready] [--writer] [--overlap] [--url URL]`: machine-readable health and an appropriate exit code.
- `control promote|drain [--url URL]`: private collector administration.
- `archive-check --exchange binance|bybit --date YYYY-MM-DD --file PATH`: parse a downloaded public archive and report exact totals; independent checksum/candle verification remains part of automatic recovery.
- `bench --rows N`: deterministic scalar/prepared Rust comparison, with exact state parity. Excludes ClickHouse and network time.
- `soak --hours 72 --output /var/lib/footprints/soak.jsonl`: record collector and server health every five seconds, plus a final completion/availability summary. Run inside the API container. Interrupted runs are not completed soaks.

See [architecture and guarantees](../../docs/footprints.md), [recovery](../../docs/trade-recovery.md), [operations and cutover](../../docs/footprints-production.md), [validation results](../../docs/footprints-validation.md), and [candle semantics](../../docs/footprint-algorithm-semantics.md).

The frozen engine oracle comes from commit `9e1a361fdfda973475c2210768ccecadc601e033`: 24 Tick/TRev tapes containing 7,200 executions, changing specifications, segment breaks, boundary crossings, and regressing timestamps. These are static JSON fixtures; the test runner does not execute Python.

The ClickHouse integration test requires a disposable server:

```sh
docker run --rm -d --name footprint-test -p 127.0.0.1:18123:8123 \
  -e CLICKHOUSE_SKIP_USER_SETUP=1 clickhouse:26.3.17.56
FOOTPRINT_TEST_CLICKHOUSE=http://127.0.0.1:18123 \
  cargo test --test integration -- --ignored
docker stop footprint-test
```

It creates a uniquely named database and checks replay beyond ClickHouse's deduplication window, checkpoint visibility, repaired-generation replacement, HTTP validation, and the actual WebSocket handshake.
