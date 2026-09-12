# Native TREV cold builds

One sequential Rust engine processes each configuration. PyO3 passes bounded
market batches across the Python boundary; it releases the GIL during decoding
and calculation. Tick and live projection continue to use the Python engines.

`TREV_COLD_BACKEND=rust` selects this path. New Docker images build and install
the native wheel and select Rust. Source-only environments default to `python`;
install the wheel before selecting Rust. Set `TREV_COLD_BACKEND=python` to roll
back: both implementations use the existing checkpoint JSON and bar schema.
There is no migration, series replacement, or mandatory rebuild.

The reader uses the existing raw/reconciled SQL with RowBinary transport. Rust
validates order, duplicates, specification divisors, sides and scalar values.
Archive/live seam verification remains in Python. Each market batch is decoded
once for all grouped configurations. TREV does not create a Python object for
each execution or footprint level. Python still owns ClickHouse insertion,
checkpoint transactions, stopping and retries. Inserts accumulate across decoded
fragments up to 2,000 bars. A single native configuration checkpoints every
1,000,000 executions; grouped builds and Python fallback keep the 100,000
execution checkpoint interval. Stop requests are still checked every 100,000
executions and flush pending bars before saving the checkpoint.

Prices are signed 64-bit integers, raw quantities are unsigned 64-bit integers,
and volume sums use checked unsigned 128-bit arithmetic. A native call changes
state only on success. Values outside native capacity select Python from the
unchanged checkpoint/batch; there is no rounding or integer wrapping. Execution
compression is deliberately absent.

The current 30-day initial history and retention policy are preserved. Existing
series continue from checkpoints even when their original trades have expired.
The existing checkpoint-expiry guard remains aligned with the schema's 31-day
physical TTL (30 usable days plus grace); this change does not recover deleted
trades or replay from an origin older than retained data without a checkpoint.

## Build

The Dockerfile builds for its target architecture and copies only the installed
Python environment into runtime. Rust and maturin are build dependencies.

For a local wheel, with Rust 1.78+ and Python installed:

```sh
python -m pip install maturin==1.9.6
maturin build --release --locked --manifest-path native/trev/Cargo.toml
python -m pip install native/trev/target/wheels/trev_native-*.whl
```

The wheel uses Python's stable ABI (3.10+); both Linux x86_64 and ARM64 wheels
were built and loaded, including ARM64 Python 3.13.15. Build on each architecture
for deployment, or use maturin's Zig cross-compilation support.

## Benchmark

Run from the repository root with the installed wheel. Input is RowBinary from
these exact columns in canonical source order:

```sql
SELECT source_epoch, source_sequence, batch_index, timestamp_us, trade_id,
       price_atoms, quantity_atoms, is_taker_buy, instrument_spec_id
FROM raw_executions
WHERE market_id = ... AND timestamp_us >= ...
ORDER BY market_id, source_epoch, source_sequence, batch_index, timestamp_us, trade_id
```

Supply the real divisors for every specification in the stream:

```sh
python -m benchmarks.trev_cold trades.rowbinary \
  --spec 2:100000000000:1000000000 --continuation 500 --reversal 700 \
  --convert-bars > rust.jsonl
python -m benchmarks.trev_cold trades.rowbinary \
  --spec 2:100000000000:1000000000 --backend python > python.jsonl
python -m benchmarks.trev_cold trades.rowbinary \
  --spec 2:100000000000:1000000000 --baseline python.jsonl > comparison.jsonl
```

`--max-rows 1000000` gives a short representative run. `--profile profile.bin`
adds cProfile output; collect that separately from unprofiled timings. The
benchmark reports read, decode, calculation, output-conversion and verification
time separately. `-` reads RowBinary from stdin.

The original frozen benchmark tape has 33-byte normalized records
`<IqqQBI`: epoch, timestamp, price tick, quantity units, buy, specification ID.
Convert it without changing engine input using:

```sh
cargo run --release --locked --manifest-path native/trev/Cargo.toml \
  --bin prepare_fixture -- frozen.bin frozen.rowbinary
python -m benchmarks.trev_cold frozen.rowbinary --baseline production-baseline.jsonl
```

This conversion reconstructs increasing source identities and uses divisors
1/1. It is for calculation comparisons, not an archive replacement. Large tapes
and raw trades stay outside git because source retention is rolling.

## Measured on 2026-09-10

Bybit, 56,909,100 executions, TREV 500/700, one stateful configuration:

| Measurement | Time |
| --- | ---: |
| Existing Python calculation, production ARM | 148.633 s |
| Installed Rust calculation, same ARM machine | 3.037 s |
| Actual ClickHouse stream read/wait | 9.998 s |
| Native RowBinary validation/normalization | 8.465 s |
| Native output conversion into Python insertion columns | 1.521 s |
| Full read/decode/calculate/convert/verify benchmark | 23.922 s |

The calculation improved **48.9×**. All **570** checkpoint comparisons matched:
20,287 closed bars, 8,974,216 footprint levels, every canonical candle prefix and
the exact engine state. Final candle SHA-256:
`1991b8f462219f23d4fc29c2fad548998a0331930b0bfda26caab81fd2018f83`.
Final state SHA-256:
`e942fe76c7ddb6ba1d34dacedb344f73094e39fb9ff60ad1696213819c802315`.

The 23.922-second run read the actual retained production stream through a
temporary, isolated extension import. **It excludes bar inserts, PostgreSQL
checkpoint writes, scheduling, and API readiness.** It is not a measured
end-to-end chart-ready latency. Production application code was not replaced.

An installed-extension run on the local Ryzen 7600X processed the frozen tape
in 7.51 seconds including decoding and output conversion (1.54 seconds of
calculation). The Python CPU profile still spends most time in `_push_trusted`,
`add_raw`, `push_raw`, and `_expansion_count`. The native profile has batch calls
instead of millions of Python calls; decoding is now larger than calculation.
No database query tuning was needed for this change.

Cold-build logs additionally report `read_decode_seconds`, `native_seconds`,
`convert_insert_seconds`, and `checkpoint_seconds` to measure the remaining
persistence cost when deployed. Raw timings are recorded in
[`benchmarks/results/trev-2026-09-10.json`](../../benchmarks/results/trev-2026-09-10.json).

## Full cold build, measured September 11–12

The full-build follow-up uses **58,738,652** real Bybit BTCUSDT executions on
the production ARM machine, with one 500/700 configuration and a fixed cursor.
It writes actual bars and checkpoints to isolated ClickHouse and PostgreSQL
tables, then checks readiness. Each run removes its temporary tables.

| Implementation | Cold build | Bar inserts | Checkpoints |
| --- | ---: | ---: | ---: |
| Initial Rust integration | 47.23 s | 1,156 | 588 |
| Accumulate bars across transport fragments | 32.04 s | 588 | 588 |
| Checkpoint single native builds every 1M executions | 21.37 s | 59 | 59 |
| Store short trade IDs inline in Rust | 18.69 s | 59 | 59 |

Two final-source runs took **19.16 s and 24.80 s** for the build. The actual
snapshot handler added **0.15 s**, producing a 566 KB JSON response. Total
build-through-snapshot times were **19.33 s and 24.97 s**; stream read/wait
time varied from 4.02 s to 8.95 s. Treat this as a measured **19–25 second**
range on this machine, rather than a guaranteed latency. Both runs matched a
checksum over every persisted bar column, including the footprint arrays.

The inline decoder avoids allocating a separate string for the usual trade IDs;
long IDs still use owned bytes without truncation. On an in-memory sample of
one million real executions, median decode-and-release time fell from 0.145 s
to 0.121 s across five trials. HTTP zstd compression was also measured and
increased the full build to 70.20 s, so transport remains unchanged.

These timings include database writes. All runs produced **22,118** bars,
**9,364,401** levels and the same checkpoint SHA-256:
`05f165e3ee7532827d1523b5de153c554afb4a3bd1277f402779ec4b7b5883d6`.
The larger checkpoint interval preserves bar-before-checkpoint durability;
an unexpected crash can require replaying up to one million executions.

The application has not been deployed. These isolated measurements exclude
scheduling, new live arrivals, network transport and browser rendering. They
must not be presented as browser chart-open latency.

### Reproduce the full build

Run in a standalone process with the application's database environment. The
database accounts must be able to create/drop a temporary PostgreSQL schema
and ClickHouse database. Public execution data is read; chart writes stay in
the temporary tables. The benchmark does not publish websocket updates.

```sh
python -m benchmarks.trev_full --market-id 2 > baseline.json
python -m benchmarks.trev_full --baseline baseline.json > comparison.json
python -m benchmarks.trev_full --baseline baseline.json --backend python > python.json
```

The output separates cold building, fixed-stream readiness, and the actual
snapshot handler (including JSON serialization). A final, separately timed
query verifies persisted bar/level totals and a checksum over all bar columns.
Comparison runs check this checksum and the exact checkpoint against the
baseline. `--checkpoint-every 100000` measures the earlier write frequency.

Frozen cursors remain reproducible only while their underlying trades are
retained. The benchmark fails on an expired checkpoint window or mismatched
output; capture a new baseline after source expiry or archive repair.

Raw measurements: [September 11](../../benchmarks/results/trev-full-2026-09-11.json)
and [September 12](../../benchmarks/results/trev-full-2026-09-12.json).
