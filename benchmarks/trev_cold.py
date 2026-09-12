"""Repeatable installed-extension benchmark; no database or series mutations.

Run from the repo root: python -m benchmarks.trev_cold --help
Input is the nine-column projector SELECT in ClickHouse RowBinary format.
The prepared 33-byte tape uses divisors 1/1 and reconstructed source identities;
its engine outputs/checkpoints must still match the original Python baseline.
"""

import argparse
import cProfile
import hashlib
import json
import platform
import pstats
import struct
import sys
import time

from footprints.trev import TrevConfig, TrevEngine


def canonical_bars(bars):
    out = bytearray()
    for bar in bars:
        out.extend(struct.pack(
            "<13q", bar.bar_index, bar.spec_id, bar.break_before,
            bar.open_tick, bar.high_tick, bar.low_tick, bar.close_tick,
            bar.buy_units, bar.sell_units, bar.trade_count,
            bar.opened_timestamp_us, bar.ended_timestamp_us, len(bar.levels),
        ))
        for level in bar.levels:
            out.extend(struct.pack("<3q", level.price_tick, level.buy_units, level.sell_units))
    return bytes(out)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", help="RowBinary file, or - for stdin")
    parser.add_argument("--backend", choices=("rust", "python"), default="rust")
    parser.add_argument("--continuation", type=int, default=500)
    parser.add_argument("--reversal", type=int, default=700)
    parser.add_argument("--batch-size", type=int, default=100_000)
    parser.add_argument("--max-rows", type=int)
    parser.add_argument("--spec", action="append", default=[], metavar="ID:PRICE_DIVISOR:QUANTITY_DIVISOR")
    parser.add_argument("--baseline", help="JSONL from this harness or the original production benchmark")
    parser.add_argument("--profile", help="write cProfile stats (run separately from timing)")
    parser.add_argument("--convert-bars", action="store_true", help="also measure native output conversion to insertion rows")
    args = parser.parse_args()
    if args.batch_size <= 0 or (args.max_rows is not None and args.max_rows <= 0):
        parser.error("batch size and max rows must be positive")
    from _trev_native import RowBinaryDecoder, TrevBuilder
    decoder = RowBinaryDecoder()
    for spec in args.spec or ["1:1:1", "2:1:1"]:
        decoder.set_divisor(*map(int, spec.split(":")))
    config = TrevConfig(args.continuation, args.reversal)
    engine = TrevEngine(config)
    state = engine.snapshot()
    native = TrevBuilder(config.continuation_ticks, config.reversal_ticks,
                         config.max_bars_per_trade, 4000, 1, json.dumps(engine.snapshot()))
    expected = {}
    if args.baseline:
        with open(args.baseline) as source:
            expected = {row["rows"]: row for line in source if (row := json.loads(line)).get("event") == "checkpoint"}
        if not expected:
            parser.error("baseline contains no checkpoints")
    def compare_checkpoint(result):
        if result["rows"] not in expected:
            return 0
        for key in ("bars", "bar_sha256", "state_sha256"):
            if result[key] != expected[result["rows"]][key]:
                raise AssertionError(f"parity mismatch at {result['rows']} rows: {key}")
        return 1
    digest = hashlib.sha256()
    rows = bars = levels = checks = 0
    read_seconds = decode_seconds = engine_seconds = convert_seconds = proof_seconds = 0.0
    previous = None
    profiler = cProfile.Profile() if args.profile else None
    started = time.perf_counter()
    if profiler:
        profiler.enable()
    last_report = 0
    with (sys.stdin.buffer if args.input == "-" else open(args.input, "rb")) as source:
        eof = False
        while args.max_rows is None or rows < args.max_rows:
            remaining = args.batch_size - rows % args.batch_size
            if args.max_rows is not None:
                remaining = min(remaining, args.max_rows - rows)
            stage = time.perf_counter()
            batch = decoder.take(remaining)
            decode_seconds += time.perf_counter() - stage
            if not len(batch):
                if eof:
                    decoder.finish()
                    break
                stage = time.perf_counter()
                data = source.read(8 * 1024 * 1024)
                read_seconds += time.perf_counter() - stage
                eof = not data
                stage = time.perf_counter()
                decoder.feed(data)
                decode_seconds += time.perf_counter() - stage
                continue
            rows += len(batch)
            if args.backend == "rust":
                offset = 0
                while offset < len(batch):
                    stage = time.perf_counter()
                    offset, closed = native.process(batch, offset, 2000)
                    engine_seconds += time.perf_counter() - stage
                    bars += len(closed)
                    levels += closed.level_count
                    stage = time.perf_counter()
                    if args.convert_bars:
                        converted = closed.rows(1)
                        del converted
                    convert_seconds += time.perf_counter() - stage
                    stage = time.perf_counter()
                    digest.update(closed.canonical_bytes())
                    proof_seconds += time.perf_counter() - stage
                state = json.loads(native.snapshot_json())
            else:
                # Decoder is shared so engine timings compare exactly the same
                # validated stream. Python adapter cost is reported separately.
                stage = time.perf_counter()
                executions = batch.executions(0)
                decode_seconds += time.perf_counter() - stage
                stage = time.perf_counter()
                closed = []
                for epoch, sequence, index, timestamp, identity, price, quantity, buy, spec, *_ in executions:
                    if previous is not None and previous != epoch:
                        closed.extend(engine.truncate_segment().bars)
                    previous = epoch
                    rank = engine.state.last_order.rank + 1 if engine.state.last_order else 0
                    closed.extend(engine.push_raw(timestamp_us=timestamp, rank=rank, price_tick=price,
                                  quantity_units=quantity, is_taker_buy=buy, spec_id=spec).bars)
                engine_seconds += time.perf_counter() - stage
                bars += len(closed)
                levels += sum(len(bar.levels) for bar in closed)
                stage = time.perf_counter()
                digest.update(canonical_bars(closed))
                proof_seconds += time.perf_counter() - stage
                state = engine.snapshot()
            stage = time.perf_counter()
            state_hash = hashlib.sha256(json.dumps(state, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
            proof_seconds += time.perf_counter() - stage
            if rows % args.batch_size == 0 or eof or (args.max_rows and rows == args.max_rows):
                result = dict(event="checkpoint", rows=rows, bars=bars, bar_sha256=digest.hexdigest(), state_sha256=state_hash)
                checks += compare_checkpoint(result)
                print(json.dumps(result), flush=True)
                last_report = rows
    if rows != last_report:
        result = dict(event="checkpoint", rows=rows, bars=bars, bar_sha256=digest.hexdigest(), state_sha256=state_hash)
        checks += compare_checkpoint(result)
        print(json.dumps(result), flush=True)
    if expected:
        required_rows = min(args.max_rows, max(expected)) if args.max_rows else max(expected)
        if rows != required_rows:
            raise AssertionError(f"incomplete baseline replay: expected {required_rows} rows, got {rows}")
        if checks != sum(checkpoint <= rows for checkpoint in expected):
            raise AssertionError("not all baseline checkpoints were compared; use the baseline batch size")
    if profiler:
        profiler.disable()
        profiler.dump_stats(args.profile)
        pstats.Stats(profiler, stream=sys.stderr).sort_stats("tottime").print_stats(20)
    print(json.dumps(dict(
        event="result", backend=args.backend, machine=platform.machine(), python=platform.python_version(),
        rows=rows, bars=bars, levels=levels, checkpoint_comparisons=checks,
        read_seconds=read_seconds, decode_seconds=decode_seconds, engine_seconds=engine_seconds,
        executions_per_second=rows / engine_seconds if engine_seconds else 0,
        convert_seconds=convert_seconds, proof_seconds=proof_seconds,
        elapsed_seconds=time.perf_counter() - started,
        bar_sha256=digest.hexdigest(), state_sha256=hashlib.sha256(
            json.dumps(state, sort_keys=True, separators=(",", ":")).encode()).hexdigest(),
    )), flush=True)


if __name__ == "__main__":
    main()
