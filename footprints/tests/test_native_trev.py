"""Differential tests against the unchanged uninterrupted Python engine."""

import importlib.util
import json
import random
import struct
import unittest

from footprints.trev import TrevConfig, TrevEngine


def rowbinary(rows):
    data = bytearray()
    for epoch, sequence, index, timestamp, identity, price, quantity, buy, spec in rows:
        identity = identity.encode()
        data.extend(struct.pack("<IQIq", epoch, sequence, index, timestamp))
        length = len(identity)
        while length >= 128:
            data.append((length & 127) | 128)
            length >>= 7
        data.append(length)
        data.extend(identity)
        data.extend(struct.pack("<qQBQ", price, quantity, buy, spec))
    return bytes(data)


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


def builder(engine, cursor=None, *, anchor=1, max_levels=4000):
    from _trev_native import TrevBuilder
    return TrevBuilder(
        engine.config.continuation_ticks, engine.config.reversal_ticks,
        engine.config.max_bars_per_trade, max_levels, anchor,
        json.dumps(engine.snapshot()), json.dumps(cursor) if cursor else None,
    )


@unittest.skipUnless(importlib.util.find_spec("_trev_native"), "native wheel is not installed")
class NativeTrevTests(unittest.TestCase):
    def decoder(self, *, after=None, through=None):
        from _trev_native import RowBinaryDecoder
        decoder = RowBinaryDecoder(
            json.dumps(after) if after else None, json.dumps(through) if through else None,
        )
        decoder.set_divisor(1, 1, 1)
        decoder.set_divisor(2, 1, 1)
        return decoder

    def assert_parity(self, rows, config, *, seed=0):
        rng = random.Random(seed)
        data = rowbinary(rows)
        decoder = self.decoder()
        oracle = TrevEngine(config)
        native = builder(oracle)
        previous = None
        offset = 0
        while offset < len(data):
            size = rng.randint(1, 2000)
            decoder.feed(data[offset:offset + size])
            offset += size
            while True:
                batch = decoder.take(rng.randint(1, 49))
                if not len(batch):
                    break
                expected = []
                for row in batch.executions(0):
                    epoch, sequence, index, timestamp, identity, price, quantity, buy, spec, *_ = row
                    if previous and epoch != previous[0]:
                        expected.extend(oracle.truncate_segment().bars)
                    rank = oracle.state.last_order.rank + 1 if oracle.state.last_order else 0
                    expected.extend(oracle.push_raw(
                        timestamp_us=timestamp, rank=rank, price_tick=price,
                        quantity_units=quantity, is_taker_buy=buy, spec_id=spec,
                    ).bars)
                    previous = (epoch, sequence, index, timestamp, identity)
                actual = bytearray()
                consumed = 0
                while consumed < len(batch):
                    consumed, bars = native.process(batch, consumed, rng.randint(1, 13))
                    actual.extend(bars.canonical_bytes())
                self.assertEqual(bytes(actual), canonical_bars(expected))
                self.assertEqual(json.loads(native.snapshot_json()), oracle.snapshot())
                self.assertEqual(native.cursor, previous)
                # Exercise both checkpoint directions repeatedly, including empty
                # active candles after exact equality and after a gap.
                restored = TrevEngine.restore(json.loads(native.snapshot_json()), config=config)
                native = builder(restored, native.cursor)
        decoder.finish()
        return oracle

    def test_randomized_batches_gaps_spec_changes_and_timestamp_regressions(self):
        for seed in range(30):
            rng = random.Random(seed)
            price, epoch = 100_000, 1
            rows = []
            for index in range(700):
                price += rng.choice((0, 0, 0, 1, -1, 20, -31, 100, -101))
                epoch += int(index % 173 == 172)
                row = (epoch, index, 0, 1_000_000 + rng.randrange(5000),
                       str(index), price, rng.randint(1, 1000), bool(index % 2), 1 + (index // 131) % 2)
                rows.append(row)
                if index % 47 == 0:
                    rows.append(row)  # identical source duplicates do not count
            for continuation, reversal in ((1, 3), (20, 30), (314, 471), (4000, 4000)):
                with self.subTest(seed=seed, config=(continuation, reversal)):
                    self.assert_parity(rows, TrevConfig(continuation, reversal), seed=seed)

    def test_equality_reversal_synthetic_and_final_python_handoff(self):
        prices = [100, 100, 102, 110, 125, 105, 72, 72, 82, 82, 123]
        rows = [(1, i, 0, 1000 - i, str(i), price, 2 + i, bool(i % 2), 1)
                for i, price in enumerate(prices)]
        state = self.assert_parity(rows, TrevConfig(10, 15))
        continued = TrevEngine.restore(state.snapshot(), config=state.config)
        kwargs = dict(timestamp_us=1, rank=len(rows), price_tick=155, quantity_units=7, is_taker_buy=False, spec_id=1)
        self.assertEqual(continued.push_raw(**kwargs), state.push_raw(**kwargs))

    def test_decoder_fragmented_strings_exact_divisors_and_unknown_spec_retry(self):
        from _trev_native import RowBinaryDecoder, UnknownSpec
        row = (1, 2**64 - 1, 2**32 - 1, 123, "µ" * 180, 1000, 45, True, 9)
        data = rowbinary([row])
        decoder = RowBinaryDecoder()
        for byte in data[:-1]:
            decoder.feed(bytes([byte]))
            self.assertEqual(len(decoder.take(1)), 0)
        decoder.feed(data[-1:])
        with self.assertRaises(UnknownSpec):
            decoder.take(1)
        decoder.set_divisor(9, 10, 5)
        batch = decoder.take(1)
        self.assertEqual(batch.executions(0)[0][5:9], (100, 9, True, 9))
        self.assertEqual(batch.last_cursor, row[:5])
        decoder.finish()

    def test_conflicting_duplicates_order_bounds_and_truncated_input(self):
        from _trev_native import DataError
        row = (1, 1, 0, 100, "a", 100, 3, True, 1)
        for rows, message in (
            ([row, (*row[:6], 4, *row[7:])], "conflicting duplicate"),
            ([row, (1, 0, *row[2:])], "global source order"),
        ):
            decoder = self.decoder()
            decoder.feed(rowbinary(rows))
            with self.assertRaisesRegex(DataError, message):
                decoder.take(100)
            # Decoder failures are atomic, too.
            with self.assertRaisesRegex(DataError, message):
                decoder.take(100)
        decoder = self.decoder(after=row[:5])
        decoder.feed(rowbinary([row]))
        with self.assertRaisesRegex(DataError, "did not advance"):
            decoder.take(1)
        decoder = self.decoder(through=(1, 0, *row[2:5]))
        decoder.feed(rowbinary([row]))
        with self.assertRaisesRegex(DataError, "beyond"):
            decoder.take(1)
        decoder = self.decoder()
        decoder.feed(rowbinary([row])[:-1])
        self.assertEqual(len(decoder.take(1)), 0)
        with self.assertRaisesRegex(DataError, "truncated"):
            decoder.finish()

    def test_bad_native_values_and_non_divisible_atoms(self):
        from _trev_native import DataError
        row = [1, 1, 0, 100, "a", 100, 3, True, 1]
        for column, value in ((0, 0), (3, 0), (4, ""), (5, -1), (6, 0), (7, 2), (8, 0)):
            decoder = self.decoder()
            bad = row.copy()
            bad[column] = value
            decoder.feed(rowbinary([bad]))
            with self.assertRaises(DataError):
                decoder.take(1)
        decoder = self.decoder()
        decoder.set_divisor(1, 3, 1)
        decoder.feed(rowbinary([row]))
        with self.assertRaisesRegex(DataError, "not exactly divisible"):
            decoder.take(1)

    def test_engine_errors_do_not_commit_any_part_of_the_batch(self):
        for config, max_levels, prices, message in (
            (TrevConfig(1, 1), 4000, (1000, 1600), "TREV_EXPANSION_LIMIT"),
            (TrevConfig(4000, 4000), 2, (1000, 1001, 1002), "CHART_BAR_TOO_DENSE"),
        ):
            native = builder(TrevEngine(config), max_levels=max_levels)
            before = native.snapshot_json()
            decoder = self.decoder()
            decoder.feed(rowbinary([(1, i, 0, 100 + i, str(i), p, 1, True, 1) for i, p in enumerate(prices)]))
            with self.assertRaisesRegex(ValueError, message):
                native.process(decoder.take(10), 0, 2000)
            self.assertEqual(native.snapshot_json(), before)
            self.assertIsNone(native.cursor)

    def test_wide_volume_totals_are_exact_and_overflow_is_atomic(self):
        engine = TrevEngine(TrevConfig(10, 15))
        engine.push_raw(timestamp_us=100, rank=0, price_tick=100, quantity_units=2**128 - 2,
                        is_taker_buy=True, spec_id=1)
        cursor = (1, 0, 0, 100, "0")
        native = builder(engine, cursor)
        decoder = self.decoder(after=cursor)
        decoder.feed(rowbinary([(1, 1, 0, 101, "1", 100, 3, True, 1)]))
        batch = decoder.take(1)
        with self.assertRaises(OverflowError):
            native.process(batch, 0, 2000)
        self.assertEqual(json.loads(native.snapshot_json()), engine.snapshot())
        self.assertEqual(native.cursor, cursor)
        # The unchanged checkpoint can resume exactly in Python.
        restored = TrevEngine.restore(json.loads(native.snapshot_json()), config=engine.config)
        restored.push_raw(timestamp_us=101, rank=1, price_tick=100, quantity_units=3,
                          is_taker_buy=True, spec_id=1)
        self.assertEqual(restored.state.active.buy_units, 2**128 + 1)
        with self.assertRaises(OverflowError):
            builder(restored, (1, 1, 0, 101, "1"))

    def test_uint64_quantity_and_large_positive_prices(self):
        decoder = self.decoder()
        decoder.feed(rowbinary([(1, 0, 0, 100, "0", 2**63 - 1, 2**64 - 1, True, 1),
                               (1, 1, 0, 101, "1", 2**63 - 1, 2**64 - 1, True, 1)]))
        native = builder(TrevEngine(TrevConfig(10, 15)))
        native.process(decoder.take(2), 0, 2000)
        state = json.loads(native.snapshot_json())
        self.assertEqual(state["active"]["buyUnits"], 2 * (2**64 - 1))
        self.assertEqual(state["active"]["openTick"], 2**63 - 1)
