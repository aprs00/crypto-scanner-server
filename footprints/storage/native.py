"""Bounded native batches over the same raw/reconciled replay queries."""

import json
from contextlib import closing
from dataclasses import dataclass
from time import perf_counter

from footprints.storage.charts import ProjectorDataError, ProjectorSpecDivisors


@dataclass(slots=True)
class NativeBatch:
    data: object
    seam_break: bool = False

    def __len__(self):
        return len(self.data)


class NativeReplayReader:
    def __init__(
        self, store, *, batch_size, market_id, start_timestamp_us,
        after_cursor=None, through_cursor=None, limit=None,
    ):
        from _trev_native import RowBinaryDecoder

        if batch_size <= 0:
            raise ValueError("batch size must be positive")
        if limit is not None:
            raise ValueError("native cold replay requires a fixed cursor, not a limit")
        self.store = store
        self.market_id = market_id
        self.batch_size = batch_size
        self.emitted = 0
        self.fetch_seconds = 0.0
        self.decode_seconds = 0.0
        self.decoder = RowBinaryDecoder(
            json.dumps(after_cursor) if after_cursor is not None else None,
            json.dumps(through_cursor) if through_cursor is not None else None,
        )
        for spec, divisors in store.spec_divisors.items():
            self._set_divisor(spec, divisors)

    def _set_divisor(self, spec, divisors):
        if not isinstance(divisors, ProjectorSpecDivisors):
            raise ProjectorDataError(
                "instrument specification divisor loader returned an invalid value"
            )
        # A divisor larger than every possible UInt64 input cannot exactly
        # divide a positive raw value. Zero is the decoder's non-divisible marker.
        maximum = (1 << 64) - 1
        self.decoder.set_divisor(
            spec,
            divisors.price_atoms_per_tick if divisors.price_atoms_per_tick <= maximum else 0,
            divisors.quantity_atoms_per_unit if divisors.quantity_atoms_per_unit <= maximum else 0,
        )

    def _take(self, partition):
        from _trev_native import DataError, UnknownSpec

        while True:
            started = perf_counter()
            try:
                return self.decoder.take(
                    self.batch_size - self.emitted % self.batch_size, partition
                )
            except UnknownSpec as exc:
                spec = exc.args[0]
                divisors = (
                    self.store.load_spec_divisor(spec)
                    if self.store.load_spec_divisor is not None else None
                )
                if divisors is None:
                    raise ProjectorDataError(
                        f"no native-unit divisors exist for instrument specification {spec}"
                    ) from exc
                self._set_divisor(spec, divisors)
                self.store.spec_divisors[spec] = divisors
            except DataError as exc:
                if partition is not None and str(exc) == "conflicting duplicate replay cursor":
                    raise ValueError(str(exc)) from exc
                raise ProjectorDataError(str(exc)) from exc
            finally:
                self.decode_seconds += perf_counter() - started

    def query(self, sql, parameters, *, partition=None):
        from _trev_native import DataError

        with closing(self.store.access.stream_rowbinary(sql, parameters=parameters)) as blocks:
            while not self.decoder.done:
                batch = self._take(partition)
                if len(batch):
                    broken = False
                    previous, current = batch.previous_cursor, batch.first_cursor
                    if partition is not None and previous and current[2] != previous[2]:
                        if current[2] == 0:
                            broken = self.store._live_epoch(self.market_id, current[6], current[7]) != previous[3]
                        else:
                            broken = self.store._live_epoch(self.market_id, previous[6], previous[7]) != current[3]
                    self.emitted += len(batch)
                    yield NativeBatch(batch, broken)
                    continue
                if self.decoder.done:
                    break
                started = perf_counter()
                block = next(blocks, None)
                self.fetch_seconds += perf_counter() - started
                if block is None:
                    try:
                        self.decoder.finish()
                    except DataError as exc:
                        raise ProjectorDataError(str(exc)) from exc
                    break
                self.decoder.feed(block)
