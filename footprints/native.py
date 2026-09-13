"""Private TREV/Tick cold-build bindings; live projection uses Python engines."""

import json
import logging

from footprints.constants import CHART_MAX_LEVELS_PER_BAR
from footprints.trev import TrevEngine
from footprints.tick import TickEngine

logger = logging.getLogger(__name__)


class NativeTrevBuilder:
    def __init__(self, engine: TrevEngine, *, cursor, anchor_timestamp_us):
        from _trev_native import TrevBuilder

        self.config = engine.config
        self.native = TrevBuilder(
            self.config.continuation_ticks,
            self.config.reversal_ticks,
            self.config.max_bars_per_trade,
            CHART_MAX_LEVELS_PER_BAR,
            anchor_timestamp_us,
            json.dumps(engine.snapshot()),
            json.dumps(cursor) if cursor is not None else None,
        )

    def snapshot(self):
        return json.loads(self.native.snapshot_json())

    def process(self, data, *args):
        if hasattr(data, "process"):
            return data.process(self.native, *args)
        return self.native.process(data, *args)

    def python_engine(self):
        return TrevEngine.restore(self.snapshot(), config=self.config)


class NativeTickBuilder(NativeTrevBuilder):
    def __init__(self, engine: TickEngine, *, cursor, anchor_timestamp_us):
        from _trev_native import TrevBuilder

        self.config = engine.config
        self.native = TrevBuilder(
            1, 1, 1, CHART_MAX_LEVELS_PER_BAR, anchor_timestamp_us,
            json.dumps(dict(engine.snapshot(), direction="unset")),
            json.dumps(cursor) if cursor is not None else None,
            tick=self.config.trades_per_bar,
        )

    def snapshot(self):
        state = super().snapshot()
        state.pop("direction")
        return state

    def python_engine(self):
        return TickEngine.restore(self.snapshot(), config=self.config)


def native_builder(engine, *, cursor, anchor_timestamp_us):
    try:
        builder = NativeTickBuilder if isinstance(engine, TickEngine) else NativeTrevBuilder
        return builder(
            engine, cursor=cursor, anchor_timestamp_us=anchor_timestamp_us
        )
    except OverflowError:
        logger.warning("Native cold build using Python for checkpoint integer range")
        return engine
