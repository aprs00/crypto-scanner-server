"""Private cold-build binding. Live projection continues to use TrevEngine."""

import json
import logging

from footprints.constants import CHART_MAX_LEVELS_PER_BAR
from footprints.trev import TrevEngine

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

    def python_engine(self):
        return TrevEngine.restore(self.snapshot(), config=self.config)


def native_builder(engine, *, cursor, anchor_timestamp_us):
    try:
        return NativeTrevBuilder(
            engine, cursor=cursor, anchor_timestamp_us=anchor_timestamp_us
        )
    except OverflowError:
        logger.warning("TREV cold build using Python for checkpoint integer range")
        return engine
