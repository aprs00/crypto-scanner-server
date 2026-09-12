"""Day-wise historical overrides without altering the original live tape."""

from contextlib import closing
from datetime import datetime, timezone

from footprints.models import FootprintMarket, TradeRecoveryDay
from footprints.storage.charts import ClickHouseProjectorExecutionStore
from footprints.storage.clickhouse import qualified_table

DAY_US = 86_400_000_000
CURSOR_VERSION = -2


def is_reconciled_cursor(cursor):
    return cursor is not None and len(cursor) == 8 and cursor[0] == CURSOR_VERSION


def segment_changed(execution, previous):
    return (
        execution.segment_start
        if is_reconciled_cursor(execution.cursor)
        else execution.source_epoch != previous[0]
    )


class ReconciledExecutionStore(ClickHouseProjectorExecutionStore):
    def __init__(self, *args, database, **kwargs):
        super().__init__(*args, database=database, **kwargs)
        self.archive_table = qualified_table(database, "archive_executions")
        self._fixed = {}

    def _inputs(self, market_id):
        enabled = FootprintMarket.objects.filter(
            pk=market_id, recovery_enabled=True, exchange__in=("binance", "bybit")
        ).exists()
        if not enabled:
            return None
        return {
            int(
                datetime.combine(row.day, datetime.min.time(), timezone.utc).timestamp()
            )
            // 86400: str(row.active_import)
            for row in TradeRecoveryDay.objects.filter(
                market_id=market_id, active_import__isnull=False
            )
        }

    def _query_day(
        self,
        market_id,
        day,
        imports,
        *,
        start_us,
        after=None,
        descending=False,
        limit=None,
        native=False,
    ):
        archive = imports.get(day)
        parameters = {
            "market": market_id,
            "start": max(day * DAY_US, start_us),
            "end": (day + 1) * DAY_US,
        }
        conditions = [
            "market_id={market:UInt64}",
            "timestamp_us>={start:Int64}",
            "timestamp_us<{end:Int64}",
        ]
        if archive:
            parameters["import"] = archive
            conditions.append("import_id={import:UUID}")
            columns = "1,ordinal,0,timestamp_us,trade_id,price_atoms,quantity_atoms,is_taker_buy,instrument_spec_id"
            if native:
                # Literal 1/0 otherwise become UInt8 in RowBinary.
                columns = "toUInt32(1),ordinal,toUInt32(0),timestamp_us,trade_id,price_atoms,quantity_atoms,is_taker_buy,instrument_spec_id"
            order = "ordinal"
            table = self.archive_table
            if after:
                parameters["ordinal"] = after[4]
                conditions.append("ordinal>{ordinal:UInt64}")
        else:
            columns = "source_epoch,source_sequence,batch_index,timestamp_us,trade_id,price_atoms,quantity_atoms,is_taker_buy,instrument_spec_id"
            order = "source_epoch,source_sequence,batch_index,timestamp_us,trade_id"
            table = self.qualified_table
            if after:
                parameters.update(
                    dict(zip(("epoch", "sequence", "batch", "time", "id"), after[3:]))
                )
                conditions.append(
                    "(source_epoch,source_sequence,batch_index,timestamp_us,trade_id) > ({epoch:UInt32},{sequence:UInt64},{batch:UInt32},{time:Int64},{id:String})"
                )
        if descending:
            order = ",".join(f"{column} DESC" for column in order.split(","))
        clause = ""
        if limit is not None:
            parameters["limit"] = limit
            clause = " LIMIT {limit:UInt64}"
        sql = f"SELECT {columns} FROM {table} WHERE {' AND '.join(conditions)} ORDER BY {order}{clause}"
        return sql, parameters

    def latest_cursor(self, *, market_id, start_timestamp_us):
        imports = self._inputs(market_id)
        if imports is None:
            return super().latest_cursor(
                market_id=market_id, start_timestamp_us=start_timestamp_us
            )
        self._fixed[market_id] = imports
        today = int(datetime.now(timezone.utc).timestamp()) // 86400
        for day in range(today, start_timestamp_us // DAY_US - 1, -1):
            sql, parameters = self._query_day(
                market_id,
                day,
                imports,
                start_us=start_timestamp_us,
                descending=True,
                limit=1,
            )
            rows = self.access.query(
                sql, parameters=parameters, operation="latest_reconciled_cursor"
            ).result_rows
            if rows:
                row = rows[0]
                return (CURSOR_VERSION, day, 0 if day in imports else 1, *row[:5])
        return None

    def _live_epoch(self, market_id, timestamp_us, trade_id):
        rows = self.access.query(
            f"""SELECT source_epoch FROM {self.qualified_table}
            WHERE market_id={{market:UInt64}} AND timestamp_us={{time:Int64}}
            AND trade_id={{id:String}} ORDER BY source_epoch DESC LIMIT 1""",
            parameters={"market": market_id, "time": timestamp_us, "id": trade_id},
            operation="verify_archive_boundary",
        ).result_rows
        return rows[0][0] if rows else None

    def iter_native_batches(
        self, *, market_id, start_timestamp_us, batch_size,
        after_cursor=None, through_cursor=None,
    ):
        from footprints.storage.native import NativeReplayReader

        imports = (
            self._fixed.pop(market_id, None)
            if through_cursor is not None else self._inputs(market_id)
        )
        if imports is None:
            yield from super().iter_native_batches(
                market_id=market_id, start_timestamp_us=start_timestamp_us,
                after_cursor=after_cursor, through_cursor=through_cursor,
                batch_size=batch_size,
            )
            return
        if after_cursor is not None and not is_reconciled_cursor(after_cursor):
            raise ValueError("old chart cursor requires a rebuild")
        reader = NativeReplayReader(
            self, market_id=market_id, start_timestamp_us=start_timestamp_us,
            after_cursor=after_cursor, through_cursor=through_cursor,
            batch_size=batch_size,
        )
        first_day = max(start_timestamp_us // DAY_US, after_cursor[1] if after_cursor else 0)
        last_day = through_cursor[1] if through_cursor else int(datetime.now(timezone.utc).timestamp()) // 86400
        for day in range(first_day, last_day + 1):
            after = after_cursor if after_cursor and after_cursor[1] == day else None
            sql, parameters = self._query_day(
                market_id, day, imports, start_us=start_timestamp_us, after=after,
                native=True,
            )
            yield from reader.query(sql, parameters, partition=(day, 0 if day in imports else 1))
            if reader.decoder.done:
                return

    def iter_executions(
        self,
        *,
        market_id,
        start_timestamp_us,
        after_cursor=None,
        through_cursor=None,
        limit=None,
    ):
        imports = (
            self._fixed.pop(market_id, None)
            if through_cursor is not None
            else self._inputs(market_id)
        )
        if imports is None:
            yield from super().iter_executions(
                market_id=market_id,
                start_timestamp_us=start_timestamp_us,
                after_cursor=after_cursor,
                through_cursor=through_cursor,
                limit=limit,
            )
            return
        if after_cursor is not None and not is_reconciled_cursor(after_cursor):
            raise ValueError("old chart cursor requires a rebuild")
        first_day = max(
            start_timestamp_us // DAY_US, after_cursor[1] if after_cursor else 0
        )
        last_day = (
            through_cursor[1]
            if through_cursor
            else int(datetime.now(timezone.utc).timestamp()) // 86400
        )
        previous = after_cursor
        previous_execution = None
        emitted = 0
        for day in range(first_day, last_day + 1):
            after = after_cursor if after_cursor and after_cursor[1] == day else None
            sql, parameters = self._query_day(
                market_id, day, imports, start_us=start_timestamp_us, after=after
            )
            with closing(
                self.access.stream_rows(
                    sql, parameters=parameters, operation="stream_reconciled_executions"
                )
            ) as rows:
                for row in rows:
                    execution = self._execution_from_row(row)
                    cursor = (CURSOR_VERSION, day, 0 if day in imports else 1, *row[:5])
                    if through_cursor and cursor > through_cursor:
                        return
                    if previous == cursor:
                        if previous_execution and execution != previous_execution:
                            raise ValueError("conflicting duplicate replay cursor")
                        continue
                    broken = False
                    if previous:
                        if cursor[2] == previous[2] == 1:
                            broken = cursor[3] != previous[3]
                        elif cursor[2] != previous[2]:
                            # Verify the seam using a shared original observation.
                            if cursor[2] == 0:
                                broken = (
                                    self._live_epoch(market_id, cursor[6], cursor[7])
                                    != previous[3]
                                )
                            else:
                                broken = (
                                    self._live_epoch(
                                        market_id, previous[6], previous[7]
                                    )
                                    != cursor[3]
                                )
                    previous, previous_execution = cursor, execution
                    yield execution._replace(replay_cursor=cursor, segment_start=broken)
                    emitted += 1
                    if limit is not None and emitted >= limit:
                        return
