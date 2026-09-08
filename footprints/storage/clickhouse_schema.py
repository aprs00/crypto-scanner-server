from __future__ import annotations

from footprints.storage.clickhouse import ClickHouseAccess, quote_identifier


RAW_EXECUTION_COLUMNS = (
    "market_id",
    "source_epoch",
    "source_sequence",
    "batch_index",
    "timestamp_us",
    "trade_id",
    "price_atoms",
    "quantity_atoms",
    "is_taker_buy",
    "instrument_spec_id",
)

CHART_BAR_COLUMNS = (
    "series_id",
    "bar_index",
    "opened_at",
    "ended_at",
    "instrument_spec_id",
    "open_tick",
    "high_tick",
    "low_tick",
    "close_tick",
    "buy_units",
    "sell_units",
    "trade_count",
    "break_before",
    "level_ticks",
    "level_buy_units",
    "level_sell_units",
)


def ensure_clickhouse_schema(access: ClickHouseAccess, database: str) -> None:
    """Create the resettable V0 ClickHouse schema if it does not exist."""

    quoted_database = quote_identifier(database)
    access.command(
        f"CREATE DATABASE IF NOT EXISTS {quoted_database}",
        operation="ensure_footprint_database",
    )
    access.command(
        f"""
        CREATE TABLE IF NOT EXISTS {quoted_database}.`raw_executions`
        (
            market_id UInt64,
            source_epoch UInt32,
            source_sequence UInt64,
            batch_index UInt32,
            timestamp_us Int64 CODEC(Delta, ZSTD(1)),
            trade_id String CODEC(ZSTD(1)),
            price_atoms Int64 CODEC(T64, ZSTD(1)),
            quantity_atoms UInt64 CODEC(T64, ZSTD(1)),
            is_taker_buy Bool,
            instrument_spec_id UInt64,
            INDEX timestamp_us_minmax timestamp_us TYPE minmax GRANULARITY 1,
            CONSTRAINT positive_execution_price CHECK price_atoms > 0,
            CONSTRAINT positive_execution_quantity CHECK quantity_atoms > 0
        )
        ENGINE = MergeTree()
        PARTITION BY toYYYYMM(fromUnixTimestamp64Micro(timestamp_us))
        ORDER BY
        (
            market_id,
            source_epoch,
            source_sequence,
            batch_index,
            timestamp_us,
            trade_id
        )
        TTL fromUnixTimestamp64Micro(timestamp_us) + INTERVAL 31 DAY DELETE
        SETTINGS non_replicated_deduplication_window = 10000
        """,
        operation="ensure_raw_executions_table",
    )
    access.command(
        f"""
        CREATE TABLE IF NOT EXISTS {quoted_database}.`chart_bars`
        (
            series_id UInt64,
            bar_index UInt64 CODEC(Delta(8), ZSTD(1)),
            opened_at DateTime64(6, 'UTC') CODEC(Delta(8), ZSTD(1)),
            ended_at DateTime64(6, 'UTC') CODEC(Delta(8), ZSTD(1)),
            instrument_spec_id UInt64,
            open_tick Int64 CODEC(T64, ZSTD(1)),
            high_tick Int64 CODEC(T64, ZSTD(1)),
            low_tick Int64 CODEC(T64, ZSTD(1)),
            close_tick Int64 CODEC(T64, ZSTD(1)),
            buy_units UInt64 CODEC(T64, ZSTD(1)),
            sell_units UInt64 CODEC(T64, ZSTD(1)),
            trade_count UInt64 CODEC(T64, ZSTD(1)),
            break_before Bool,
            level_ticks Array(Int64) CODEC(Delta(8), ZSTD(1)),
            level_buy_units Array(UInt64) CODEC(T64, ZSTD(1)),
            level_sell_units Array(UInt64) CODEC(T64, ZSTD(1)),
            CONSTRAINT positive_chart_ticks CHECK
                open_tick > 0 AND high_tick > 0 AND low_tick > 0 AND close_tick > 0,
            CONSTRAINT parallel_chart_levels CHECK
                length(level_ticks) = length(level_buy_units)
                AND length(level_ticks) = length(level_sell_units)
        )
        ENGINE = ReplacingMergeTree()
        PARTITION BY toYYYYMM(ended_at)
        ORDER BY (series_id, bar_index)
        TTL ended_at + INTERVAL 31 DAY DELETE
        SETTINGS non_replicated_deduplication_window = 10000
        """,
        operation="ensure_chart_bars_table",
    )


def ensure_archive_schema(access: ClickHouseAccess, database: str) -> None:
    """Additive schema; original V0 tables and Hyperliquid are untouched."""
    access.command(
        f"""
        CREATE TABLE IF NOT EXISTS {quote_identifier(database)}.archive_executions (
            market_id UInt64, day Date, import_id UUID, ordinal UInt64,
            timestamp_us Int64, trade_id String, price_atoms Int64,
            quantity_atoms UInt64, is_taker_buy Bool, instrument_spec_id UInt64
        ) ENGINE = MergeTree
        PARTITION BY toYYYYMM(day)
        ORDER BY (market_id,day,import_id,ordinal)
        TTL day + INTERVAL 31 DAY DELETE
        SETTINGS non_replicated_deduplication_window = 10000
    """,
        operation="ensure_archive_executions",
    )
