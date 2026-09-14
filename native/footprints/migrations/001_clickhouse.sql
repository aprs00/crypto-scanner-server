CREATE TABLE IF NOT EXISTS {db}.executions (
 market UInt8, day Int32, revision UInt64, ordinal UInt64,
 segment UInt64, timestamp_us Int64 CODEC(Delta,ZSTD(1)),
 trade_id String CODEC(ZSTD(1)), venue_sequence UInt64,
 price_atoms Int64 CODEC(T64,ZSTD(1)), quantity_atoms UInt64 CODEC(T64,ZSTD(1)),
 is_taker_buy Bool, tick_atoms Int64, step_atoms UInt64,
 INDEX timestamp_minmax timestamp_us TYPE minmax GRANULARITY 1,
 CONSTRAINT valid_execution CHECK price_atoms>0 AND quantity_atoms>0 AND tick_atoms>0 AND step_atoms>0
) ENGINE=ReplacingMergeTree
PARTITION BY day ORDER BY (market,day,revision,ordinal)
TTL fromUnixTimestamp64Micro(timestamp_us)+INTERVAL 31 DAY DELETE
SETTINGS non_replicated_deduplication_window=10000, fsync_after_insert=1, fsync_part_directory=1;

CREATE TABLE IF NOT EXISTS {db}.bars (
 series_id UInt64, bar_index UInt64, opened_us Int64, ended_us Int64,
 tick_atoms Int64, step_atoms UInt64,
 open_tick Int64, high_tick Int64, low_tick Int64, close_tick Int64,
 buy_units UInt64, sell_units UInt64, trade_count UInt64, break_before Bool,
 level_ticks Array(Int64), level_buy Array(UInt64), level_sell Array(UInt64),
 CONSTRAINT parallel_levels CHECK length(level_ticks)=length(level_buy) AND length(level_ticks)=length(level_sell)
) ENGINE=ReplacingMergeTree
PARTITION BY toYYYYMM(fromUnixTimestamp64Micro(ended_us)) ORDER BY (series_id,bar_index)
TTL fromUnixTimestamp64Micro(ended_us)+INTERVAL 31 DAY DELETE
SETTINGS non_replicated_deduplication_window=10000,fsync_after_insert=1,fsync_part_directory=1;
