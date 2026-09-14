use crate::{
    config::{Config, Exchange, READ_BATCH},
    model::{ChartBar, RawTrade, Spec, Trade, decimal},
};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};

const TRADE_COLUMNS: &str = "market,day,revision,ordinal,segment,timestamp_us,trade_id,venue_sequence,price_atoms,quantity_atoms,is_taker_buy,tick_atoms,step_atoms";
const BAR_COLUMNS: &str = "series_id,bar_index,opened_us,ended_us,tick_atoms,step_atoms,open_tick,high_tick,low_tick,close_tick,buy_units,sell_units,trade_count,break_before,level_ticks,level_buy,level_sell";
#[derive(Clone)]
pub struct Store {
    client: reqwest::Client,
    pub config: Config,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StoredBar {
    pub series: u64,
    pub ended: i64,
    pub value: ChartBar,
}
impl Store {
    pub fn new(config: &Config) -> Result<Self> {
        Ok(Self {
            config: config.clone(),
            client: reqwest::Client::builder()
                .connect_timeout(std::time::Duration::from_secs(5))
                .timeout(std::time::Duration::from_secs(30))
                .build()?,
        })
    }
    pub async fn sql(&self, query: &str, body: Vec<u8>) -> Result<bytes::Bytes> {
        let response = self
            .client
            .post(&self.config.clickhouse_url)
            .basic_auth(&self.config.user, Some(&self.config.password))
            .header(reqwest::header::CONTENT_LENGTH, body.len())
            .query(&[
                ("query", query),
                ("max_execution_time", "25"),
                ("wait_end_of_query", "1"),
            ])
            .body(body)
            .send()
            .await?;
        let code = response.status();
        let bytes = response.bytes().await?;
        ensure!(
            code.is_success(),
            "ClickHouse {}: {}",
            code,
            String::from_utf8_lossy(&bytes[..bytes.len().min(1000)])
        );
        Ok(bytes)
    }
    pub async fn ping(&self) -> Result<()> {
        self.sql("SELECT 1", vec![]).await?;
        Ok(())
    }
    pub async fn migrate(&self) -> Result<()> {
        self.sql(
            &format!("CREATE DATABASE IF NOT EXISTS {}", self.config.database),
            vec![],
        )
        .await?;
        for statement in include_str!("../migrations/001_clickhouse.sql")
            .replace("{db}", &self.config.database)
            .split(';')
            .filter(|s| !s.trim().is_empty())
        {
            self.sql(statement, vec![]).await?;
        }
        Ok(())
    }
    pub async fn insert_trades(&self, rows: &[Trade]) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let mut body = Vec::with_capacity(rows.len() * 110);
        for row in rows {
            encode_trade(&mut body, row)
        }
        // Bodies and ordering are stable across retries. Replay also deduplicates
        // adjacent record IDs, independently of CH's finite deduplication window.
        self.sql(&format!("INSERT INTO {}.executions ({TRADE_COLUMNS}) SETTINGS async_insert=1,wait_for_async_insert=1,async_insert_deduplicate=1,insert_deduplicate=1,async_insert_use_adaptive_busy_timeout=0,async_insert_busy_timeout_ms=100 FORMAT RowBinary",self.config.database),body).await?;
        Ok(())
    }
    pub async fn read(
        &self,
        market: u8,
        day: i32,
        revision: u64,
        after: u64,
        limit: usize,
    ) -> Result<Vec<Trade>> {
        let bytes=self.sql(&format!("SELECT {TRADE_COLUMNS} FROM {}.executions WHERE market={market} AND day={day} AND revision={revision} AND ordinal>{after} ORDER BY ordinal LIMIT {} FORMAT RowBinary",self.config.database,limit.min(READ_BATCH)),vec![]).await?;
        let mut input = Input::new(&bytes);
        let mut rows = Vec::new();
        while !input.done() {
            let row = decode_trade(&mut input)?;
            if let Some(last) = rows.last() {
                let previous: &Trade = last;
                if previous.ordinal == row.ordinal {
                    ensure!(*previous == row, "conflicting persisted execution");
                    continue;
                }
            }
            rows.push(row);
        }
        Ok(rows)
    }
    pub async fn days(&self, market: u8, start: i32) -> Result<Vec<i32>> {
        let bytes=self.sql(&format!("SELECT DISTINCT day FROM {}.executions WHERE market={market} AND day>={start} AND revision=0 ORDER BY day FORMAT JSONEachRow",self.config.database),vec![]).await?;
        #[derive(Deserialize)]
        struct Row {
            day: i32,
        }
        Ok(json_rows::<Row>(&bytes)?
            .into_iter()
            .map(|v| v.day)
            .collect())
    }
    pub async fn latest(&self, market: u8) -> Result<Option<Trade>> {
        let bytes=self.sql(&format!("SELECT {TRADE_COLUMNS} FROM {}.executions WHERE market={market} AND revision=0 ORDER BY day DESC,ordinal DESC LIMIT 1 FORMAT RowBinary",self.config.database),vec![]).await?;
        if bytes.is_empty() {
            return Ok(None);
        }
        Ok(Some(decode_trade(&mut Input::new(&bytes))?))
    }
    pub async fn insert_bars(&self, rows: &[StoredBar]) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let body = encode_bars(rows)?;
        self.sql(&format!("INSERT INTO {}.bars ({BAR_COLUMNS}) SETTINGS async_insert=0,insert_deduplicate=1 FORMAT Native",self.config.database),body).await?;
        Ok(())
    }
    pub async fn bars(&self, series: u64, before: u64, limit: usize) -> Result<Vec<StoredBar>> {
        let bytes=self.sql(&format!("SELECT {BAR_COLUMNS} FROM {}.bars WHERE series_id={series} AND bar_index<{before} ORDER BY bar_index DESC LIMIT {} FORMAT RowBinary",self.config.database,limit.min(1000)),vec![]).await?;
        let mut input = Input::new(&bytes);
        let mut rows: Vec<StoredBar> = Vec::new();
        while !input.done() {
            let row = decode_bar(&mut input)?;
            if let Some(last) = rows.last()
                && last.value.bar_index == row.value.bar_index
            {
                ensure!(last.value == row.value, "conflicting chart bar");
                continue;
            }
            rows.push(row)
        }
        Ok(rows)
    }
    pub async fn history_start(
        &self,
        series: u64,
        before: u64,
        cutoff: i64,
    ) -> Result<Option<u64>> {
        let data=self.sql(&format!("SELECT minOrNull(bar_index) AS first FROM {}.bars WHERE series_id={series} AND bar_index<{before} AND ended_us>={cutoff} FORMAT JSONEachRow",self.config.database),vec![]).await?;
        #[derive(Deserialize)]
        struct Row {
            first: Option<u64>,
        }
        Ok(json_rows::<Row>(&data)?.first().and_then(|r| r.first))
    }
    pub async fn minute_totals(
        &self,
        exchange: Exchange,
        start: i64,
        end: i64,
    ) -> Result<Vec<Minute>> {
        let bytes=self.sql(&format!("SELECT intDiv(timestamp_us,60000000)*60000000 AS minute,count() AS count,toString(sum(toUInt128(quantity_atoms))) AS volume FROM (SELECT * FROM {}.executions FINAL WHERE market={} AND revision=0 AND timestamp_us>={start} AND timestamp_us<{end}) GROUP BY minute ORDER BY minute FORMAT JSONEachRow",self.config.database,exchange.id()),vec![]).await?;
        json_rows(&bytes)
    }
}
#[derive(Clone, Deserialize, Serialize)]
pub struct Minute {
    pub minute: i64,
    pub count: u64,
    pub volume: String,
}
pub fn json_rows<T: for<'a> Deserialize<'a>>(bytes: &[u8]) -> Result<Vec<T>> {
    bytes
        .split(|b| *b == b'\n')
        .filter(|s| !s.is_empty())
        .map(|s| Ok(serde_json::from_slice(s)?))
        .collect()
}
fn put(out: &mut Vec<u8>, n: u64) {
    out.extend_from_slice(&n.to_le_bytes())
}
fn varint(out: &mut Vec<u8>, mut n: usize) {
    while n >= 128 {
        out.push((n as u8 & 127) | 128);
        n >>= 7
    }
    out.push(n as u8)
}
fn string(out: &mut Vec<u8>, s: &str) {
    varint(out, s.len());
    out.extend_from_slice(s.as_bytes())
}
pub fn encode_trade(out: &mut Vec<u8>, r: &Trade) {
    out.push(r.market);
    out.extend_from_slice(&r.day.to_le_bytes());
    for n in [r.revision, r.ordinal, r.segment, r.raw.timestamp as u64] {
        put(out, n)
    }
    string(out, &r.raw.id);
    for n in [r.raw.sequence, r.raw.price as u64, r.raw.quantity] {
        put(out, n)
    }
    out.push(r.raw.buy as u8);
    put(out, r.spec.tick as u64);
    put(out, r.spec.step)
}
pub struct Input<'a> {
    data: &'a [u8],
    at: usize,
}
impl<'a> Input<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self { data, at: 0 }
    }
    pub fn done(&self) -> bool {
        self.at == self.data.len()
    }
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        let end = self.at.checked_add(n).context("binary length overflow")?;
        let bytes = self
            .data
            .get(self.at..end)
            .context("truncated binary response")?;
        self.at = end;
        Ok(bytes)
    }
    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into()?))
    }
    fn byte(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }
    fn varint(&mut self) -> Result<usize> {
        let mut n = 0;
        for shift in (0..64).step_by(7) {
            let b = self.byte()?;
            n |= usize::from(b & 127)
                .checked_shl(shift)
                .context("invalid binary length")?;
            if b < 128 {
                return Ok(n);
            }
        }
        anyhow::bail!("invalid binary length")
    }
    fn string(&mut self) -> Result<String> {
        let n = self.varint()?;
        ensure!(n <= 128, "oversized identity");
        Ok(std::str::from_utf8(self.take(n)?)?.into())
    }
    fn array(&mut self) -> Result<Vec<u64>> {
        let n = self.varint()?;
        ensure!(n <= 4000, "CHART_BAR_TOO_DENSE");
        (0..n).map(|_| self.u64()).collect()
    }
}
pub fn decode_trade(input: &mut Input<'_>) -> Result<Trade> {
    let market = input.byte()?;
    let day = i32::from_le_bytes(input.take(4)?.try_into()?);
    let revision = input.u64()?;
    let ordinal = input.u64()?;
    let segment = input.u64()?;
    let timestamp = input.u64()? as i64;
    let id = input.string()?;
    let sequence = input.u64()?;
    let price = input.u64()? as i64;
    let quantity = input.u64()?;
    let buy = input.byte()?;
    ensure!(buy <= 1, "invalid side");
    let spec = Spec {
        tick: input.u64()? as i64,
        step: input.u64()?,
    };
    let raw = RawTrade {
        id,
        timestamp,
        sequence,
        price,
        quantity,
        buy: buy == 1,
    };
    raw.validate()?;
    Ok(Trade {
        market,
        day,
        revision,
        ordinal,
        segment,
        spec,
        raw,
    })
}
pub fn encode_bars(rows: &[StoredBar]) -> Result<Vec<u8>> {
    let types = [
        "UInt64",
        "UInt64",
        "Int64",
        "Int64",
        "Int64",
        "UInt64",
        "Int64",
        "Int64",
        "Int64",
        "Int64",
        "UInt64",
        "UInt64",
        "UInt64",
        "Bool",
        "Array(Int64)",
        "Array(UInt64)",
        "Array(UInt64)",
    ];
    let names: Vec<_> = BAR_COLUMNS.split(',').collect();
    let mut bytes = Vec::new();
    varint(&mut bytes, names.len());
    varint(&mut bytes, rows.len());
    let specs: Vec<_> = rows
        .iter()
        .map(|r| {
            Ok((
                crate::model::atoms(&r.value.tick_size)?,
                crate::model::atoms(&r.value.quantity_step)?,
            ))
        })
        .collect::<Result<_>>()?;
    for (column, (name, kind)) in names.iter().zip(types).enumerate() {
        string(&mut bytes, name);
        string(&mut bytes, kind);
        if column >= 14 {
            let mut offset = 0;
            for row in rows {
                offset += row.value.levels.len() as u64;
                put(&mut bytes, offset)
            }
            for row in rows {
                for (p, b, s) in &row.value.levels {
                    put(
                        &mut bytes,
                        match column {
                            14 => *p as u64,
                            15 => *b,
                            _ => *s,
                        },
                    )
                }
            }
            continue;
        }
        for (index, row) in rows.iter().enumerate() {
            let b = &row.value;
            if column == 13 {
                bytes.push(b.break_before as u8);
                continue;
            }
            let n = match column {
                0 => row.series,
                1 => b.bar_index,
                2 => b.opened_at_ms * 1000,
                3 => row.ended as u64,
                4 => specs[index].0,
                5 => specs[index].1,
                6 => b.open_tick as u64,
                7 => b.high_tick as u64,
                8 => b.low_tick as u64,
                9 => b.close_tick as u64,
                10 => b.buy_units,
                11 => b.sell_units,
                _ => b.trade_count,
            };
            put(&mut bytes, n)
        }
    }
    Ok(bytes)
}
fn decode_bar(i: &mut Input<'_>) -> Result<StoredBar> {
    let series = i.u64()?;
    let bar_index = i.u64()?;
    let opened_at_ms = i.u64()? / 1000;
    let ended = i.u64()? as i64;
    let tick_size = decimal(i.u64()?);
    let quantity_step = decimal(i.u64()?);
    let open_tick = i.u64()? as i64;
    let high_tick = i.u64()? as i64;
    let low_tick = i.u64()? as i64;
    let close_tick = i.u64()? as i64;
    let buy_units = i.u64()?;
    let sell_units = i.u64()?;
    let trade_count = i.u64()?;
    let break_before = i.byte()? != 0;
    let p = i.array()?;
    let b = i.array()?;
    let s = i.array()?;
    ensure!(
        p.len() == b.len() && b.len() == s.len(),
        "invalid stored levels"
    );
    Ok(StoredBar {
        series,
        ended,
        value: ChartBar {
            bar_index,
            opened_at_ms,
            break_before,
            tick_size,
            quantity_step,
            open_tick,
            high_tick,
            low_tick,
            close_tick,
            buy_units,
            sell_units,
            trade_count,
            levels: p
                .into_iter()
                .zip(b)
                .zip(s)
                .map(|((p, b), s)| (p as i64, b, s))
                .collect(),
        },
    })
}
