use crate::{
    config::{DAY_US, Exchange, SCALE},
    engine,
};
use anyhow::{Result, bail, ensure};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Spec {
    pub tick: i64,
    pub step: u64,
}
impl Spec {
    pub fn initial(exchange: Exchange) -> Self {
        Self {
            tick: 100_000_000_000,
            step: if exchange == Exchange::Hyperliquid {
                10_000_000
            } else {
                1_000_000_000
            },
        }
    }
    pub fn id(self) -> u64 {
        (xxhash_rust::xxh3::xxh3_64(&[self.tick.to_le_bytes(), self.step.to_le_bytes()].concat())
            & i64::MAX as u64)
            .max(1)
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawTrade {
    pub id: String,
    pub timestamp: i64,
    pub sequence: u64,
    pub price: i64,
    pub quantity: u64,
    pub buy: bool,
}
impl RawTrade {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            !self.id.is_empty() && self.id.len() <= 128,
            "invalid execution identity"
        );
        ensure!(
            self.price > 0 && self.quantity > 0 && self.timestamp > 0,
            "invalid execution values"
        );
        Ok(())
    }
    pub fn same_execution(&self, other: &Self) -> bool {
        self.id == other.id
            && self.timestamp == other.timestamp
            && self.price == other.price
            && self.quantity == other.quantity
            && self.buy == other.buy
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Trade {
    pub market: u8,
    // Monotonic source partition; timestamp regressions do not change order.
    pub day: i32,
    pub revision: u64,
    pub ordinal: u64,
    pub segment: u64,
    pub spec: Spec,
    pub raw: RawTrade,
}
impl Trade {
    pub fn cursor(&self) -> Cursor {
        Cursor {
            day: self.day,
            revision: self.revision,
            ordinal: self.ordinal,
        }
    }
    pub fn units(&self) -> Result<(i64, u64)> {
        ensure!(
            self.spec.tick > 0
                && self.spec.step > 0
                && self.raw.price % self.spec.tick == 0
                && self.raw.quantity.is_multiple_of(self.spec.step),
            "CHART_INVALID_SPECIFICATION"
        );
        Ok((
            self.raw.price / self.spec.tick,
            self.raw.quantity / self.spec.step,
        ))
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Cursor {
    pub day: i32,
    pub revision: u64,
    pub ordinal: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct Checkpoint {
    pub cursor: Option<Cursor>,
    pub segment: u64,
    pub state: engine::State,
    pub spec: Option<Spec>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Coverage {
    pub status: CoverageStatus,
    pub verified_through_ms: Option<i64>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CoverageStatus {
    Live,
    RepairPending,
    GapRecorded,
}
impl Default for Coverage {
    fn default() -> Self {
        Self {
            status: CoverageStatus::Live,
            verified_through_ms: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChartBar {
    pub bar_index: u64,
    pub opened_at_ms: u64,
    pub break_before: bool,
    pub tick_size: String,
    pub quantity_step: String,
    pub open_tick: i64,
    pub high_tick: i64,
    pub low_tick: i64,
    pub close_tick: i64,
    pub buy_units: u64,
    pub sell_units: u64,
    pub trade_count: u64,
    pub levels: Vec<(i64, u64, u64)>,
}
pub fn safe(value: u128) -> Result<u64> {
    ensure!(value <= 9_007_199_254_740_991, "CHART_VALUE_OVERFLOW");
    Ok(value as u64)
}
impl ChartBar {
    pub fn closed(bar: &engine::Bar, spec: Spec) -> Result<Self> {
        for price in [bar.open, bar.high, bar.low, bar.close] {
            ensure!(price > 0, "invalid chart price");
            safe(price as u128)?;
        }
        Ok(Self {
            bar_index: safe(bar.index.into())?,
            opened_at_ms: safe((bar.opened / 1000).into())?,
            break_before: bar.gap,
            tick_size: decimal(spec.tick as u64),
            quantity_step: decimal(spec.step),
            open_tick: bar.open,
            high_tick: bar.high,
            low_tick: bar.low,
            close_tick: bar.close,
            buy_units: safe(bar.buy)?,
            sell_units: safe(bar.sell)?,
            trade_count: safe(bar.count.into())?,
            levels: bar
                .levels
                .iter()
                .map(|(p, (b, s))| Ok((*p, safe(*b)?, safe(*s)?)))
                .collect::<Result<_>>()?,
        })
    }
    pub fn active(checkpoint: &Checkpoint) -> Result<Option<Self>> {
        let Some(active) = &checkpoint.state.active else {
            return Ok(None);
        };
        let Some(order) = active.first_order else {
            return Ok(None);
        };
        let Some(spec) = checkpoint.spec else {
            bail!("missing checkpoint specification")
        };
        for price in [
            active.open_tick,
            active.high_tick,
            active.low_tick,
            active.last_tick,
        ] {
            ensure!(price > 0, "CHART_INVALID_PRICE");
            safe(price as u128)?;
        }
        let mut levels: Vec<_> = active
            .levels
            .iter()
            .map(|(p, (b, s))| Ok((*p, safe(*b)?, safe(*s)?)))
            .collect::<Result<_>>()?;
        levels.sort_unstable_by_key(|v| v.0);
        Ok(Some(Self {
            bar_index: safe(checkpoint.state.next_bar_index.into())?,
            opened_at_ms: safe((order[0] / 1000).into())?,
            break_before: active.break_before,
            tick_size: decimal(spec.tick as u64),
            quantity_step: decimal(spec.step),
            open_tick: active.open_tick,
            high_tick: active.high_tick,
            low_tick: active.low_tick,
            close_tick: active.last_tick,
            buy_units: safe(active.buy_units)?,
            sell_units: safe(active.sell_units)?,
            trade_count: safe(active.trade_count.into())?,
            levels,
        }))
    }
}
pub fn decimal(atoms: u64) -> String {
    if atoms.is_multiple_of(SCALE) {
        return (atoms / SCALE).to_string();
    }
    format!("{}.{:012}", atoms / SCALE, atoms % SCALE)
        .trim_end_matches('0')
        .to_owned()
}
/// Parse exchange decimals without floats, rounding, or arbitrary precision fallback.
pub fn atoms(text: &str) -> Result<u64> {
    ensure!(!text.is_empty() && text.len() <= 80, "invalid decimal");
    let mut parts = text.split('.');
    let whole = parts.next().unwrap();
    let fraction = parts.next().unwrap_or("");
    ensure!(
        parts.next().is_none()
            && !whole.is_empty()
            && whole
                .bytes()
                .chain(fraction.bytes())
                .all(|b| b.is_ascii_digit()),
        "invalid decimal"
    );
    let fraction = fraction.trim_end_matches('0');
    ensure!(
        fraction.len() <= 12,
        "decimal exceeds exact storage precision"
    );
    let base = whole
        .parse::<u64>()?
        .checked_mul(SCALE)
        .ok_or_else(|| anyhow::anyhow!("decimal overflow"))?;
    let tail = if fraction.is_empty() {
        0
    } else {
        fraction.parse::<u64>()? * 10u64.pow(12 - fraction.len() as u32)
    };
    base.checked_add(tail)
        .ok_or_else(|| anyhow::anyhow!("decimal overflow"))
}
pub fn utc_day(value: i64) -> i32 {
    value.div_euclid(DAY_US) as i32
}
