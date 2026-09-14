use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use std::{env, net::SocketAddr, path::PathBuf};

pub const DAY_US: i64 = 86_400_000_000;
pub const SCALE: u64 = 1_000_000_000_000;
pub const MAX_LEVELS: usize = 4_000;
pub const MAX_SERIES: usize = 16;
pub const READ_BATCH: usize = 100_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, clap::ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum Exchange {
    Binance,
    Bybit,
    Hyperliquid,
}
impl Exchange {
    pub const ALL: [Self; 3] = [Self::Binance, Self::Bybit, Self::Hyperliquid];
    pub fn id(self) -> u8 {
        match self {
            Self::Binance => 1,
            Self::Bybit => 2,
            Self::Hyperliquid => 3,
        }
    }
    pub fn name(self) -> &'static str {
        match self {
            Self::Binance => "binance",
            Self::Bybit => "bybit",
            Self::Hyperliquid => "hyperliquid",
        }
    }
    pub fn symbol(self) -> &'static str {
        if self == Self::Hyperliquid {
            "BTC"
        } else {
            "BTCUSDT"
        }
    }
    pub fn key(self) -> String {
        format!("{}:perpetual:{}", self.name(), self.symbol())
    }
    pub fn from_id(id: u8) -> Result<Self> {
        Self::ALL
            .into_iter()
            .find(|v| v.id() == id)
            .context("unknown market")
    }
    pub fn from_key(key: &str) -> Result<Self> {
        Self::ALL
            .into_iter()
            .find(|v| v.key() == key)
            .context("UNKNOWN_FOOTPRINT_MARKET")
    }
    pub fn recoverable(self) -> bool {
        self != Self::Hyperliquid
    }
    pub fn ws_url(self) -> &'static str {
        match self {
            Self::Binance => "wss://fstream.binance.com/public/ws",
            Self::Bybit => "wss://stream.bybit.com/v5/public/linear",
            Self::Hyperliquid => "wss://api.hyperliquid.xyz/ws",
        }
    }
}

#[derive(Clone)]
pub struct Config {
    pub directory: PathBuf,
    pub bind: SocketAddr,
    pub clickhouse_url: String,
    pub database: String,
    pub user: String,
    pub password: String,
    pub journal_bytes: u64,
    pub reserve_bytes: u64,
    pub origins: Vec<String>,
    pub collector_urls: Vec<String>,
}
impl Config {
    pub fn load() -> Result<Self> {
        let database = env::var("FOOTPRINT_DATABASE").unwrap_or_else(|_| "footprints_rust".into());
        ensure!(
            !database.is_empty()
                && database
                    .bytes()
                    .all(|c| c.is_ascii_alphanumeric() || c == b'_'),
            "invalid FOOTPRINT_DATABASE"
        );
        Ok(Self {
            directory: env::var("FOOTPRINT_DATA_DIR").unwrap_or_else(|_| "/var/lib/footprints".into()).into(),
            bind: env::var("FOOTPRINT_BIND").unwrap_or_else(|_| "0.0.0.0:8010".into()).parse()?,
            clickhouse_url: env::var("FOOTPRINT_CLICKHOUSE_URL").unwrap_or_else(|_| "http://clickhouse:8123".into()),
            database,
            user: env::var("CLICKHOUSE_USER").unwrap_or_else(|_| "footprints".into()),
            password: env::var("CLICKHOUSE_PASSWORD").unwrap_or_default(),
            journal_bytes: number("FOOTPRINT_OUTBOX_MAX_BYTES", 2*1024*1024*1024)?,
            reserve_bytes: number("FOOTPRINT_DISK_RESERVE_BYTES", 5*1024*1024*1024)?,
            origins: env::var("FOOTPRINT_ALLOWED_ORIGINS").unwrap_or_else(|_| "http://localhost:5173,http://localhost:3000".into()).split(',').map(|v|v.trim().to_owned()).filter(|v|!v.is_empty()).collect(),
            collector_urls: env::var("FOOTPRINT_COLLECTOR_URLS").unwrap_or_else(|_| "http://binance-footprints:8010,http://bybit-footprints:8010,http://hyperliquid-footprints:8010".into()).split(',').map(str::to_owned).collect(),
        })
    }
}
fn number(key: &str, default: u64) -> Result<u64> {
    env::var(key)
        .map(|v| v.parse().with_context(|| format!("invalid {key}")))
        .unwrap_or(Ok(default))
}
pub fn now_us() -> i64 {
    chrono::Utc::now().timestamp_micros()
}
pub fn day(now: i64) -> i32 {
    now.div_euclid(DAY_US) as i32
}
