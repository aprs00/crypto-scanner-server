use crate::{
    config::Exchange,
    model::{RawTrade, Spec, atoms},
};
use anyhow::{Context, Result, bail, ensure};
use serde_json::{Value, json};

pub enum Frame {
    Ready,
    Pong,
    Trades(Vec<RawTrade>),
    Ignore,
}
pub fn subscription(exchange: Exchange) -> Value {
    match exchange {
        Exchange::Binance => json!({"method":"SUBSCRIBE","params":["btcusdt@trade"],"id":1}),
        Exchange::Bybit => json!({"op":"subscribe","args":["publicTrade.BTCUSDT"]}),
        Exchange::Hyperliquid => {
            json!({"method":"subscribe","subscription":{"type":"trades","coin":"BTC"}})
        }
    }
}
fn string<'a>(v: &'a Value, k: &str) -> Result<&'a str> {
    v[k].as_str().with_context(|| format!("missing {k}"))
}
fn integer(v: &Value, k: &str) -> Result<u64> {
    v[k].as_u64()
        .or_else(|| v[k].as_str().and_then(|s| s.parse().ok()))
        .with_context(|| format!("invalid {k}"))
}
fn price(v: &Value, k: &str) -> Result<i64> {
    Ok(i64::try_from(atoms(string(v, k)?)?)?)
}
pub fn normalize(exchange: Exchange, v: &Value) -> Result<RawTrade> {
    let row = match exchange {
        Exchange::Binance => RawTrade {
            id: integer(v, "t")?.to_string(),
            timestamp: i64::try_from(integer(v, "T")?)?
                .checked_mul(1000)
                .context("timestamp overflow")?,
            sequence: integer(v, "t")?,
            price: price(v, "p")?,
            quantity: atoms(string(v, "q")?)?,
            buy: !v["m"].as_bool().context("invalid maker flag")?,
        },
        Exchange::Bybit => RawTrade {
            id: string(v, "i")?.into(),
            timestamp: i64::try_from(integer(v, "T")?)?
                .checked_mul(1000)
                .context("timestamp overflow")?,
            sequence: integer(v, "seq")?,
            price: price(v, "p")?,
            quantity: atoms(string(v, "v")?)?,
            buy: match string(v, "S")? {
                "Buy" => true,
                "Sell" => false,
                _ => bail!("invalid side"),
            },
        },
        Exchange::Hyperliquid => RawTrade {
            id: format!(
                "{}:{}:{}",
                integer(v, "time")?,
                string(v, "coin")?,
                integer(v, "tid")?
            ),
            timestamp: i64::try_from(integer(v, "time")?)?
                .checked_mul(1000)
                .context("timestamp overflow")?,
            sequence: 0,
            price: price(v, "px")?,
            quantity: atoms(string(v, "sz")?)?,
            buy: match string(v, "side")? {
                "B" => true,
                "A" => false,
                _ => bail!("invalid side"),
            },
        },
    };
    row.validate()?;
    Ok(row)
}
pub fn frame(exchange: Exchange, text: &str) -> Result<Frame> {
    let v: Value = serde_json::from_str(text)?;
    ensure!(v.is_object(), "expected exchange object");
    match exchange {
        Exchange::Binance => {
            if v["id"] == 1 {
                ensure!(
                    v.get("result") == Some(&Value::Null),
                    "subscription rejected"
                );
                return Ok(Frame::Ready);
            }
            let data = v.get("data").unwrap_or(&v);
            if data["e"] != "trade" || data["s"] != "BTCUSDT" {
                return Ok(Frame::Ignore);
            }
            if data["X"] == "NA"
                && (data["st"] == 1 || data["st"] == "1")
                && string(data, "p").ok().and_then(|s| atoms(s).ok()) == Some(0)
                && string(data, "q").ok().and_then(|s| atoms(s).ok()) == Some(0)
            {
                return Ok(Frame::Ignore);
            }
            Ok(Frame::Trades(vec![normalize(exchange, data)?]))
        }
        Exchange::Bybit => {
            if v["op"] == "ping" && v["ret_msg"] == "pong" || v["op"] == "pong" {
                return Ok(Frame::Pong);
            }
            if v["op"] == "subscribe" {
                ensure!(v["success"] == true, "subscription rejected");
                return Ok(Frame::Ready);
            }
            if v["topic"] != "publicTrade.BTCUSDT" {
                return Ok(Frame::Ignore);
            }
            let rows = v["data"].as_array().context("expected trade array")?;
            ensure!(rows.len() <= 1024, "oversized trade array");
            Ok(Frame::Trades(
                rows.iter()
                    .map(|r| normalize(exchange, r))
                    .collect::<Result<_>>()?,
            ))
        }
        Exchange::Hyperliquid => {
            if v["channel"] == "pong" {
                return Ok(Frame::Pong);
            }
            if v["channel"] == "error" {
                bail!("Hyperliquid subscription error")
            }
            if v["channel"] == "subscriptionResponse" {
                if v["data"]["method"] == "subscribe"
                    && v["data"]["subscription"]["type"] == "trades"
                    && v["data"]["subscription"]["coin"] == "BTC"
                {
                    return Ok(Frame::Ready);
                }
                return Ok(Frame::Ignore);
            }
            if v["channel"] != "trades" {
                return Ok(Frame::Ignore);
            }
            let rows = v["data"].as_array().context("expected trades")?;
            ensure!(rows.len() <= 4096, "oversized trade array");
            Ok(Frame::Trades(
                rows.iter()
                    .filter(|r| r["coin"] == "BTC")
                    .map(|r| normalize(exchange, r))
                    .collect::<Result<_>>()?,
            ))
        }
    }
}
pub fn http_client() -> Result<reqwest::Client> {
    Ok(reqwest::Client::builder()
        .connect_timeout(std::time::Duration::from_secs(5))
        .timeout(std::time::Duration::from_secs(10))
        .user_agent("crypto-scanner-footprints-rust/1")
        .build()?)
}
pub async fn recent(exchange: Exchange, client: &reqwest::Client) -> Result<Vec<RawTrade>> {
    ensure!(
        exchange.recoverable(),
        "Hyperliquid historical recovery is disabled"
    );
    let v: Value = match exchange {
        Exchange::Binance => {
            client
                .get("https://fapi.binance.com/fapi/v1/trades")
                .query(&[("symbol", "BTCUSDT"), ("limit", "1000")])
                .send()
                .await?
                .error_for_status()?
                .json()
                .await?
        }
        _ => {
            client
                .get("https://api.bybit.com/v5/market/recent-trade")
                .query(&[
                    ("category", "linear"),
                    ("symbol", "BTCUSDT"),
                    ("limit", "1000"),
                ])
                .send()
                .await?
                .error_for_status()?
                .json()
                .await?
        }
    };
    let mut rows=match exchange {
        Exchange::Binance=>v.as_array().context("invalid recent trades")?.iter().map(|r|normalize(exchange,&json!({"t":r["id"],"m":r["isBuyerMaker"],"T":r["time"],"p":r["price"],"q":r["qty"]}))).collect::<Result<Vec<_>>>()?,
        _=>{ensure!(v["retCode"]==0,"Bybit recent request rejected"); let mut rows=v["result"]["list"].as_array().context("invalid recent trades")?.iter().map(|r|normalize(exchange,&json!({"i":r["execId"],"S":r["side"],"T":r["time"],"p":r["price"],"v":r["size"],"seq":r["seq"]}))).collect::<Result<Vec<_>>>()?;rows.reverse();rows}
    };
    if exchange == Exchange::Binance {
        rows.sort_by_key(|r| r.sequence);
    }
    Ok(rows)
}
pub fn bridge(
    exchange: Exchange,
    last: &RawTrade,
    first: &RawTrade,
    rows: &[RawTrade],
) -> Result<Vec<RawTrade>> {
    let left = rows
        .iter()
        .position(|r| r.id == last.id)
        .context("recent window does not reach the durable tail")?;
    let right = rows
        .iter()
        .position(|r| r.id == first.id)
        .context("recent window does not overlap the new feed")?;
    ensure!(
        left < right && rows[left].same_execution(last) && rows[right].same_execution(first),
        "conflicting bridge endpoints"
    );
    let mut identities = std::collections::HashSet::new();
    for (i, row) in rows[left..=right].iter().enumerate() {
        ensure!(identities.insert(&row.id), "duplicate bridge identity");
        if i > 0 {
            let prior = &rows[left + i - 1];
            ensure!(row.sequence >= prior.sequence, "bridge order regressed");
            if exchange == Exchange::Binance {
                ensure!(
                    row.sequence > prior.sequence,
                    "Binance bridge order regressed"
                );
            }
        }
    }
    Ok(rows[left + 1..right].to_vec())
}
pub async fn specification(exchange: Exchange, client: &reqwest::Client) -> Result<Spec> {
    let spec = match exchange {
        Exchange::Binance => {
            let v: Value = client
                .get("https://fapi.binance.com/fapi/v1/exchangeInfo")
                .send()
                .await?
                .error_for_status()?
                .json()
                .await?;
            let row = v["symbols"]
                .as_array()
                .context("symbols missing")?
                .iter()
                .find(|r| r["symbol"] == "BTCUSDT")
                .context("BTCUSDT missing")?;
            let filters = row["filters"].as_array().context("filters missing")?;
            let p = filters
                .iter()
                .find(|r| r["filterType"] == "PRICE_FILTER")
                .context("price filter missing")?;
            let q = filters
                .iter()
                .find(|r| r["filterType"] == "LOT_SIZE")
                .context("lot filter missing")?;
            Spec {
                tick: price(p, "tickSize")?,
                step: atoms(string(q, "stepSize")?)?,
            }
        }
        Exchange::Bybit => {
            let v:Value=client.get("https://api.bybit.com/v5/market/instruments-info?category=linear&symbol=BTCUSDT").send().await?.error_for_status()?.json().await?;
            ensure!(v["retCode"] == 0, "Bybit spec request rejected");
            let r = &v["result"]["list"][0];
            Spec {
                tick: price(&r["priceFilter"], "tickSize")?,
                step: atoms(string(&r["lotSizeFilter"], "qtyStep")?)?,
            }
        }
        Exchange::Hyperliquid => {
            let v: Value = client
                .post("https://api.hyperliquid.xyz/info")
                .json(&json!({"type":"meta"}))
                .send()
                .await?
                .error_for_status()?
                .json()
                .await?;
            let r = v["universe"]
                .as_array()
                .context("universe missing")?
                .iter()
                .find(|r| r["name"] == "BTC")
                .context("BTC missing")?;
            let n = integer(r, "szDecimals")?;
            ensure!(n <= 6, "unsupported precision");
            Spec {
                tick: 10i64.pow(6 + n as u32),
                step: 10u64.pow(12 - n as u32),
            }
        }
    };
    ensure!(spec.tick > 0 && spec.step > 0, "invalid specification");
    Ok(spec)
}
