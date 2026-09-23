//! Free public archives are the authority for closed Binance/Bybit days.
//! A revision is invisible until the entire archive and captured overlap pass.
use crate::{
    config::{DAY_US, Exchange, READ_BATCH, day, now_us},
    model::{RawTrade, Spec, Trade, atoms},
    service::Service,
    storage::Store,
};
use anyhow::{Context, Result, bail, ensure};
use chrono::DateTime;
use futures_util::StreamExt;
use rusqlite::{Connection, params};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    io::Read,
    path::{Path, PathBuf},
    time::Duration,
};

const DOWNLOAD_LIMIT: u64 = 2 * 1024 * 1024 * 1024;
const EXPANDED_LIMIT: u64 = 16 * 1024 * 1024 * 1024;
pub async fn audit_loop(service: Service) -> Result<()> {
    let client = crate::venues::http_client()?;
    let mut timer = tokio::time::interval(Duration::from_secs(300));
    timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {_=service.health.stop.cancelled()=>return Ok(()),_=timer.tick()=>{}}
        for exchange in [Exchange::Binance, Exchange::Bybit] {
            if let Err(error) = audit(&service, &client, exchange).await {
                tracing::warn!(%error,exchange=exchange.name(),"execution audit delayed");
            }
        }
    }
}
pub fn archive_url(exchange: Exchange, partition: i32) -> Result<String> {
    ensure!(
        exchange.recoverable(),
        "Hyperliquid paid history is disabled"
    );
    let date = DateTime::from_timestamp(i64::from(partition) * 86400, 0)
        .context("invalid archive date")?
        .format("%Y-%m-%d");
    Ok(if exchange == Exchange::Binance {
        format!(
            "https://data.binance.vision/data/futures/um/daily/trades/BTCUSDT/BTCUSDT-trades-{date}.zip"
        )
    } else {
        format!("https://public.bybit.com/trading/BTCUSDT/BTCUSDT{date}.csv.gz")
    })
}
pub async fn run(service: Service) -> Result<()> {
    let client = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(900))
        .user_agent("crypto-scanner-footprints-rust/1")
        .build()?;
    loop {
        if service.health.stop.is_cancelled() {
            return Ok(());
        }
        service
            .db
            .call(|c| {
                let tx = c.transaction()?;
                for market in [1, 2] {
                    for partition in day(now_us()) - 30..day(now_us()) {
                        tx.execute(
                            "INSERT OR IGNORE INTO archives(market,day) VALUES(?,?)",
                            params![market, partition],
                        )?;
                    }
                }
                tx.commit()?;
                Ok(())
            })
            .await?;
        let jobs=service.db.call(|c|{let mut q=c.prepare("SELECT market,day FROM archives WHERE status!='verified' AND attempted<? ORDER BY day DESC,market LIMIT 2")?;Ok(q.query_map([now_us()-900_000_000],|r|Ok((r.get::<_,u8>(0)?,r.get::<_,i32>(1)?)))?.collect::<rusqlite::Result<Vec<_>>>()?)}).await?;
        if jobs.is_empty() {
            tokio::select! {_=service.health.stop.cancelled()=>return Ok(()),_=tokio::time::sleep(Duration::from_secs(15))=>{}}
            continue;
        }
        for (market, partition) in jobs {
            if service.health.stop.is_cancelled() {
                return Ok(());
            }
            let exchange = Exchange::from_id(market)?;
            service.db.call(move|c|{c.execute("UPDATE archives SET attempted=?,status='repairing' WHERE market=? AND day=?",params![now_us(),market,partition])?;Ok(())}).await?;
            let result = tokio::select! {_=service.health.stop.cancelled()=>return Ok(()),result=repair(&service,&client,exchange,partition)=>result};
            if let Err(error) = result {
                let message = error.to_string();
                tracing::warn!(exchange=exchange.name(),partition,%error,"archive remains pending");
                service
                    .db
                    .call(move |c| {
                        c.execute(
                            "UPDATE archives SET status='pending',error=? WHERE market=? AND day=?",
                            params![message, market, partition],
                        )?;
                        Ok(())
                    })
                    .await?;
            }
        }
    }
}
async fn download(
    client: &reqwest::Client,
    url: &str,
    dir: &Path,
    reserve: u64,
) -> Result<(tempfile::NamedTempFile, String)> {
    std::fs::create_dir_all(dir)?;
    ensure!(
        fs2::available_space(dir)? > reserve + DOWNLOAD_LIMIT,
        "recovery disk reserve reached"
    );
    let response = client.get(url).send().await?.error_for_status()?;
    ensure!(
        response
            .content_length()
            .is_none_or(|n| n <= DOWNLOAD_LIMIT),
        "oversized archive"
    );
    let file = tempfile::NamedTempFile::new_in(dir)?;
    let mut writer = tokio::fs::File::from_std(file.reopen()?);
    let mut stream = response.bytes_stream();
    let mut count = 0u64;
    let mut hash = Sha256::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        count += chunk.len() as u64;
        ensure!(count <= DOWNLOAD_LIMIT, "archive download limit exceeded");
        ensure!(
            fs2::available_space(dir)? > reserve,
            "recovery disk reserve reached"
        );
        hash.update(&chunk);
        tokio::io::AsyncWriteExt::write_all(&mut writer, &chunk).await?;
    }
    tokio::io::AsyncWriteExt::flush(&mut writer).await?;
    drop(writer);
    Ok((
        file,
        hash.finalize().iter().map(|b| format!("{b:02x}")).collect(),
    ))
}
pub struct Archive {
    pub path: PathBuf,
    pub rows: u64,
    pub volume: u128,
    pub first: RawTrade,
    pub last: RawTrade,
}
pub fn parse_archive(
    path: &Path,
    output: &Path,
    exchange: Exchange,
    partition: i32,
) -> Result<Archive> {
    parse_archive_cancellable(
        path,
        output,
        exchange,
        partition,
        &tokio_util::sync::CancellationToken::new(),
    )
}
fn parse_archive_cancellable(
    path: &Path,
    output: &Path,
    exchange: Exchange,
    partition: i32,
    stop: &tokio_util::sync::CancellationToken,
) -> Result<Archive> {
    let file = std::fs::File::open(path)?;
    if exchange == Exchange::Binance {
        let mut zip = zip::ZipArchive::new(file)?;
        ensure!(zip.len() == 1, "unexpected archive members");
        let member = zip.by_index(0)?;
        ensure!(
            member.name().ends_with(".csv") && member.size() <= EXPANDED_LIMIT,
            "invalid archive member"
        );
        parse_csv_cancellable(member, output, exchange, partition, stop)
    } else {
        parse_csv_cancellable(
            flate2::read::GzDecoder::new(file),
            output,
            exchange,
            partition,
            stop,
        )
    }
}
pub fn parse_csv(
    reader: impl Read,
    output: &Path,
    exchange: Exchange,
    partition: i32,
) -> Result<Archive> {
    parse_csv_cancellable(
        reader,
        output,
        exchange,
        partition,
        &tokio_util::sync::CancellationToken::new(),
    )
}
fn parse_csv_cancellable(
    reader: impl Read,
    output: &Path,
    exchange: Exchange,
    partition: i32,
    stop: &tokio_util::sync::CancellationToken,
) -> Result<Archive> {
    ensure!(
        exchange.recoverable(),
        "Hyperliquid historical recovery is disabled"
    );
    let mut csv = csv::ReaderBuilder::new()
        .has_headers(false)
        .from_reader(reader.take(EXPANDED_LIMIT + 1));
    let mut db = Connection::open(output)?;
    db.execute_batch("PRAGMA journal_mode=OFF;PRAGMA synchronous=OFF;PRAGMA cache_size=-65536;CREATE TABLE trades(ordinal INTEGER PRIMARY KEY,id TEXT UNIQUE NOT NULL,payload BLOB NOT NULL);")?;
    let tx = db.transaction()?;
    let mut rows = 0u64;
    let mut volume = 0u128;
    let mut first = None;
    let mut last: Option<RawTrade> = None;
    let mut headers = None;
    {
        let mut insert = tx.prepare("INSERT INTO trades VALUES(?,?,?)")?;
        for record in csv.records() {
            if rows.is_multiple_of(1000) {
                ensure!(!stop.is_cancelled(), "archive parsing interrupted");
            }
            let record = record?;
            if rows == 0
                && headers.is_none()
                && record.get(0).is_some_and(|v| v == "id" || v == "timestamp")
            {
                headers = Some(record);
                continue;
            }
            let get = |name: &str, fallback: usize| -> Result<&str> {
                let index = if let Some(h) = &headers {
                    h.iter()
                        .position(|v| v == name)
                        .with_context(|| format!("missing archive column {name}"))?
                } else {
                    fallback
                };
                record.get(index).context("truncated archive row")
            };
            let raw = if exchange == Exchange::Binance {
                let id = get("id", 0)?.parse::<u64>()?;
                RawTrade {
                    id: id.to_string(),
                    sequence: id,
                    timestamp: get("time", 4)?
                        .parse::<i64>()?
                        .checked_mul(1000)
                        .context("timestamp overflow")?,
                    price: i64::try_from(atoms(get("price", 1)?)?)?,
                    quantity: atoms(get("qty", 2)?)?,
                    buy: match get("is_buyer_maker", 5)? {
                        "true" | "True" => false,
                        "false" | "False" => true,
                        _ => bail!("invalid archive maker flag"),
                    },
                }
            } else {
                ensure!(get("symbol", 1)? == "BTCUSDT", "unexpected archive market");
                RawTrade {
                    id: get("trdMatchID", 7)?.into(),
                    sequence: 0,
                    timestamp: timestamp_seconds(get("timestamp", 0)?)?,
                    price: i64::try_from(atoms(get("price", 4)?)?)?,
                    quantity: atoms(get("size", 3)?)?,
                    buy: match get("side", 2)? {
                        "Buy" => true,
                        "Sell" => false,
                        _ => bail!("invalid archive side"),
                    },
                }
            };
            raw.validate()?;
            ensure!(
                day(raw.timestamp) == partition,
                "execution outside archive date"
            );
            if let Some(previous) = &last {
                if exchange == Exchange::Binance {
                    ensure!(
                        raw.sequence > previous.sequence,
                        "Binance archive IDs regressed"
                    );
                } else {
                    ensure!(
                        raw.timestamp >= previous.timestamp,
                        "Bybit archive time regressed"
                    );
                }
            }
            // Millisecond timestamps match the public feed; original CSV row
            // order resolves same-time Bybit executions, never UUID sorting.
            rows += 1;
            ensure!(rows <= 50_000_000, "archive row limit exceeded");
            volume = volume
                .checked_add(raw.quantity.into())
                .context("archive volume overflow")?;
            insert
                .execute(params![rows, raw.id, postcard::to_allocvec(&raw)?])
                .context("duplicate archive execution identity")?;
            first.get_or_insert_with(|| raw.clone());
            last = Some(raw);
        }
    }
    ensure!(
        csv.position().byte() <= EXPANDED_LIMIT,
        "archive expansion limit exceeded"
    );
    tx.commit()?;
    Ok(Archive {
        path: output.into(),
        rows,
        volume,
        first: first.context("empty archive")?,
        last: last.context("empty archive")?,
    })
}
pub fn timestamp_seconds(text: &str) -> Result<i64> {
    let (whole, tail) = text.split_once('.').unwrap_or((text, ""));
    ensure!(
        tail.bytes().all(|b| b.is_ascii_digit()),
        "invalid archive timestamp"
    );
    let mut ms = tail
        .bytes()
        .take(3)
        .fold(0i64, |n, b| n * 10 + i64::from(b - b'0'));
    ms *= 10i64.pow(3 - tail.len().min(3) as u32);
    whole
        .parse::<i64>()?
        .checked_mul(1_000_000)
        .and_then(|n| n.checked_add(ms * 1000))
        .context("timestamp overflow")
}
async fn repair(
    service: &Service,
    client: &reqwest::Client,
    exchange: Exchange,
    partition: i32,
) -> Result<()> {
    let directory = service.config.directory.join("recovery");
    let url = archive_url(exchange, partition)?;
    let (file, checksum) = download(client, &url, &directory, service.config.reserve_bytes).await?;
    if exchange == Exchange::Binance {
        let expected = client
            .get(format!("{url}.CHECKSUM"))
            .send()
            .await?
            .error_for_status()?
            .text()
            .await?;
        ensure!(
            expected.split_whitespace().next() == Some(&checksum),
            "Binance archive checksum mismatch"
        );
    }
    let scratch = tempfile::NamedTempFile::new_in(&directory)?;
    let output = scratch.path().to_owned();
    let input = file.path().to_owned();
    let stop = service.health.stop.clone();
    let archive = tokio::task::spawn_blocking(move || {
        parse_archive_cancellable(&input, &output, exchange, partition, &stop)
    })
    .await??;
    let candles = candles(
        &crate::venues::http_client()?,
        exchange,
        i64::from(partition) * DAY_US,
        (i64::from(partition) + 1) * DAY_US,
        false,
    )
    .await?;
    ensure!(
        candles.len() == 1 && candles[0].start == i64::from(partition) * DAY_US,
        "incomplete daily candle evidence"
    );
    ensure!(
        archive.volume == candles[0].volume,
        "archive volume disagrees with daily candle"
    );
    if let Some(count) = candles[0].count {
        ensure!(
            count == archive.rows,
            "archive count disagrees with daily candle"
        );
    }
    let spec = verify_overlap(&service.store, &archive, exchange, partition).await?;
    // Content-derived IDs make interrupted uploads idempotent across restarts.
    let revision = (xxhash_rust::xxh3::xxh3_64(checksum.as_bytes()) & i64::MAX as u64).max(1);
    let mut after = 0;
    loop {
        if service.health.stop.is_cancelled() {
            bail!("recovery interrupted")
        }
        let path = archive.path.clone();
        let batch = tokio::task::spawn_blocking(move || -> Result<Vec<Trade>> {
            let c = Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
            let mut q = c.prepare(
                "SELECT ordinal,payload FROM trades WHERE ordinal>? ORDER BY ordinal LIMIT ?",
            )?;
            let records = q
                .query_map(params![after, READ_BATCH], |r| {
                    Ok((r.get::<_, u64>(0)?, r.get::<_, Vec<u8>>(1)?))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            records
                .into_iter()
                .map(|(ordinal, data)| {
                    let trade = Trade {
                        market: exchange.id(),
                        day: partition,
                        revision,
                        ordinal,
                        segment: 1,
                        spec,
                        raw: postcard::from_bytes(&data)?,
                    };
                    trade.units()?;
                    Ok(trade)
                })
                .collect()
        })
        .await??;
        if batch.is_empty() {
            break;
        }
        let start_after = after;
        after = batch.last().unwrap().ordinal;
        service.store.insert_trades(&batch).await?;
        if let Err(error) = service.prepare_archive_batch(batch, start_after).await {
            tracing::warn!(%error, "archive cache preparation skipped");
        }
    }
    ensure!(after == archive.rows, "archive upload incomplete");
    let count = archive.rows;
    // Recheck capture after upload: collector delivery may have caught up while
    // the archive was being parsed. No conflicting revision becomes visible.
    verify_overlap(&service.store, &archive, exchange, partition).await?;
    service.db.call(move|c|{c.execute("UPDATE archives SET status='verified',revision=?,checksum=?,rows=?,error=NULL WHERE market=? AND day=?",params![revision,checksum,count,exchange.id(),partition])?;Ok(())}).await?;
    service.schedule_replacements(exchange).await?;
    tracing::info!(
        exchange = exchange.name(),
        partition,
        rows = count,
        "verified archive published"
    );
    Ok(())
}
async fn verify_overlap(
    store: &Store,
    archive: &Archive,
    exchange: Exchange,
    partition: i32,
) -> Result<Spec> {
    let mut after = 0;
    let mut ordinal = 0;
    let mut selected = None;
    loop {
        let rows = store
            .read(exchange.id(), partition, 0, after, READ_BATCH)
            .await?;
        if rows.is_empty() {
            break;
        }
        after = rows.last().unwrap().ordinal;
        let path = archive.path.clone();
        let (next, spec) = tokio::task::spawn_blocking(move || -> Result<(u64, Option<Spec>)> {
            let c = Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
            let mut q = c.prepare("SELECT ordinal,payload FROM trades WHERE id=?")?;
            let mut current = ordinal;
            let mut spec = selected;
            for row in rows {
                let (index, data): (u64, Vec<u8>) = q
                    .query_row([&row.raw.id], |r| Ok((r.get(0)?, r.get(1)?)))
                    .context("archive missing captured identity")?;
                let raw: RawTrade = postcard::from_bytes(&data)?;
                ensure!(
                    raw.same_execution(&row.raw),
                    "archive execution conflicts with capture"
                );
                ensure!(index > current, "archive order disagrees with capture");
                ensure!(
                    spec.is_none_or(|s| s == row.spec),
                    "archive spans instrument change; manual specification required"
                );
                spec = Some(row.spec);
                current = index;
            }
            Ok((current, spec))
        })
        .await??;
        ordinal = next;
        selected = spec;
    }
    Ok(selected.unwrap_or_else(|| Spec::initial(exchange)))
}
#[derive(Debug)]
pub struct Candle {
    pub start: i64,
    pub volume: u128,
    pub count: Option<u64>,
}
pub async fn candles(
    client: &reqwest::Client,
    exchange: Exchange,
    start: i64,
    end: i64,
    minute: bool,
) -> Result<Vec<Candle>> {
    ensure!(
        exchange.recoverable(),
        "Hyperliquid historical recovery is disabled"
    );
    let interval = if minute { "1" } else { "D" };
    let start_ms = (start / 1000).to_string();
    let end_ms = (end / 1000 - 1).to_string();
    let value: Value = if exchange == Exchange::Binance {
        client
            .get("https://fapi.binance.com/fapi/v1/klines")
            .query(&[
                ("symbol", "BTCUSDT"),
                ("interval", if minute { "1m" } else { "1d" }),
                ("startTime", &start_ms),
                ("endTime", &end_ms),
                ("limit", "1000"),
            ])
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?
    } else {
        client
            .get("https://api.bybit.com/v5/market/kline")
            .query(&[
                ("category", "linear"),
                ("symbol", "BTCUSDT"),
                ("interval", interval),
                ("start", &start_ms),
                ("end", &end_ms),
                ("limit", "1000"),
            ])
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?
    };
    let rows = if exchange == Exchange::Binance {
        value.as_array()
    } else {
        ensure!(value["retCode"] == 0, "Bybit candle request rejected");
        value["result"]["list"].as_array()
    }
    .context("invalid candle response")?;
    let mut candles = Vec::new();
    for r in rows {
        let timestamp = r[0]
            .as_i64()
            .or_else(|| r[0].as_str()?.parse().ok())
            .context("invalid candle timestamp")?
            * 1000;
        if timestamp >= start && timestamp < end {
            candles.push(Candle {
                start: timestamp,
                volume: u128::from(atoms(r[5].as_str().context("invalid candle volume")?)?),
                count: if exchange == Exchange::Binance {
                    Some(r[8].as_u64().context("invalid candle count")?)
                } else {
                    None
                },
            });
        }
    }
    candles.sort_by_key(|c| c.start);
    Ok(candles)
}
async fn audit(service: &Service, client: &reqwest::Client, exchange: Exchange) -> Result<()> {
    let end = now_us().div_euclid(60_000_000) * 60_000_000 - 120_000_000;
    let start = end - 600_000_000;
    let expected = candles(client, exchange, start, end, true).await?;
    ensure!(expected.len() == 10, "incomplete minute audit response");
    let actual = service.store.minute_totals(exchange, start, end).await?;
    for candle in expected {
        let matched = actual
            .iter()
            .find(|r| r.minute == candle.start)
            .is_some_and(|r| {
                r.volume.parse::<u128>().ok() == Some(candle.volume)
                    && candle.count.is_none_or(|n| n == r.count)
            });
        if !matched {
            service
                .db
                .call(move |c| {
                    c.execute(
                        "INSERT OR IGNORE INTO gaps VALUES(?,?,?,'minute_audit')",
                        params![exchange.id(), candle.start, candle.start + 60_000_000],
                    )?;
                    Ok(())
                })
                .await?;
        }
    }
    Ok(())
}
