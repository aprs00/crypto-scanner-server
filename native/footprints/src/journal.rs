use crate::{
    config::{Config, Exchange, day, now_us},
    db::Db,
    model::{RawTrade, Spec, Trade},
};
use anyhow::{Context, Result, ensure};
use rusqlite::{OptionalExtension, params};
use serde::{Deserialize, Serialize};
use std::path::Path;

const SCHEMA:&str="
CREATE TABLE IF NOT EXISTS journal_version(version INTEGER NOT NULL);
INSERT INTO journal_version SELECT 1 WHERE NOT EXISTS(SELECT 1 FROM journal_version);
CREATE TABLE IF NOT EXISTS pending(id INTEGER PRIMARY KEY, payload BLOB NOT NULL, rows INTEGER NOT NULL, created INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS recent(id TEXT PRIMARY KEY, ordinal INTEGER UNIQUE NOT NULL, payload BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS state(id INTEGER PRIMARY KEY CHECK(id=1), ordinal INTEGER NOT NULL, segment INTEGER NOT NULL, day INTEGER NOT NULL);
INSERT OR IGNORE INTO state VALUES(1,0,1,0);
CREATE TABLE IF NOT EXISTS specs(effective INTEGER PRIMARY KEY,tick INTEGER NOT NULL,step INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS gaps(id INTEGER PRIMARY KEY, start INTEGER NOT NULL,end INTEGER NOT NULL,reason TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS ownership(id INTEGER PRIMARY KEY CHECK(id=1),instance TEXT NOT NULL);
";
#[derive(Clone)]
pub struct Journal {
    pub db: Db,
    config: Config,
    exchange: Exchange,
}
#[derive(Default, Clone, Debug, Serialize, Deserialize)]
pub struct JournalStats {
    pub pending_rows: u64,
    pub pending_bytes: u64,
    pub oldest_pending_at: Option<i64>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Gap {
    pub start: i64,
    pub end: i64,
    pub reason: String,
}
impl Journal {
    pub fn promoted(directory: &Path, instance: &str) -> Result<bool> {
        let file = directory.join("journal.sqlite3");
        if !file.exists() {
            return Ok(false);
        }
        let db = rusqlite::Connection::open_with_flags(
            file,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )?;
        Ok(db
            .query_row("SELECT instance FROM ownership WHERE id=1", [], |r| {
                r.get::<_, String>(0)
            })
            .optional()?
            .as_deref()
            == Some(instance))
    }
    pub async fn activate(&self, instance: String) -> Result<()> {
        self.db.call(move|c|{c.execute("INSERT INTO ownership VALUES(1,?) ON CONFLICT(id) DO UPDATE SET instance=excluded.instance",[instance])?;Ok(())}).await
    }
    pub async fn open(config: &Config, exchange: Exchange) -> Result<Self> {
        let db = Db::open(&config.directory.join("journal.sqlite3"))?;
        let initial = Spec::initial(exchange);
        db.call(move |c| {
            c.execute_batch(SCHEMA)?;
            ensure!(
                c.query_row("SELECT version FROM journal_version", [], |r| r
                    .get::<_, i64>(0))?
                    == 1,
                "unsupported journal schema"
            );
            c.execute(
                "INSERT OR IGNORE INTO specs VALUES(0,?,?)",
                params![initial.tick, initial.step],
            )?;
            Ok(())
        })
        .await?;
        Ok(Self {
            db,
            config: config.clone(),
            exchange,
        })
    }
    pub async fn tail(&self) -> Result<Option<RawTrade>> {
        self.db
            .call(|c| {
                let bytes: Option<Vec<u8>> = c
                    .query_row(
                        "SELECT payload FROM recent ORDER BY ordinal DESC LIMIT 1",
                        [],
                        |r| r.get(0),
                    )
                    .optional()?;
                bytes.map(|b| Ok(postcard::from_bytes(&b)?)).transpose()
            })
            .await
    }
    pub async fn verify_delivery_history(&self, latest: Trade) -> Result<()> {
        let market = self.exchange.id();
        self.db
            .call(move |c| {
                let (ordinal, partition): (u64, i32) =
                    c.query_row("SELECT ordinal,day FROM state WHERE id=1", [], |r| {
                        Ok((r.get(0)?, r.get(1)?))
                    })?;
                ensure!(
                    latest.market == market && latest.ordinal <= ordinal && latest.day <= partition,
                    "JOURNAL_HISTORY_MISMATCH: restore the matching journal before delivery"
                );
                let bytes: Option<Vec<u8>> = c
                    .query_row(
                        "SELECT payload FROM recent WHERE ordinal=?",
                        [latest.ordinal],
                        |r| r.get(0),
                    )
                    .optional()?;
                if let Some(bytes) = bytes {
                    let known: RawTrade = postcard::from_bytes(&bytes)?;
                    ensure!(
                        known.same_execution(&latest.raw),
                        "JOURNAL_HISTORY_MISMATCH: stored ordinal belongs to another execution"
                    );
                }
                Ok(())
            })
            .await
    }
    pub fn peek_tail(directory: &Path) -> Result<Option<RawTrade>> {
        let file = directory.join("journal.sqlite3");
        if !file.exists() {
            return Ok(None);
        }
        let c = rusqlite::Connection::open_with_flags(
            file,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )?;
        let data: Option<Vec<u8>> = c
            .query_row(
                "SELECT payload FROM recent ORDER BY ordinal DESC LIMIT 1",
                [],
                |r| r.get(0),
            )
            .optional()?;
        data.map(|b| Ok(postcard::from_bytes(&b)?)).transpose()
    }
    pub async fn spec(&self, spec: Spec) -> Result<()> {
        self.db
            .call(move |c| {
                let latest: Spec = c.query_row(
                    "SELECT tick,step FROM specs ORDER BY effective DESC LIMIT 1",
                    [],
                    |r| {
                        Ok(Spec {
                            tick: r.get(0)?,
                            step: r.get(1)?,
                        })
                    },
                )?;
                if latest != spec {
                    c.execute(
                        "INSERT INTO specs VALUES(?,?,?)",
                        params![now_us(), spec.tick, spec.step],
                    )?;
                }
                Ok(())
            })
            .await
    }
    pub async fn gap(&self, start: i64, end: i64, reason: &str) -> Result<()> {
        let reason = reason.to_owned();
        self.db
            .call(move |c| {
                let tx = c.transaction()?;
                tx.execute(
                    "INSERT INTO gaps(start,end,reason) VALUES(?,?,?)",
                    params![start, end.max(start), reason],
                )?;
                tx.execute("UPDATE state SET segment=segment+1 WHERE id=1", [])?;
                tx.commit()?;
                Ok(())
            })
            .await
    }
    pub async fn gaps(&self) -> Result<Vec<Gap>> {
        self.db
            .call(|c| {
                let mut q =
                    c.prepare("SELECT start,end,reason FROM gaps WHERE end>=? ORDER BY id")?;
                Ok(q.query_map([now_us() - 31 * crate::config::DAY_US], |r| {
                    Ok(Gap {
                        start: r.get(0)?,
                        end: r.get(1)?,
                        reason: r.get(2)?,
                    })
                })?
                .collect::<rusqlite::Result<_>>()?)
            })
            .await
    }
    pub async fn append(&self, rows: Vec<RawTrade>) -> Result<usize> {
        let cfg = self.config.clone();
        let market = self.exchange.id();
        self.db
            .call(move |c| {
                if rows.is_empty() {
                    return Ok(0);
                }
                ensure!(
                    fs2::available_space(&cfg.directory)? >= cfg.reserve_bytes,
                    "JOURNAL_DISK_RESERVE"
                );
                let pending: u64 = c.query_row(
                    "SELECT coalesce(sum(length(payload)),0) FROM pending",
                    [],
                    |r| r.get(0),
                )?;
                let tx = c.transaction()?;
                let (mut ordinal, segment, mut partition): (u64, u64, i32) = tx.query_row(
                    "SELECT ordinal,segment,day FROM state WHERE id=1",
                    [],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                )?;
                let specs = tx
                    .prepare("SELECT effective,tick,step FROM specs ORDER BY effective")?
                    .query_map([], |r| {
                        Ok((
                            r.get::<_, i64>(0)?,
                            Spec {
                                tick: r.get(1)?,
                                step: r.get(2)?,
                            },
                        ))
                    })?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                let mut admitted = Vec::with_capacity(rows.len());
                {
                    let mut insert = tx.prepare_cached(
                        "INSERT OR IGNORE INTO recent(id,ordinal,payload) VALUES(?,?,?)",
                    )?;
                    let mut existing =
                        tx.prepare_cached("SELECT payload FROM recent WHERE id=?")?;
                    for raw in rows {
                        raw.validate()?;
                        let bytes = postcard::to_allocvec(&raw)?;
                        let next = ordinal
                            .checked_add(1)
                            .context("journal ordinal exhausted")?;
                        if insert.execute(params![raw.id, next, bytes])? == 0 {
                            let old: Vec<u8> = existing.query_row([&raw.id], |r| r.get(0))?;
                            ensure!(
                                raw.same_execution(&postcard::from_bytes(&old)?),
                                "CONFLICTING_EXECUTION_IDENTITY"
                            );
                            continue;
                        }
                        ordinal = next;
                        partition = partition.max(day(raw.timestamp));
                        let spec = specs
                            .iter()
                            .rev()
                            .find(|(at, _)| *at <= raw.timestamp)
                            .context("no instrument specification")?
                            .1;
                        admitted.push(Trade {
                            market,
                            day: partition,
                            revision: 0,
                            ordinal,
                            segment,
                            spec,
                            raw,
                        });
                    }
                }
                if admitted.is_empty() {
                    return Ok(0);
                }
                let bytes = postcard::to_allocvec(&admitted)?;
                ensure!(
                    pending + bytes.len() as u64 <= cfg.journal_bytes,
                    "JOURNAL_CAPACITY_REACHED"
                );
                tx.execute(
                    "INSERT INTO pending(payload,rows,created) VALUES(?,?,?)",
                    params![bytes, admitted.len(), now_us()],
                )?;
                tx.execute(
                    "UPDATE state SET ordinal=?,day=? WHERE id=1",
                    params![ordinal, partition],
                )?;
                tx.execute(
                    "DELETE FROM recent WHERE ordinal<?",
                    [ordinal.saturating_sub(50_000)],
                )?;
                tx.commit()?;
                Ok(admitted.len())
            })
            .await
    }
    pub async fn first(&self) -> Result<Option<(i64, Vec<Trade>)>> {
        self.db
            .call(|c| {
                let mut query =
                    c.prepare("SELECT id,payload FROM pending ORDER BY id LIMIT 1000")?;
                let mut records = query.query([])?;
                let mut rows = Vec::new();
                let mut last = None;
                let mut bytes = 0;
                while let Some(record) = records.next()? {
                    let data: Vec<u8> = record.get(1)?;
                    bytes += data.len();
                    rows.extend(postcard::from_bytes::<Vec<Trade>>(&data)?);
                    last = Some(record.get(0)?);
                    if rows.len() >= 25_000 || bytes >= 8 * 1024 * 1024 {
                        break;
                    }
                }
                Ok(last.map(|id| (id, rows)))
            })
            .await
    }
    pub async fn ack(&self, id: i64) -> Result<()> {
        self.db
            .call(move |c| {
                c.execute("DELETE FROM pending WHERE id<=?", [id])?;
                Ok(())
            })
            .await
    }
    pub async fn stats(&self) -> Result<JournalStats> {
        self.db.call(|c|Ok(c.query_row("SELECT coalesce(sum(rows),0),coalesce(sum(length(payload)),0),min(created) FROM pending",[],|r|Ok(JournalStats{pending_rows:r.get(0)?,pending_bytes:r.get(1)?,oldest_pending_at:r.get(2)?}))?)).await
    }
}
