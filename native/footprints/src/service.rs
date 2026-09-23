use crate::{
    config::{Config, DAY_US, Exchange, MAX_LEVELS, MAX_SERIES, day, now_us},
    db::Db,
    engine::Engine,
    health::Health,
    model::{ChartBar, Checkpoint, Coverage, CoverageStatus, Spec, Trade},
    prepared::{Batch, Cache},
    storage::{Store, StoredBar},
};
use anyhow::{Context, Result, ensure};
use rusqlite::{OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::{Notify, broadcast};

const PROJECT_LEVELS: usize = 500_000;
const HISTORY_BATCHES: usize = 30;
const HISTORY_SLICE: Duration = Duration::from_secs(2);

const SCHEMA:&str="
CREATE TABLE IF NOT EXISTS control_version(version INTEGER NOT NULL);
INSERT INTO control_version SELECT 1 WHERE NOT EXISTS(SELECT 1 FROM control_version);
CREATE TABLE IF NOT EXISTS series(id INTEGER PRIMARY KEY AUTOINCREMENT,public_key TEXT UNIQUE NOT NULL,config_key TEXT NOT NULL,market INTEGER NOT NULL,kind TEXT NOT NULL,a INTEGER NOT NULL,b INTEGER NOT NULL,lookback INTEGER NOT NULL,anchor INTEGER NOT NULL,requested INTEGER NOT NULL,status TEXT NOT NULL,error TEXT,checkpoint BLOB NOT NULL,manifest TEXT NOT NULL,active INTEGER NOT NULL DEFAULT 1,supersedes INTEGER);
CREATE UNIQUE INDEX IF NOT EXISTS current_configuration ON series(config_key) WHERE active=1;
CREATE TABLE IF NOT EXISTS archives(market INTEGER NOT NULL,day INTEGER NOT NULL,revision INTEGER,status TEXT NOT NULL DEFAULT 'pending',attempted INTEGER NOT NULL DEFAULT 0,error TEXT,checksum TEXT,rows INTEGER,PRIMARY KEY(market,day));
CREATE TABLE IF NOT EXISTS gaps(market INTEGER NOT NULL,start INTEGER NOT NULL,end INTEGER NOT NULL,reason TEXT NOT NULL,PRIMARY KEY(market,start,end,reason));
CREATE TABLE IF NOT EXISTS source_health(market INTEGER PRIMARY KEY,payload TEXT NOT NULL,observed INTEGER NOT NULL);
";
pub type Manifest = BTreeMap<i32, u64>;
fn has_revised_history(series: &Series, latest: &Manifest) -> bool {
    // Expiring archive metadata is not a source revision. The checkpoint
    // already incorporates that history; rebuilding it changes the anchor and
    // needlessly replays a month of executions. Pending repairs likewise keep
    // the pinned generation until a new verified revision is available.
    latest
        .range(day(series.anchor)..)
        .any(|(partition, revision)| series.manifest.get(partition) != Some(revision))
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Kind {
    Tick,
    Trev,
}
impl Kind {
    fn name(self) -> &'static str {
        if self == Self::Tick { "tick" } else { "trev" }
    }
}
#[derive(Clone, Debug)]
pub struct Request {
    pub exchange: Exchange,
    pub kind: Kind,
    pub a: u64,
    pub b: u64,
    pub lookback: u32,
}
impl Request {
    pub fn validate(&self) -> Result<()> {
        ensure!((1..=30).contains(&self.lookback), "INVALID_LOOKBACK_DAYS");
        ensure!(
            (1..=4000).contains(&self.a)
                && (self.kind == Kind::Tick || (1..=4000).contains(&self.b)),
            "INVALID_CHART_PARAMETERS"
        );
        Ok(())
    }
    fn key(&self) -> String {
        format!(
            "{}:{}:{}:{}:{}",
            self.exchange.id(),
            self.kind.name(),
            self.a,
            self.b,
            self.lookback
        )
    }
}
#[derive(Clone, Debug)]
pub struct Series {
    pub id: u64,
    pub key: String,
    pub config_key: String,
    pub exchange: Exchange,
    pub kind: Kind,
    pub a: u64,
    pub b: u64,
    pub lookback: u32,
    pub anchor: i64,
    pub requested: i64,
    pub status: String,
    pub error: Option<String>,
    pub checkpoint: Checkpoint,
    pub manifest: Manifest,
    pub active: bool,
    pub supersedes: Option<u64>,
}
fn row(r: &rusqlite::Row<'_>) -> rusqlite::Result<Series> {
    let checkpoint: Vec<u8> = r.get(12)?;
    let manifest: String = r.get(13)?;
    let decode = |e: anyhow::Error| {
        rusqlite::Error::FromSqlConversionFailure(12, rusqlite::types::Type::Blob, e.into())
    };
    Ok(Series {
        id: r.get(0)?,
        key: r.get(1)?,
        config_key: r.get(2)?,
        exchange: Exchange::from_id(r.get(3)?).map_err(decode)?,
        kind: if r.get::<_, String>(4)? == "tick" {
            Kind::Tick
        } else {
            Kind::Trev
        },
        a: r.get(5)?,
        b: r.get(6)?,
        lookback: r.get(7)?,
        anchor: r.get(8)?,
        requested: r.get(9)?,
        status: r.get(10)?,
        error: r.get(11)?,
        checkpoint: postcard::from_bytes(&checkpoint).map_err(|e| decode(e.into()))?,
        manifest: serde_json::from_str(&manifest).map_err(|e| decode(e.into()))?,
        active: r.get(14)?,
        supersedes: r.get(15)?,
    })
}
#[derive(Clone)]
pub struct Service {
    pub db: Db,
    pub store: Store,
    pub config: Config,
    pub health: Health,
    pub updates: broadcast::Sender<Arc<Value>>,
    pub wake: Arc<Notify>,
    live_cache: Cache,
    cold_cache: Cache,
}
impl Service {
    pub async fn open(config: Config, health: Health) -> Result<Self> {
        let db = Db::open(&config.directory.join("control.sqlite3"))?;
        db.call(|c| {
            c.execute_batch(SCHEMA)?;
            ensure!(
                c.query_row("SELECT version FROM control_version", [], |r| r
                    .get::<_, i64>(0))?
                    == 1,
                "unsupported control schema"
            );
            Ok(())
        })
        .await?;
        let store = Store::new(&config)?;
        let (updates, _) = broadcast::channel(256);
        Ok(Self {
            live_cache: Cache::new(&config, store.clone()),
            cold_cache: Cache::new(&config, store.clone()),
            db,
            store,
            config,
            health,
            updates,
            wake: Arc::new(Notify::new()),
        })
    }
    pub async fn request(&self, request: Request) -> Result<Series> {
        request.validate()?;
        let key = request.key();
        let mut manifest = self.manifest(request.exchange).await?;
        let result=self.db.call(move|c|{
            let tx=c.transaction()?;
            let existing=tx.query_row("SELECT * FROM series WHERE config_key=? AND active=1",[&key],row).optional()?;
            let count:u64=tx.query_row("SELECT count(*) FROM series WHERE market=? AND active=1 AND requested>?",params![request.exchange.id(),now_us()-1_800_000_000],|r|r.get(0))?;
            ensure!(existing.as_ref().is_some_and(|s|s.requested>now_us()-1_800_000_000) || count<MAX_SERIES as u64,"SERIES_CAPACITY_REACHED");
            if let Some(mut s)=existing {
                // A failed parameter set stays failed until explicitly changed;
                // database and network errors never become permanent failures.
                s.requested=now_us();tx.execute("UPDATE series SET requested=? WHERE id=? OR supersedes=?",params![s.requested,s.id,s.id])?;tx.commit()?;return Ok(s)
            }
            let anchor=(now_us()-DAY_US*i64::from(request.lookback)).div_euclid(3_600_000_000)*3_600_000_000;
            manifest.retain(|partition,_| *partition>=day(anchor));
            let public=uuid::Uuid::new_v4().to_string();let cp=postcard::to_allocvec(&Checkpoint::default())?;
            tx.execute("INSERT INTO series(public_key,config_key,market,kind,a,b,lookback,anchor,requested,status,checkpoint,manifest) VALUES(?,?,?,?,?,?,?,?,?,'queued',?,?)",params![public,key,request.exchange.id(),request.kind.name(),request.a,request.b,request.lookback,anchor,now_us(),cp,serde_json::to_string(&manifest)?])?;
            let s=tx.query_row("SELECT * FROM series WHERE id=last_insert_rowid()",[],row)?;tx.commit()?;Ok(s)
        }).await?;
        self.wake.notify_waiters();
        Ok(result)
    }
    pub(crate) async fn prepare_archive_batch(&self, rows: Vec<Trade>, after: u64) -> Result<()> {
        self.cold_cache.prepare_archive_batch(rows, after).await
    }
    pub async fn warm_recent_archives(&self, days: u32) -> Result<()> {
        ensure!((1..=30).contains(&days), "INVALID_LOOKBACK_DAYS");
        let earliest = day(now_us()) - days as i32;
        for exchange in [Exchange::Binance, Exchange::Bybit] {
            let manifest = self.manifest(exchange).await?;
            for (&partition, &revision) in manifest.iter().rev() {
                if partition < earliest {
                    break;
                }
                let rows = self
                    .cold_cache
                    .warm_archive_day(exchange.id(), partition, revision)
                    .await?;
                tracing::info!(
                    exchange = exchange.name(),
                    partition,
                    rows,
                    "prepared archive ready"
                );
            }
        }
        Ok(())
    }
    pub async fn verify_storage_history(&self) -> Result<()> {
        let allocated = self
            .db
            .call(|c| {
                Ok(c.query_row(
                    "SELECT seq FROM sqlite_sequence WHERE name='series'",
                    [],
                    |r| r.get::<_, u64>(0),
                )
                .optional()?
                .unwrap_or(0))
            })
            .await?;
        let bytes = self
            .store
            .sql(
                &format!(
                    "SELECT max(series_id) FROM {}.bars FORMAT RowBinary",
                    self.config.database
                ),
                vec![],
            )
            .await?;
        let persisted = u64::from_le_bytes(
            bytes
                .as_ref()
                .try_into()
                .context("invalid stored series counter")?,
        );
        ensure!(
            persisted <= allocated,
            "CONTROL_HISTORY_MISMATCH: restore matching control storage or use a fresh database"
        );
        Ok(())
    }
    pub async fn get(&self, key: &str, refresh: bool) -> Result<Option<Series>> {
        let key = key.to_owned();
        self.db
            .call(move |c| {
                let s = c
                    .query_row("SELECT * FROM series WHERE public_key=?", [&key], row)
                    .optional()?;
                if refresh
                    && let Some(s) = &s
                    && s.active
                {
                    let count: u64 = c.query_row(
                        "SELECT count(*) FROM series WHERE market=? AND active=1 AND requested>?",
                        params![s.exchange.id(), now_us() - 1_800_000_000],
                        |r| r.get(0),
                    )?;
                    ensure!(
                        s.requested > now_us() - 1_800_000_000 || count < MAX_SERIES as u64,
                        "SERIES_CAPACITY_REACHED"
                    );
                    c.execute(
                        "UPDATE series SET requested=? WHERE id=? OR supersedes=?",
                        params![now_us(), s.id, s.id],
                    )?;
                }
                Ok(s)
            })
            .await
    }
    pub async fn manifest(&self, exchange: Exchange) -> Result<Manifest> {
        self.db.call(move|c|{
        let mut query=c.prepare("SELECT day,revision FROM archives WHERE market=? AND status='verified' AND revision IS NOT NULL AND day>=?")?;
        Ok(query.query_map(params![exchange.id(),day(now_us())-30],|r|Ok((r.get(0)?,r.get(1)?)))?.collect::<rusqlite::Result<_>>()?)
    }).await
    }
    pub async fn coverage(&self, s: &Series) -> Result<Coverage> {
        let market = s.exchange.id();
        let anchor = s.anchor;
        let pinned = s.manifest.clone();
        let recoverable = s.exchange.recoverable();
        self.db.call(move|c|{
            let mut verified=None;let mut pending=false;let mut contiguous=true;
            if recoverable {
                // Archives before retention cannot be rechecked. Their
                // already-projected state remains pinned by the checkpoint.
                for day in day(anchor).max(day(now_us()) - 30)..day(now_us()) {
                    let revision:Option<u64>=c.query_row("SELECT revision FROM archives WHERE market=? AND day=? AND status='verified'",params![market,day],|r|r.get(0)).optional()?.flatten();
                    if revision.is_some() && pinned.get(&day).copied()==revision {if contiguous{verified=Some((i64::from(day)+1)*DAY_US/1000);}}
                    else {pending=true;contiguous=false;}
                }
            }
            let gap:bool=c.query_row("SELECT EXISTS(SELECT 1 FROM gaps WHERE market=? AND end>=? AND end>?)",params![market,anchor,verified.unwrap_or(0)*1000],|r|r.get(0))?;
            let observed:Option<(String,i64)>=c.query_row("SELECT payload,observed FROM source_health WHERE market=?",[market],|r|Ok((r.get(0)?,r.get(1)?))).optional()?;
            let connected=observed.and_then(|(value,at)|serde_json::from_str::<crate::health::Status>(&value).ok().map(|h|at>now_us()-15_000_000 && h.ready && h.writer && h.journal.pending_rows<10_000)).unwrap_or(false);
            Ok(Coverage {status:if recoverable && (pending || gap || !connected){CoverageStatus::RepairPending}else if gap || !connected{CoverageStatus::GapRecorded}else{CoverageStatus::Live},verified_through_ms:verified})
        }).await
    }
    pub async fn response(&self, s: &Series) -> Result<Value> {
        Ok(
            json!({"seriesKey":s.key,"status":s.status,"errorCode":s.error,"coverage":self.coverage(s).await?}),
        )
    }
    pub async fn snapshot(
        &self,
        s: &Series,
        before: Option<u64>,
        limit: usize,
        lookback: u32,
    ) -> Result<Value> {
        ensure!(s.active && s.status == "ready", "CHART_SERIES_NOT_READY");
        let next = s.checkpoint.state.next_bar_index;
        let bound = before.unwrap_or(next).min(next);
        let active = if before.is_none() {
            ChartBar::active(&s.checkpoint)?
        } else {
            None
        };
        let cutoff = if s.kind == Kind::Trev {
            now_us() - i64::from(lookback) * DAY_US
        } else {
            s.anchor
        };
        let start = self
            .store
            .history_start(s.id, next, cutoff)
            .await?
            .unwrap_or(next);
        let mut budget = 40_000 - active.as_ref().map(|b| b.levels.len()).unwrap_or(0);
        let mut selected = Vec::new();
        let mut cursor = bound;
        // Replayed CH blocks may repeat before merges. Page by the last unique
        // index until the requested count is satisfied, never skip a level cap.
        while selected.len() < limit && cursor > start {
            let page = self
                .store
                .bars(s.id, cursor, limit - selected.len())
                .await?;
            if page.is_empty() {
                break;
            }
            let mut stop = false;
            for bar in page {
                cursor = bar.value.bar_index;
                if cursor < start {
                    stop = true;
                    break;
                }
                if bar.value.levels.len() > budget {
                    stop = true;
                    break;
                }
                budget -= bar.value.levels.len();
                selected.push(bar.value);
            }
            if stop {
                break;
            }
        }
        selected.reverse();
        let next_before = selected
            .first()
            .map(|b| b.bar_index)
            .filter(|index| *index > start);
        Ok(
            json!({"seriesKey":s.key,"bars":selected,"activeBar":active,"hasMore":next_before.is_some(),"nextBeforeBarIndex":next_before,"historyStartBarIndex":start,"coverage":self.coverage(s).await?}),
        )
    }
    async fn work(&self, live: bool) -> Result<Vec<Series>> {
        self.db.call(move|c|{
        let query=if live {"SELECT * FROM series WHERE status='ready' AND active=1 AND requested>? ORDER BY id"}else{"SELECT * FROM series WHERE status IN ('queued','building') AND requested>? ORDER BY requested DESC,id"};
        let mut q=c.prepare(query)?;Ok(q.query_map([now_us()-1_800_000_000],row)?.collect::<rusqlite::Result<_>>()?)
    }).await
    }
    pub async fn worker(&self, live: bool) -> Result<()> {
        loop {
            if self.health.stop.is_cancelled() {
                return Ok(());
            }
            let work = self.work(live).await?;
            if live {
                self.live_cache.begin_pass().await;
            } else {
                self.cold_cache.begin_pass().await;
            }
            let mut advanced = false;
            for s in work {
                if self.health.stop.is_cancelled() {
                    return Ok(());
                }
                match self.step(s.clone(), live).await {
                    Ok(progress) => advanced |= progress,
                    Err(error) => {
                        let message = error.to_string();
                        if error.downcast_ref::<crate::Error>().is_some()
                            || message.starts_with("CHART_")
                        {
                            let id = s.id;
                            let code = message.clone();
                            self.db
                                .call(move |c| {
                                    c.execute(
                                        "UPDATE series SET status='failed',error=? WHERE id=?",
                                        params![code, id],
                                    )?;
                                    Ok(())
                                })
                                .await?;
                            self.publish(json!({"type":"error","seriesKey":s.key,"code":message}));
                        } else {
                            tracing::warn!(series=s.key,%error,"chart projection delayed; checkpoint retained");
                            tokio::time::sleep(Duration::from_secs(1)).await;
                        }
                    }
                }
            }
            if !advanced {
                tokio::select! {_=self.health.stop.cancelled()=>return Ok(()),_=self.wake.notified()=>{},_=tokio::time::sleep(Duration::from_millis(if live{100}else{500}))=>{}}
            }
        }
    }
    async fn step(&self, mut series: Series, live: bool) -> Result<bool> {
        if series
            .checkpoint
            .cursor
            .is_some_and(|cursor| cursor.day < day(now_us()) - 30)
        {
            // A checkpoint whose unread source has expired cannot be extended
            // across that hole. Keep its chart available while rebasing.
            self.replace_or_restart(&series, self.manifest(series.exchange).await?)
                .await?;
            return Ok(false);
        }
        let start = series
            .checkpoint
            .cursor
            .map(|c| c.day)
            .unwrap_or(day(series.anchor));
        // The checkpoint already identifies the current partition. Discover
        // later days only after reaching its end, not for every 100k trades.
        let mut days = vec![start];
        let mut discovered = false;
        let cache = if live {
            &self.live_cache
        } else {
            &self.cold_cache
        };
        let mut day_index = 0;
        while let Some(&partition) = days.get(day_index) {
            day_index += 1;
            let revision = series.manifest.get(&partition).copied().unwrap_or(0);
            let before = series.checkpoint.state.next_bar_index;
            let mut bars = Vec::new();
            let (mut batches, mut levels) = (0, 0);
            let (mut read_ms, mut project_ms) = (0., 0.);
            let slice = Instant::now();
            loop {
                let after = series
                    .checkpoint
                    .cursor
                    .filter(|c| c.day == partition && c.revision == revision)
                    .map(|c| c.ordinal)
                    .unwrap_or(0);
                let at = Instant::now();
                let batch = cache
                    .read(
                        series.exchange.id(),
                        partition,
                        revision,
                        after,
                        series.kind == Kind::Trev,
                        if series.checkpoint.state.last_order.is_some() {
                            0
                        } else {
                            series.anchor
                        },
                    )
                    .await?;
                read_ms += at.elapsed().as_secs_f64() * 1000.;
                if batch.rows.is_empty() {
                    break;
                }
                let input = series.clone();
                let at = Instant::now();
                let (checkpoint, output) =
                    tokio::task::spawn_blocking(move || project(&input, &batch)).await??;
                project_ms += at.elapsed().as_secs_f64() * 1000.;
                ensure!(
                    checkpoint.cursor != series.checkpoint.cursor,
                    "CHART_INVALID_PREPARED_TAPE"
                );
                series.checkpoint = checkpoint;
                levels += output.iter().map(|b| b.value.levels.len()).sum::<usize>();
                bars.extend(output);
                batches += 1;
                // Retain only one input chunk at a time. Bound accumulated
                // output and time; live projection still commits every chunk.
                if live
                    || batches >= HISTORY_BATCHES
                    || bars.len() >= 2_000
                    || levels >= PROJECT_LEVELS
                    || slice.elapsed() >= HISTORY_SLICE
                    || self.health.stop.is_cancelled()
                {
                    break;
                }
            }
            if batches == 0 {
                if !discovered {
                    let mut later = self
                        .store
                        .days(series.exchange.id(), start.saturating_add(1))
                        .await?;
                    later.extend(series.manifest.keys().copied().filter(|d| *d > start));
                    later.sort_unstable();
                    later.dedup();
                    days.extend(later);
                    discovered = true;
                }
                continue;
            }
            // Durable bars precede the FULL SQLite checkpoint. On a crash this
            // input is replayed; readers hide uncheckpointed rows. Bar values
            // remain identical even when a time budget changes insert grouping.
            let at = Instant::now();
            self.store.insert_bars(&bars).await?;
            let insert_ms = at.elapsed().as_secs_f64() * 1000.;
            let at = Instant::now();
            let data = postcard::to_allocvec(&series.checkpoint)?;
            let id = series.id;
            self.db.call(move|c|{c.execute("UPDATE series SET checkpoint=?,status=CASE WHEN status='queued' THEN 'building' ELSE status END WHERE id=?",params![data,id])?;Ok(())}).await?;
            let checkpoint_ms = at.elapsed().as_secs_f64() * 1000.;
            if live {
                let levels = bars.iter().map(|b| b.value.levels.len()).sum::<usize>()
                    + ChartBar::active(&series.checkpoint)?
                        .as_ref()
                        .map(|b| b.levels.len())
                        .unwrap_or(0);
                if bars.len() > 50 || levels > 8000 {
                    self.publish(json!({"type":"error","seriesKey":series.key,"code":"CHART_RESYNC_REQUIRED"}));
                } else {
                    self.publish(json!({"type":"update","seriesKey":series.key,"bars":bars.iter().map(|b|&b.value).collect::<Vec<_>>(),"activeBar":ChartBar::active(&series.checkpoint)?,"coverage":self.coverage(&series).await?}));
                }
                self.health.update(|h| {
                    h.chart_lag_ms = series
                        .checkpoint
                        .state
                        .last_order
                        .map(|o| (now_us() - o[0] as i64).max(0) / 1000)
                        .unwrap_or(0)
                });
            }
            tracing::debug!(
                series = series.key,
                bars = series.checkpoint.state.next_bar_index - before,
                batches,
                read_ms,
                project_ms,
                insert_ms,
                checkpoint_ms,
                "checkpoint committed"
            );
            return Ok(true);
        }
        if series.status != "ready" {
            // Refuse publication if recovery advanced again during the rebuild.
            let mut latest = self.manifest(series.exchange).await?;
            latest.retain(|partition, _| *partition >= day(series.anchor));
            if has_revised_history(&series, &latest) {
                self.replace_or_restart(&series, latest).await?;
                return Ok(true);
            }
            let id = series.id;
            let previous = series.supersedes;
            let retired=self.db.call(move|c|{
                let tx=c.transaction()?;
                let mut key=None;
                if let Some(old)=previous {
                    key=tx.query_row("SELECT public_key FROM series WHERE id=?",[old],|r|r.get::<_,String>(0)).optional()?;
                    tx.execute("UPDATE series SET active=0,status='replaced',error='CHART_SERIES_REPLACED' WHERE id=?",[old])?;
                }
                tx.execute("UPDATE series SET active=1,status='ready',supersedes=NULL WHERE id=?",[id])?;tx.commit()?;Ok(key)
            }).await?;
            if let Some(key) = retired {
                self.publish(
                    json!({"type":"error","seriesKey":key,"code":"CHART_SERIES_REPLACED"}),
                );
            }
            return Ok(true);
        }
        Ok(false)
    }
    pub fn publish(&self, value: Value) {
        let _ = self.updates.send(Arc::new(value));
    }
    async fn replace_or_restart(&self, s: &Series, mut manifest: Manifest) -> Result<()> {
        let s = s.clone();
        let anchor = s.anchor.max(
            (now_us() - i64::from(s.lookback) * DAY_US).div_euclid(3_600_000_000) * 3_600_000_000,
        );
        manifest.retain(|partition, _| *partition >= day(anchor));
        self.db.call(move|c|{
            let tx=c.transaction()?;let checkpoint=postcard::to_allocvec(&Checkpoint::default())?;
            if s.status=="ready" && tx.query_row("SELECT EXISTS(SELECT 1 FROM series WHERE supersedes=? AND status IN ('queued','building'))",[s.id],|r|r.get::<_,bool>(0))? {
                return Ok(());
            }
            // Never reuse IDs when rows already exist in ClickHouse.
            if s.supersedes.is_some() || s.status!="ready" {tx.execute("UPDATE series SET active=0,status='replaced',error='CHART_SERIES_REPLACED' WHERE id=?",[s.id])?;}
            let parent=if s.status=="ready" {Some(s.id)}else{s.supersedes};
            let active=parent.is_none();
            tx.execute("INSERT INTO series(public_key,config_key,market,kind,a,b,lookback,anchor,requested,status,checkpoint,manifest,active,supersedes) VALUES(?,?,?,?,?,?,?,?,?,'queued',?,?,?,?)",params![uuid::Uuid::new_v4().to_string(),s.config_key,s.exchange.id(),s.kind.name(),s.a,s.b,s.lookback,anchor,s.requested,checkpoint,serde_json::to_string(&manifest)?,active,parent])?;tx.commit()?;Ok(())
        }).await?;
        self.wake.notify_waiters();
        Ok(())
    }
    pub async fn schedule_replacements(&self, exchange: Exchange) -> Result<()> {
        let manifest = self.manifest(exchange).await?;
        // Do not recreate expired replacement jobs after maintenance removes
        // them. Otherwise each job protects its old parent from deletion and
        // the pair is recreated indefinitely, even with no chart subscribers.
        let candidates=self.db.call(move|c|{let mut q=c.prepare("SELECT * FROM series WHERE market=? AND active=1 AND status='ready' AND requested>? AND NOT EXISTS(SELECT 1 FROM series AS replacement WHERE replacement.supersedes=series.id AND replacement.status IN ('queued','building'))")?;Ok(q.query_map(params![exchange.id(),now_us()-DAY_US],row)?.collect::<rusqlite::Result<Vec<_>>>()?)}).await?;
        for s in candidates {
            let relevant: Manifest = manifest
                .range(day(s.anchor)..)
                .map(|(day, revision)| (*day, *revision))
                .collect();
            if has_revised_history(&s, &relevant) {
                self.replace_or_restart(&s, relevant).await?;
            }
        }
        Ok(())
    }
    pub async fn monitor(&self) -> Result<()> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(2))
            .build()?;
        let mut maintenance = tokio::time::Instant::now();
        loop {
            self.health.beat();
            if self.health.stop.is_cancelled() {
                return Ok(());
            }
            for (exchange, url) in Exchange::ALL.into_iter().zip(&self.config.collector_urls) {
                let response = client.get(format!("{url}/health/status")).send().await;
                if let Ok(response) = response
                    && let Ok(status) = response.json::<crate::health::Status>().await
                {
                    let data = serde_json::to_string(&status)?;
                    self.db.call(move|c|{let tx=c.transaction()?;
                        tx.execute("INSERT INTO source_health VALUES(?,?,?) ON CONFLICT(market) DO UPDATE SET payload=excluded.payload,observed=excluded.observed",params![exchange.id(),data,now_us()])?;
                        for gap in status.gaps {tx.execute("INSERT OR IGNORE INTO gaps VALUES(?,?,?,?)",params![exchange.id(),gap.start,gap.end,gap.reason])?;}
                        tx.commit()?;Ok(())
                    }).await?;
                }
            }
            let ready = self.store.ping().await.is_ok();
            self.health.update(|h| {
                h.ready = ready;
                h.writer = true;
            });
            for series in self.work(true).await? {
                self.publish(json!({"type":"coverage","seriesKey":series.key,"coverage":self.coverage(&series).await?}));
            }
            if maintenance.elapsed() > Duration::from_secs(300) {
                if let Err(error) = self.cold_cache.prune().await {
                    tracing::warn!(%error,"prepared cache cleanup delayed");
                }
                self.db.call(|c|{c.execute("DELETE FROM gaps WHERE end<?",[now_us()-31*DAY_US])?;c.execute("DELETE FROM archives WHERE day<?",[day(now_us())-31])?;c.execute("DELETE FROM series WHERE requested<? AND id NOT IN (SELECT supersedes FROM series WHERE supersedes IS NOT NULL)",[now_us()-DAY_US])?;Ok(())}).await?;
                for exchange in Exchange::ALL {
                    self.schedule_replacements(exchange).await?;
                }
                maintenance = tokio::time::Instant::now();
            }
            tokio::select! {_=self.health.stop.cancelled()=>return Ok(()),_=tokio::time::sleep(Duration::from_secs(3))=>{}}
        }
    }
}

/// Pure deterministic projection. The input checkpoint is never mutated on an
/// error. Limit output per step, including a single maximum expansion of 500.
pub fn project(series: &Series, batch: &Batch) -> Result<(Checkpoint, Vec<StoredBar>)> {
    ensure!(
        series.kind == Kind::Trev || !batch.compacted,
        "CHART_INVALID_PREPARED_TAPE"
    );
    let mut engine = Engine::new(series.a as i64, series.b.max(1) as i64, 500, MAX_LEVELS)?;
    engine.tick_count = (series.kind == Kind::Tick).then_some(series.a);
    engine.state = series.checkpoint.state.clone();
    let mut checkpoint = series.checkpoint.clone();
    let mut bars = Vec::new();
    let mut output = Vec::new();
    let mut index = 0;
    if let Some(cursor) = series.checkpoint.cursor {
        index = batch.rows.partition_point(|row| {
            row.day == cursor.day
                && row.revision == cursor.revision
                && row.ordinal <= cursor.ordinal
        });
    }
    let mut run_index = batch.runs.partition_point(|run| run.end <= index);
    let mut levels = 0;
    while index < batch.rows.len() {
        let trade = &batch.rows[index];
        if trade.raw.timestamp < series.anchor && engine.state.last_order.is_none() {
            checkpoint.cursor = Some(trade.cursor());
            index += 1;
            continue;
        }
        let emitted_before = bars.len();
        if checkpoint.spec.is_some()
            && (checkpoint.segment != trade.segment || checkpoint.spec != Some(trade.spec))
        {
            engine.truncate(&mut output)?;
            drain_bars(
                series.id,
                &mut output,
                checkpoint.spec.context("missing prior spec")?,
                &mut bars,
            )?;
        }
        checkpoint.segment = trade.segment;
        checkpoint.spec = Some(trade.spec);
        let (price, quantity) = trade.units()?;
        while run_index < batch.runs.len() && batch.runs[run_index].end <= index {
            run_index += 1;
        }
        let run = batch.runs.get(run_index);
        let mut cursor = None;
        if series.kind == Kind::Trev && run.is_some_and(|r| r.start == index) {
            let run = run.unwrap();
            engine.push_run(
                trade.raw.timestamp as u64,
                price,
                quantity,
                trade.raw.buy,
                trade.spec.id(),
                &mut output,
                &run.tail(),
            )?;
            index = run.end;
            cursor = Some(run.cursor);
        } else {
            engine.push(
                trade.raw.timestamp as u64,
                price,
                quantity,
                trade.raw.buy,
                trade.spec.id(),
                &mut output,
            )?;
            index += 1;
        }
        drain_bars(series.id, &mut output, trade.spec, &mut bars)?;
        levels += bars[emitted_before..]
            .iter()
            .map(|b| b.value.levels.len())
            .sum::<usize>();
        checkpoint.cursor = cursor.or_else(|| Some(batch.rows[index - 1].cursor()));
        if bars.len() >= 2_000 || levels >= PROJECT_LEVELS {
            break;
        }
    }
    checkpoint.state = engine.state;
    ChartBar::active(&checkpoint)?;
    Ok((checkpoint, bars))
}
fn drain_bars(
    series: u64,
    output: &mut Vec<crate::engine::Bar>,
    spec: Spec,
    bars: &mut Vec<StoredBar>,
) -> Result<()> {
    for bar in output.drain(..) {
        bars.push(StoredBar {
            series,
            ended: bar.ended as i64,
            value: ChartBar::closed(&bar, spec)?,
        })
    }
    Ok(())
}

#[cfg(test)]
mod history_tests {
    use super::*;
    use tokio_util::sync::CancellationToken;

    #[tokio::test]
    async fn trev_lookback_bounds_the_build_and_identifies_a_separate_series() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let mut config = Config::load()?;
        config.directory = directory.path().into();
        let service = Service::open(config, Health::new("test", CancellationToken::new())).await?;
        let request = |lookback| Request {
            exchange: Exchange::Binance,
            kind: Kind::Trev,
            a: 314,
            b: 471,
            lookback,
        };
        let five = service.request(request(5)).await?;
        let thirty = service.request(request(30)).await?;
        assert_ne!(five.key, thirty.key);
        assert_eq!(five.key, service.request(request(5)).await?.key);
        assert!(five.anchor - thirty.anchor >= 25 * DAY_US - 3_600_000_000);
        assert!(five.anchor - thirty.anchor <= 25 * DAY_US + 3_600_000_000);
        Ok(())
    }

    #[tokio::test]
    async fn expired_archive_does_not_replace_ready_chart_or_recreate_idle_job() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let mut config = Config::load()?;
        config.directory = directory.path().into();
        config.collector_urls.clear();
        let service = Service::open(config, Health::new("test", CancellationToken::new())).await?;
        let old_day = day(now_us()) - 30;
        service
            .db
            .call(move |c| {
                c.execute(
                    "INSERT INTO archives(market,day,revision,status) VALUES(1,?,7,'verified')",
                    [old_day],
                )?;
                Ok(())
            })
            .await?;
        let original = service
            .request(Request {
                exchange: Exchange::Binance,
                kind: Kind::Trev,
                a: 314,
                b: 471,
                lookback: 30,
            })
            .await?;
        let id = original.id;
        assert_eq!(original.manifest.get(&old_day), Some(&7));
        service
            .db
            .call(move |c| {
                c.execute("UPDATE series SET status='ready' WHERE id=?", [id])?;
                c.execute("DELETE FROM archives WHERE market=1 AND day=?", [old_day])?;
                Ok(())
            })
            .await?;
        service.schedule_replacements(Exchange::Binance).await?;
        assert_eq!(
            service
                .db
                .call(|c| Ok(
                    c.query_row("SELECT count(*) FROM series", [], |r| r.get::<_, u64>(0))?
                ))
                .await?,
            1
        );

        let new_day = day(now_us()) - 1;
        service
            .db
            .call(move |c| {
                c.execute(
                    "INSERT INTO archives(market,day,revision,status) VALUES(1,?,8,'verified')",
                    [new_day],
                )?;
                Ok(())
            })
            .await?;
        service.schedule_replacements(Exchange::Binance).await?;
        assert_eq!(
            service
                .db
                .call(move |c| Ok(c.query_row(
                    "SELECT count(*) FROM series WHERE supersedes=?",
                    [id],
                    |r| r.get::<_, u64>(0)
                )?))
                .await?,
            1
        );

        service.db.call(move |c| {
            c.execute("UPDATE series SET requested=?", [now_us() - DAY_US - 1])?;
            c.execute("DELETE FROM series WHERE requested<? AND id NOT IN (SELECT supersedes FROM series WHERE supersedes IS NOT NULL)", [now_us()-DAY_US])?;
            Ok(())
        }).await?;
        service.schedule_replacements(Exchange::Binance).await?;
        assert_eq!(
            service
                .db
                .call(|c| Ok(
                    c.query_row("SELECT count(*) FROM series", [], |r| r.get::<_, u64>(0))?
                ))
                .await?,
            1
        );
        Ok(())
    }
}
