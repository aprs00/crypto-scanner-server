//! Shared immutable chunks and unchanged-price runs. Tick always sees each trade.
use crate::{
    config::{Config, READ_BATCH},
    engine::Tail,
    model::{Cursor, Trade},
    storage::Store,
};
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{collections::VecDeque, io::Write, path::PathBuf, sync::Arc};
use tokio::sync::Mutex;

#[derive(Clone, Serialize, Deserialize)]
pub struct Run {
    pub start: usize,
    pub end: usize,
    pub first: u64,
    pub last: u64,
    pub buy: u128,
    pub sell: u128,
    pub count: u64,
    pub cursor: Cursor,
}
impl Run {
    pub fn tail(&self) -> Tail {
        Tail {
            first: self.first,
            last: self.last,
            count: self.count,
            buy: self.buy,
            sell: self.sell,
        }
    }
}
#[derive(Serialize, Deserialize)]
pub struct Batch {
    pub rows: Vec<Trade>,
    pub runs: Vec<Run>,
    pub compacted: bool,
}
impl Batch {
    pub fn new(rows: Vec<Trade>) -> Result<Self> {
        let mut runs = Vec::new();
        let mut start = 0;
        while start < rows.len() {
            let head = &rows[start];
            let mut end = start + 1;
            let (mut buy, mut sell) = (0u128, 0u128);
            head.units()?;
            while end < rows.len()
                && rows[end].raw.price == head.raw.price
                && rows[end].spec == head.spec
                && rows[end].segment == head.segment
            {
                let quantity = rows[end].units()?.1 as u128;
                if rows[end].raw.buy {
                    buy = buy.checked_add(quantity).ok_or(crate::Error::Overflow)?;
                } else {
                    sell = sell.checked_add(quantity).ok_or(crate::Error::Overflow)?;
                }
                end += 1;
            }
            runs.push(Run {
                start,
                end,
                first: rows[(start + 1).min(end - 1)].raw.timestamp as u64,
                last: rows[end - 1].raw.timestamp as u64,
                buy,
                sell,
                count: (end - start - 1) as u64,
                cursor: rows[end - 1].cursor(),
            });
            start = end;
        }
        Ok(Self {
            rows,
            runs,
            compacted: false,
        })
    }
    pub fn compact(&mut self) {
        if self.compacted {
            return;
        }
        let mut heads = Vec::with_capacity(self.runs.len());
        for run in &mut self.runs {
            heads.push(self.rows[run.start].clone());
            run.start = heads.len() - 1;
            run.end = heads.len();
        }
        self.rows = heads;
        self.compacted = true;
    }
    pub fn row_count(&self) -> usize {
        self.runs.iter().map(|r| r.count as usize + 1).sum()
    }
    pub fn last_cursor(&self) -> Option<Cursor> {
        self.runs.last().map(|r| r.cursor)
    }
}
type Key = (u8, i32, u64, u64, bool, i64);
type Entries = VecDeque<(Key, Arc<Batch>, bool)>;
#[derive(Clone)]
pub struct Cache {
    entries: Arc<Mutex<Entries>>,
    directory: PathBuf,
    store: Store,
}
impl Cache {
    pub async fn begin_pass(&self) {
        self.entries
            .lock()
            .await
            .retain(|(_, _, immutable)| *immutable);
    }
    pub fn new(config: &Config, store: Store) -> Self {
        Self {
            entries: Arc::new(Mutex::new(VecDeque::new())),
            directory: config.directory.join("prepared-v2"),
            store,
        }
    }
    pub async fn read(
        &self,
        market: u8,
        day: i32,
        revision: u64,
        after: u64,
        trev: bool,
        anchor: i64,
    ) -> Result<Arc<Batch>> {
        let cutoff = if trev && crate::config::day(anchor) == day {
            anchor
        } else {
            0
        };
        let key = (market, day, revision, after, trev, cutoff);
        // Lock also coalesces concurrent identical reads; at most two workers use
        // this path, and live projection has its own cache instance.
        let mut cache = self.entries.lock().await;
        if let Some(index) = cache.iter().position(|(k, batch, _)| {
            *k == key
                || (k.0 == market
                    && k.1 == day
                    && k.2 == revision
                    && k.4 == trev
                    && k.5 == cutoff
                    && batch
                        .rows
                        .first()
                        .is_some_and(|r| r.ordinal <= after.saturating_add(1))
                    && batch.last_cursor().is_some_and(|r| r.ordinal > after))
        }) {
            let item = cache.remove(index).unwrap();
            let batch = item.1.clone();
            cache.push_back(item);
            return Ok(batch);
        }
        let path = self.directory.join(format!(
            "{market}_{day}_{revision}_{after}_{trev}_{cutoff}.bin"
        ));
        let saved = path.clone();
        let from_disk = if revision != 0 {
            tokio::task::spawn_blocking(move || load(&saved)).await?
        } else {
            None
        };
        let batch = if let Some(batch) = from_disk {
            batch
        } else {
            let rows = self
                .store
                .read(market, day, revision, after, READ_BATCH)
                .await?;
            let batch = tokio::task::spawn_blocking(move || {
                let mut batch = Batch::new(rows)?;
                if trev && batch.rows.iter().all(|r| r.raw.timestamp >= cutoff) {
                    batch.compact();
                }
                Ok::<_, anyhow::Error>(batch)
            })
            .await??;
            if revision != 0 && !batch.rows.is_empty() {
                let bytes = postcard::to_allocvec(&batch)?;
                if let Err(error) = tokio::task::spawn_blocking(move || save(&path, &bytes)).await?
                {
                    tracing::warn!(%error,"prepared cache write skipped");
                }
            }
            batch
        };
        let immutable = revision != 0 || batch.row_count() == READ_BATCH;
        let batch = Arc::new(batch);
        cache.push_back((key, batch.clone(), immutable));
        if cache.iter().filter(|(_, _, immutable)| *immutable).count() > 8
            && let Some(index) = cache.iter().position(|(_, _, immutable)| *immutable)
        {
            cache.remove(index);
        }
        if cache.len() > 64 {
            cache.pop_front();
        }
        Ok(batch)
    }
    pub async fn prune(&self) -> Result<()> {
        let dir = self.directory.clone();
        tokio::task::spawn_blocking(move || -> Result<()> {
            if !dir.exists() {
                return Ok(());
            }
            let mut entries = std::fs::read_dir(&dir)?.collect::<std::io::Result<Vec<_>>>()?;
            entries.sort_by_key(|e| e.metadata().and_then(|m| m.modified()).ok());
            let mut bytes = entries
                .iter()
                .filter_map(|e| e.metadata().ok())
                .map(|m| m.len())
                .sum::<u64>();
            for entry in entries {
                let meta = entry.metadata()?;
                if bytes > 8 * 1024 * 1024 * 1024
                    || meta.modified()?.elapsed().unwrap_or_default().as_secs() > 31 * 86400
                {
                    std::fs::remove_file(entry.path())?;
                    bytes = bytes.saturating_sub(meta.len());
                }
            }
            Ok(())
        })
        .await?
    }
}
fn load(path: &std::path::Path) -> Option<Batch> {
    let file = std::fs::File::open(path).ok()?;
    let data = unsafe { memmap2::Mmap::map(&file).ok()? };
    if data.len() < 40 || &data[..8] != b"FPRUN002" {
        return None;
    }
    if Sha256::digest(&data[40..]).as_slice() != &data[8..40] {
        return None;
    }
    postcard::from_bytes(&data[40..]).ok()
}
fn save(path: &std::path::Path, bytes: &[u8]) -> Result<()> {
    let dir = path.parent().unwrap();
    std::fs::create_dir_all(dir)?;
    // Cache is disposable. Pressure cannot prevent raw capture or projection.
    if fs2::available_space(dir)? < 6 * 1024 * 1024 * 1024 {
        return Ok(());
    }
    let mut temp = tempfile::NamedTempFile::new_in(dir)?;
    temp.write_all(b"FPRUN002")?;
    temp.write_all(&Sha256::digest(bytes))?;
    temp.write_all(bytes)?;
    temp.as_file().sync_all()?;
    ensure!(temp.persist(path).is_ok(), "prepared cache publish failed");
    Ok(())
}
