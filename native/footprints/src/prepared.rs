//! Shared immutable chunks and unchanged-price runs. Tick always sees each trade.
use crate::{
    config::{Config, READ_BATCH},
    engine::Tail,
    model::{Cursor, RawTrade, Spec, Trade},
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
    fn retained_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.rows.capacity() * std::mem::size_of::<Trade>()
            + self.runs.capacity() * std::mem::size_of::<Run>()
            + self.rows.iter().map(|r| r.raw.id.capacity()).sum::<usize>()
    }
}
type Key = (u8, i32, u64, u64, bool, i64);
type Entries = VecDeque<(Key, Arc<Batch>, bool, usize)>;
// Independent live/cold caches retain at most 128 MiB combined. This accounts
// for owned vector/string capacities, not allocator overhead or in-flight work.
const CACHE_BYTES: usize = 64 * 1024 * 1024;

fn trim(entries: &mut Entries, budget: usize) {
    let mut bytes = entries.iter().map(|entry| entry.3).sum::<usize>();
    while bytes > budget || entries.len() > 64 {
        let Some(entry) = entries.pop_front() else {
            break;
        };
        bytes -= entry.3;
    }
    if entries.iter().filter(|entry| entry.2).count() > 8
        && let Some(index) = entries.iter().position(|entry| entry.2)
    {
        entries.remove(index);
    }
}
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
            .retain(|(_, _, immutable, _)| *immutable);
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
        if let Some(index) = cache.iter().position(|(k, batch, _, _)| {
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
            tokio::task::spawn_blocking(move || load_versioned(&saved)).await?
        } else {
            None
        };
        let batch = if let Some((batch, legacy)) = from_disk {
            if legacy && flat_eligible(&batch) {
                // Upgrade one chunk at a time, atomically. Readers holding an
                // older mapping keep its inode; a failed upgrade is a cache miss
                // optimization only and cannot prevent chart projection.
                let bytes = encode(&batch)?;
                if let Err(error) = tokio::task::spawn_blocking(move || save(&path, &bytes)).await?
                {
                    tracing::warn!(%error, "prepared cache upgrade skipped");
                }
            }
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
                let bytes = encode(&batch)?;
                if let Err(error) = tokio::task::spawn_blocking(move || save(&path, &bytes)).await?
                {
                    tracing::warn!(%error,"prepared cache write skipped");
                }
            }
            batch
        };
        let immutable = revision != 0 || batch.row_count() == READ_BATCH;
        let bytes = batch.retained_bytes();
        let batch = Arc::new(batch);
        cache.push_back((key, batch.clone(), immutable, bytes));
        trim(&mut cache, CACHE_BYTES);
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
// Common archive chunks share source partition, segment and specification.
// Fixed-width records avoid millions of temporary trade-ID allocations and
// variable-length integer decodes. Tick and mixed-spec/segment chunks retain
// the general format. Identities have already been validated/deduplicated by
// Store::read; projection uses source ordinals, never identity or venue sequence.
fn flat_eligible(batch: &Batch) -> bool {
    batch.compacted
        && !batch.rows.is_empty()
        && batch.rows.len() == batch.runs.len()
        && batch.rows.len() <= READ_BATCH
        && batch
            .rows
            .iter()
            .zip(&batch.runs)
            .enumerate()
            .all(|(i, (row, run))| {
                let head = &batch.rows[0];
                row.market == head.market
                    && row.day == head.day
                    && row.revision == head.revision
                    && row.segment == head.segment
                    && row.spec == head.spec
                    && run.start == i
                    && run.end == i + 1
                    && run.cursor.day == head.day
                    && run.cursor.revision == head.revision
            })
}
pub(crate) fn encode(batch: &Batch) -> Result<Vec<u8>> {
    let flat = flat_eligible(batch);
    let mut out = Vec::with_capacity(if flat { 85 + batch.rows.len() * 97 } else { 40 });
    out.extend_from_slice(if flat { b"FPRUN003" } else { b"FPRUN002" });
    out.resize(40, 0);
    if flat {
        let head = &batch.rows[0];
        out.extend_from_slice(&(batch.rows.len() as u64).to_le_bytes());
        out.push(head.market);
        out.extend_from_slice(&head.day.to_le_bytes());
        for n in [
            head.revision,
            head.segment,
            head.spec.tick as u64,
            head.spec.step,
        ] {
            out.extend_from_slice(&n.to_le_bytes());
        }
        for (row, run) in batch.rows.iter().zip(&batch.runs) {
            for n in [
                row.ordinal,
                row.raw.timestamp as u64,
                row.raw.price as u64,
                row.raw.quantity,
                run.first,
                run.last,
                run.count,
                run.cursor.ordinal,
            ] {
                out.extend_from_slice(&n.to_le_bytes());
            }
            out.extend_from_slice(&run.buy.to_le_bytes());
            out.extend_from_slice(&run.sell.to_le_bytes());
            out.push(u8::from(row.raw.buy));
        }
    } else {
        out.extend_from_slice(&postcard::to_allocvec(batch)?);
    }
    let digest = Sha256::digest(&out[40..]);
    out[8..40].copy_from_slice(&digest);
    Ok(out)
}
pub(crate) fn decode(data: &[u8]) -> Option<Batch> {
    if data.len() < 40 || Sha256::digest(&data[40..]).as_slice() != &data[8..40] {
        return None;
    }
    match &data[..8] {
        b"FPRUN002" => postcard::from_bytes(&data[40..]).ok(),
        b"FPRUN003" => decode_flat(&data[40..]),
        _ => None,
    }
}
fn decode_flat(data: &[u8]) -> Option<Batch> {
    // Validate lengths before indexing or allocating. A chunk represents at
    // most READ_BATCH source trades even when every price changes.
    if data.len() < 45 {
        return None;
    }
    let n = u64::from_le_bytes(data[..8].try_into().ok()?);
    if n == 0 || n > READ_BATCH as u64 || data.len() != 45 + n as usize * 97 {
        return None;
    }
    let u64_at =
        |bytes: &[u8], at: usize| u64::from_le_bytes(bytes[at..at + 8].try_into().unwrap());
    let market = data[8];
    let day = i32::from_le_bytes(data[9..13].try_into().ok()?);
    let revision = u64_at(data, 13);
    let segment = u64_at(data, 21);
    let spec = Spec {
        tick: u64_at(data, 29) as i64,
        step: u64_at(data, 37),
    };
    let mut rows = Vec::with_capacity(n as usize);
    let mut runs = Vec::with_capacity(n as usize);
    let mut count = 0u64;
    for (i, record) in data[45..].chunks_exact(97).enumerate() {
        let ordinal = u64_at(record, 0);
        let timestamp = u64_at(record, 8) as i64;
        let price = u64_at(record, 16) as i64;
        let quantity = u64_at(record, 24);
        let tail_count = u64_at(record, 48);
        let last = u64_at(record, 56);
        count = count.checked_add(tail_count)?.checked_add(1)?;
        if count > READ_BATCH as u64
            || record[96] > 1
            || last < ordinal
            || timestamp <= 0
            || price <= 0
            || quantity == 0
        {
            return None;
        }
        let row = Trade {
            market,
            day,
            revision,
            ordinal,
            segment,
            spec,
            raw: RawTrade {
                id: String::new(),
                sequence: 0,
                timestamp,
                price,
                quantity,
                buy: record[96] == 1,
            },
        };
        row.units().ok()?;
        rows.push(row);
        runs.push(Run {
            start: i,
            end: i + 1,
            first: u64_at(record, 32),
            last: u64_at(record, 40),
            count: tail_count,
            buy: u128::from_le_bytes(record[64..80].try_into().ok()?),
            sell: u128::from_le_bytes(record[80..96].try_into().ok()?),
            cursor: Cursor {
                day,
                revision,
                ordinal: last,
            },
        });
    }
    Some(Batch {
        rows,
        runs,
        compacted: true,
    })
}
pub(crate) fn load(path: &std::path::Path) -> Option<Batch> {
    load_versioned(path).map(|(batch, _)| batch)
}
fn load_versioned(path: &std::path::Path) -> Option<(Batch, bool)> {
    let file = std::fs::File::open(path).ok()?;
    let data = unsafe { memmap2::Mmap::map(&file).ok()? };
    let batch = decode(&data)?;
    Some((batch, &data[..8] == b"FPRUN002"))
}
fn save(path: &std::path::Path, bytes: &[u8]) -> Result<()> {
    let dir = path.parent().unwrap();
    std::fs::create_dir_all(dir)?;
    // Cache is disposable. Pressure cannot prevent raw capture or projection.
    if fs2::available_space(dir)? < 6 * 1024 * 1024 * 1024 {
        return Ok(());
    }
    let mut temp = tempfile::NamedTempFile::new_in(dir)?;
    temp.write_all(bytes)?;
    temp.as_file().sync_all()?;
    ensure!(temp.persist(path).is_ok(), "prepared cache publish failed");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::Exchange,
        model::Checkpoint,
        service::{Kind, Series, project},
    };
    use std::collections::BTreeMap;

    fn tape() -> Vec<Trade> {
        let spec = Spec::initial(Exchange::Binance);
        (0..72)
            .map(|i| Trade {
                market: 1,
                day: 20701,
                revision: 42,
                ordinal: i + 1,
                segment: 1,
                spec,
                raw: RawTrade {
                    id: format!("trade-{i}"),
                    sequence: i,
                    timestamp: 1_000_000 + (i / 3 * 3 + (2 - i % 3)) as i64,
                    price: (1000 + (i / 3 % 8) as i64) * spec.tick,
                    quantity: (i % 7 + 1) * spec.step,
                    buy: i % 2 == 0,
                },
            })
            .collect()
    }
    fn projected(batch: &Batch, kind: Kind) -> Vec<u8> {
        let mut series = Series {
            id: 1,
            key: String::new(),
            config_key: String::new(),
            exchange: Exchange::Binance,
            kind,
            a: 3,
            b: 4,
            lookback: 30,
            anchor: 0,
            requested: 0,
            status: "building".into(),
            error: None,
            checkpoint: Checkpoint::default(),
            manifest: BTreeMap::new(),
            active: true,
            supersedes: None,
        };
        let mut bars = Vec::new();
        while series.checkpoint.cursor != batch.last_cursor() {
            let (checkpoint, output) = project(&series, batch).unwrap();
            series.checkpoint = checkpoint;
            bars.extend(output);
        }
        postcard::to_allocvec(&(series.checkpoint, bars)).unwrap()
    }
    fn legacy(batch: &Batch) -> Vec<u8> {
        let payload = postcard::to_allocvec(batch).unwrap();
        let mut out = b"FPRUN002".to_vec();
        out.extend_from_slice(&Sha256::digest(&payload));
        out.extend_from_slice(&payload);
        out
    }
    #[test]
    fn fixed_cache_and_legacy_preserve_projection_and_tick_executions() {
        for compact in [false, true] {
            let mut batch = Batch::new(tape()).unwrap();
            if compact {
                batch.compact();
            }
            let kind = if compact { Kind::Trev } else { Kind::Tick };
            let encoded = encode(&batch).unwrap();
            assert_eq!(
                &encoded[..8],
                if compact { b"FPRUN003" } else { b"FPRUN002" }
            );
            for decoded in [decode(&encoded).unwrap(), decode(&legacy(&batch)).unwrap()] {
                assert_eq!(decoded.row_count(), 72);
                assert_eq!(decoded.last_cursor(), batch.last_cursor());
                assert_eq!(projected(&decoded, kind), projected(&batch, kind));
            }
        }
    }
    #[test]
    fn mixed_specs_and_segments_use_lossless_general_cache() {
        for change_spec in [false, true] {
            let mut rows = tape();
            for row in &mut rows[17..] {
                if change_spec {
                    row.spec.tick *= 2;
                    row.raw.price *= 2;
                } else {
                    row.segment += 1;
                }
            }
            let mut batch = Batch::new(rows).unwrap();
            batch.compact();
            let encoded = encode(&batch).unwrap();
            assert_eq!(&encoded[..8], b"FPRUN002");
            assert_eq!(
                projected(&decode(&encoded).unwrap(), Kind::Trev),
                projected(&batch, Kind::Trev)
            );
        }
    }
    #[test]
    fn invalid_fixed_cache_is_rejected_before_allocating() {
        let mut batch = Batch::new(tape()).unwrap();
        batch.compact();
        let valid = encode(&batch).unwrap();
        for end in [0, 7, 39, 40, 84, valid.len() - 1] {
            assert!(decode(&valid[..end]).is_none());
        }
        let mut broken = valid.clone();
        *broken.last_mut().unwrap() ^= 1;
        assert!(decode(&broken).is_none());
        let mut oversized = valid;
        oversized[40..48].copy_from_slice(&u64::MAX.to_le_bytes());
        let hash = Sha256::digest(&oversized[40..]);
        oversized[8..40].copy_from_slice(&hash);
        assert!(decode(&oversized).is_none());
    }
    #[test]
    fn cache_budget_accounts_for_owned_buffers_and_evicts_oldest() {
        let batch = Arc::new(Batch::new(tape()).unwrap());
        let bytes = batch.retained_bytes();
        assert!(
            bytes > std::mem::size_of::<Batch>() + batch.rows.len() * std::mem::size_of::<Trade>()
        );
        let mut entries = Entries::new();
        for after in [0, 100] {
            entries.push_back(((1, 20701, 42, after, false, 0), batch.clone(), true, bytes));
        }
        trim(&mut entries, bytes);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0.3, 100);
        trim(&mut entries, bytes - 1);
        assert!(entries.is_empty());
    }
}
