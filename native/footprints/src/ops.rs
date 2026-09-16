//! Offline engine measurements and durable operational observations, in Rust.
use crate::{
    config::{Config, Exchange, day, now_us},
    engine::Engine,
    health::Status,
    model::{Checkpoint, RawTrade, Spec, Trade},
    prepared::Batch,
    service::{Kind, Series, project},
};
use anyhow::{Context, Result, ensure};
use serde_json::json;
use std::{
    collections::BTreeMap,
    io::Write,
    path::Path,
    time::{Duration, Instant},
};

/// This deterministic tape isolates preparation and projection. It deliberately
/// reports the scalar Rust core separately; it is not a Python-to-Rust claim.
pub fn benchmark(count: usize) -> Result<serde_json::Value> {
    ensure!(
        (1_000..=10_000_000).contains(&count),
        "benchmark rows must be between 1000 and 10000000"
    );
    let spec = Spec::initial(Exchange::Binance);
    let start = now_us();
    let trades: Vec<_> = (0..count)
        .map(|i| Trade {
            market: 1,
            day: day(start),
            revision: 0,
            ordinal: i as u64 + 1,
            segment: 1,
            spec,
            raw: RawTrade {
                id: i.to_string(),
                sequence: i as u64,
                timestamp: start + i as i64,
                price: (600_000 + (i / 32 % 1000) as i64) * spec.tick,
                quantity: (1 + (i % 17) as u64) * spec.step,
                buy: i % 2 == 0,
            },
        })
        .collect();
    let prepared_at = Instant::now();
    let mut batch = Batch::new(trades)?;
    let mut prepare_ms = prepared_at.elapsed().as_secs_f64() * 1000.;
    let plain_at = Instant::now();
    let mut plain = Engine::new(314, 471, 500, 4000)?;
    let mut scratch = Vec::new();
    let mut plain_bars = 0;
    for t in &batch.rows {
        let (p, q) = t.units()?;
        plain.push(
            t.raw.timestamp as u64,
            p,
            q,
            t.raw.buy,
            spec.id(),
            &mut scratch,
        )?;
        plain_bars += scratch.len();
        scratch.clear();
    }
    let scalar_ms = plain_at.elapsed().as_secs_f64() * 1000.;
    let compact_at = Instant::now();
    batch.compact();
    prepare_ms += compact_at.elapsed().as_secs_f64() * 1000.;
    let series = Series {
        id: 1,
        key: "benchmark".into(),
        config_key: "benchmark".into(),
        exchange: Exchange::Binance,
        kind: Kind::Trev,
        a: 314,
        b: 471,
        lookback: 30,
        anchor: 0,
        requested: start,
        status: "building".into(),
        error: None,
        checkpoint: Checkpoint::default(),
        manifest: BTreeMap::new(),
        active: true,
        supersedes: None,
    };
    let mut series = series;
    let mut bars = 0;
    let project_at = Instant::now();
    // Production has bounded output steps. Reuse the same prepared tape with a
    // checkpoint-aware starting position instead of rebuilding its suffix.
    while series
        .checkpoint
        .cursor
        .is_none_or(|c| c.ordinal < count as u64)
    {
        let (checkpoint, out) = project(&series, &batch)?;
        bars += out.len();
        series.checkpoint = checkpoint;
    }
    let prepared_ms = project_at.elapsed().as_secs_f64() * 1000.;
    ensure!(
        bars == plain_bars && series.checkpoint.state == plain.state,
        "benchmark parity failed"
    );
    Ok(
        json!({"scope":"deterministic Rust engine tape; excludes network, ClickHouse, and Python orchestration","rows":count,"runs":batch.runs.len(),"closedBars":bars,"prepareMs":prepare_ms,"scalarRustMs":scalar_ms,"preparedRustMs":prepared_ms,"preparedTradesPerSecond":count as f64/(prepared_ms/1000.)}),
    )
}

/// Replay frozen on-disk chunks through the actual production projector.
/// One sample is one pass; samples are never presented as distinct history.
pub fn benchmark_prepared(
    directory: &Path,
    exchange: Exchange,
    samples: usize,
    continuation: u64,
    reversal: u64,
) -> Result<serde_json::Value> {
    ensure!((1..=10).contains(&samples), "samples must be 1..10");
    crate::service::Request {
        exchange,
        kind: Kind::Trev,
        a: continuation,
        b: reversal,
        lookback: 30,
    }
    .validate()?;
    let mut paths = Vec::new();
    for entry in std::fs::read_dir(directory)? {
        let path = entry?.path();
        let name = path
            .file_name()
            .context("missing filename")?
            .to_string_lossy();
        let fields: Vec<_> = name.trim_end_matches(".bin").split('_').collect();
        if fields.len() != 6
            || fields[0] != exchange.id().to_string()
            || fields[4] != "true"
            || fields[5] != "0"
            || !name.ends_with(".bin")
        {
            continue;
        }
        paths.push((
            (
                fields[1].parse::<i32>()?,
                fields[2].parse::<u64>()?,
                fields[3].parse::<u64>()?,
            ),
            path,
        ));
    }
    paths.sort_by_key(|(key, _)| *key);
    ensure!(!paths.is_empty(), "no matching prepared chunks");
    let mut observed = Vec::new();
    let mut expected = None;
    for _ in 0..samples {
        let mut series = Series {
            id: 1,
            key: "read-only-benchmark".into(),
            config_key: String::new(),
            exchange,
            kind: Kind::Trev,
            a: continuation,
            b: reversal,
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
        let (mut rows, mut runs, mut bars, mut bytes) = (0usize, 0usize, 0usize, 0u64);
        let (mut load_ms, mut project_ms, mut encode_ms) = (0., 0., 0.);
        let (mut codec_encode_ms, mut codec_decode_ms, mut codec_bytes, mut fixed_chunks) =
            (0., 0., 0, 0);
        let mut hash = sha2::Sha256::new();
        use sha2::Digest;
        let started = Instant::now();
        for ((day, revision, after), path) in &paths {
            if let Some(cursor) = series.checkpoint.cursor {
                ensure!(
                    cursor.day != *day
                        || (cursor.revision == *revision && cursor.ordinal == *after),
                    "overlapping or missing chunks at {}",
                    path.display()
                );
                ensure!(
                    cursor.day == *day || *after == 0,
                    "missing first chunk at {}",
                    path.display()
                );
            } else {
                ensure!(*after == 0, "missing first chunk");
            }
            let at = Instant::now();
            let batch = crate::prepared::load(path)
                .with_context(|| format!("invalid cache: {}", path.display()))?;
            load_ms += at.elapsed().as_secs_f64() * 1000.;
            let at = Instant::now();
            let encoded = crate::prepared::encode(&batch)?;
            codec_encode_ms += at.elapsed().as_secs_f64() * 1000.;
            codec_bytes += encoded.len();
            fixed_chunks += usize::from(&encoded[..8] == b"FPRUN003");
            let at = Instant::now();
            let batch =
                crate::prepared::decode(&encoded).context("cache codec roundtrip failed")?;
            codec_decode_ms += at.elapsed().as_secs_f64() * 1000.;
            bytes += path.metadata()?.len();
            rows += batch.row_count();
            runs += batch.runs.len();
            let last = batch.last_cursor().context("empty prepared chunk")?;
            while series.checkpoint.cursor != Some(last) {
                let at = Instant::now();
                let (checkpoint, output) = project(&series, &batch)?;
                project_ms += at.elapsed().as_secs_f64() * 1000.;
                ensure!(
                    checkpoint.cursor != series.checkpoint.cursor,
                    "projection did not advance"
                );
                series.checkpoint = checkpoint;
                bars += output.len();
                // Hash bars individually so the digest is independent of batching.
                for bar in &output {
                    hash.update(postcard::to_allocvec(bar)?);
                }
                let at = Instant::now();
                let encoded = crate::storage::encode_bars(&output)?;
                std::hint::black_box(&encoded);
                encode_ms += at.elapsed().as_secs_f64() * 1000.;
            }
        }
        hash.update(postcard::to_allocvec(&series.checkpoint)?);
        let digest = hash
            .finalize()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>();
        if let Some(expected) = &expected {
            ensure!(*expected == digest, "replay digest changed");
        }
        expected = Some(digest.clone());
        observed.push(
            json!({"rows":rows,"runs":runs,"closedBars":bars,"cacheBytes":bytes,
            "loadAndDecodeMs":load_ms,"projectMs":project_ms,"barEncodeMs":encode_ms,
            "codecEncodeMs":codec_encode_ms,"codecDecodeFromMemoryMs":codec_decode_ms,
            "encodedCacheBytes":codec_bytes,"fixedWidthChunks":fixed_chunks,
            "elapsedMsIncludingDigest":started.elapsed().as_secs_f64()*1000.,"digest":digest}),
        );
    }
    Ok(
        json!({"scope":"Read-only replay of real prepared files. Existing-file load/checksum/decode and new codec encode/decode measured separately. Codec decode includes checksum but reads freshly encoded memory, excluding disk I/O. Projector consumes the roundtripped cache. No ClickHouse, durable writes, queue, network, or rendering. Samples repeat the same history; OS page cache is not cleared.",
        "exchange":exchange.name(),"chunks":paths.len(),"continuation":continuation,"reversal":reversal,"samples":observed}),
    )
}

pub async fn soak(config: &Config, hours: u32, output: &Path) -> Result<()> {
    ensure!(
        (1..=168).contains(&hours),
        "soak duration must be 1..168 hours"
    );
    let mut file = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(output)
        .context("soak output must be a new file")?;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(4))
        .build()?;
    let mut urls = config.collector_urls.clone();
    urls.push("http://127.0.0.1:8010".into());
    let started = Instant::now();
    let duration = Duration::from_secs(u64::from(hours) * 3600);
    let mut samples = 0u64;
    let mut unavailable = 0u64;
    while started.elapsed() < duration {
        let observations = futures_util::future::join_all(urls.iter().map(|url| {
            let client = &client;
            async move {
                match client.get(format!("{url}/health/ready")).send().await {
                    Ok(response) => {
                        let ok = response.status().is_success();
                        match response.json::<Status>().await {
                            Ok(status) => json!({"url":url,"ready":ok,"status":status}),
                            Err(error) => {
                                json!({"url":url,"ready":false,"error":error.to_string()})
                            }
                        }
                    }
                    Err(error) => json!({"url":url,"ready":false,"error":error.to_string()}),
                }
            }
        }))
        .await;
        samples += 1;
        if observations.iter().any(|v| v["ready"] != true) {
            unavailable += 1;
        }
        writeln!(
            file,
            "{}",
            json!({"atUs":now_us(),"observations":observations})
        )?;
        file.sync_data()?;
        tokio::select! {_=tokio::signal::ctrl_c()=>break,_=tokio::time::sleep(Duration::from_secs(5))=>{}}
    }
    let completed = started.elapsed() >= duration;
    let summary = json!({"completed":completed,"elapsedSeconds":started.elapsed().as_secs(),"samples":samples,"samplesWithUnavailableService":unavailable,"allServicesReadyPercent":100.*(samples-unavailable) as f64/samples.max(1) as f64});
    writeln!(file, "{}", json!({"summary":summary}))?;
    file.sync_all()?;
    println!("{summary}");
    Ok(())
}
