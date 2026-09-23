//! Run against an isolated ClickHouse: FOOTPRINT_TEST_CLICKHOUSE=http://... cargo test --test integration -- --ignored
use anyhow::{Context, Result, ensure};
use axum::{
    body::{Body, to_bytes},
    http::Request as HttpRequest,
};
use footprints::{
    config::{Config, Exchange, day, now_us},
    health::Health,
    journal::Journal,
    model::{RawTrade, Spec, Trade},
    service::{Kind, Request, Service},
};
use serde_json::{Value, json};
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;

async fn ready(service: &Service, key: &str) -> Result<footprints::service::Series> {
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let s = service.get(key, false).await?.context("missing series")?;
            if s.status == "ready" {
                return Ok(s);
            }
            ensure!(s.status != "failed", "series failed: {:?}", s.error);
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await?
}
#[tokio::test]
#[ignore = "requires a disposable ClickHouse server"]
async fn database_retries_projection_api_and_atomic_replacement() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let config = Config {
        directory: dir.path().into(),
        bind: "127.0.0.1:0".parse()?,
        clickhouse_url: std::env::var("FOOTPRINT_TEST_CLICKHOUSE")?,
        database: format!("footprint_test_{}", uuid::Uuid::new_v4().simple()),
        user: "default".into(),
        password: String::new(),
        journal_bytes: 1_000_000,
        reserve_bytes: 0,
        origins: vec!["http://localhost:5173".into()],
        collector_urls: vec![],
    };
    let stop = CancellationToken::new();
    let service = Service::open(config.clone(), Health::new("test", stop.clone())).await?;
    service.store.migrate().await?;
    let timestamp = (now_us() - 1_000_000).div_euclid(1000) * 1000;
    let partition = day(timestamp);
    let raw = |id: u64| RawTrade {
        id: id.to_string(),
        sequence: id,
        timestamp: timestamp + id as i64 * 1000,
        price: 10_000_000_000_000 + id as i64 * 100_000_000_000,
        quantity: 1_000_000_000,
        buy: id.is_multiple_of(2),
    };
    let journal = Journal::open(&config, Exchange::Binance).await?;
    journal.append(vec![raw(1), raw(3), raw(4), raw(5)]).await?;
    let (ack, rows) = journal.first().await?.unwrap();
    service.store.insert_trades(&rows).await?;
    // Simulate retry after an insert acknowledgement was lost, outside CH's
    // finite deduplication window. Reader must still project each ordinal once.
    service
        .store
        .sql(
            &format!(
                "ALTER TABLE {}.executions MODIFY SETTING non_replicated_deduplication_window=0",
                config.database
            ),
            vec![],
        )
        .await?;
    service.store.insert_trades(&rows).await?;
    journal.ack(ack).await?;
    assert_eq!(service.store.read(1, partition, 0, 0, 100).await?.len(), 4);
    assert_eq!(
        service
            .store
            .first_ordinal_at_or_after(1, partition, 0, timestamp + 3_000)
            .await?,
        Some(2)
    );
    let requested = service
        .request(Request {
            exchange: Exchange::Binance,
            kind: Kind::Tick,
            a: 2,
            b: 0,
            lookback: 1,
        })
        .await?;
    let router = footprints::api::router(service.clone());
    let pending = router
        .clone()
        .oneshot(
            HttpRequest::builder()
                .uri(format!(
                    "/api/v1/footprints/chart-bars?seriesKey={}",
                    requested.key
                ))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(pending.status(), 409);
    let pending: Value = serde_json::from_slice(&to_bytes(pending.into_body(), 4096).await?)?;
    assert_eq!(pending["status"], "queued");
    let live = service.clone();
    let live = tokio::spawn(async move { live.worker(true).await });
    let cold = service.clone();
    let cold = tokio::spawn(async move { cold.worker(false).await });
    let s = ready(&service, &requested.key).await?;
    let snapshot = service.snapshot(&s, None, 500, 1).await?;
    assert_eq!(snapshot["bars"].as_array().unwrap().len(), 2);
    assert_eq!(snapshot["bars"][0]["tradeCount"], 2);
    service.verify_storage_history().await?;
    let missing_control_dir = tempfile::tempdir()?;
    let mut missing_control_config = config.clone();
    missing_control_config.directory = missing_control_dir.path().into();
    let missing_control = Service::open(
        missing_control_config,
        Health::new("test", CancellationToken::new()),
    )
    .await?;
    assert!(missing_control.verify_storage_history().await.is_err());
    // Filling older archives cannot rebuild a Tick chart anchored after them.
    service
        .db
        .call(move |c| {
            c.execute(
                "INSERT INTO archives(market,day,revision,status) VALUES(1,?,66,'verified')",
                [partition - 2],
            )?;
            Ok(())
        })
        .await?;
    service.schedule_replacements(Exchange::Binance).await?;
    assert_eq!(
        service
            .db
            .call(|c| {
                Ok(c.query_row(
                    "SELECT count(*) FROM series WHERE supersedes IS NOT NULL",
                    [],
                    |r| r.get::<_, u64>(0),
                )?)
            })
            .await?,
        0
    );
    // Bars from a crash before checkpoint commit remain invisible.
    let mut uncommitted = service.store.bars(s.id, u64::MAX, 500).await?;
    uncommitted[0].value.bar_index = 99;
    service.store.insert_bars(&uncommitted[..1]).await?;
    assert_eq!(
        service.snapshot(&s, None, 500, 1).await?["bars"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    let mut listener = service.updates.subscribe();
    let revised: Vec<_> = (1..=5)
        .map(|id| Trade {
            market: 1,
            day: partition,
            revision: 77,
            ordinal: id,
            segment: 1,
            spec: Spec::initial(Exchange::Binance),
            raw: raw(id),
        })
        .collect();
    service.store.insert_trades(&revised).await?;
    service
        .db
        .call(move |c| {
            c.execute(
                "INSERT INTO archives(market,day,revision,status) VALUES(1,?,77,'verified')",
                [partition],
            )?;
            Ok(())
        })
        .await?;
    service.schedule_replacements(Exchange::Binance).await?;
    let new = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let s = service
                .request(Request {
                    exchange: Exchange::Binance,
                    kind: Kind::Tick,
                    a: 2,
                    b: 0,
                    lookback: 1,
                })
                .await?;
            if s.key != requested.key && s.status == "ready" {
                return Ok::<_, anyhow::Error>(s);
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await??;
    let snapshot = service.snapshot(&new, None, 500, 1).await?;
    assert_eq!(snapshot["activeBar"]["tradeCount"], 1);
    assert_ne!(new.id, s.id);
    let notice = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let event = listener.recv().await?;
            if event["seriesKey"] == requested.key && event["code"] == "CHART_SERIES_REPLACED" {
                return Ok::<_, anyhow::Error>(());
            }
        }
    })
    .await?;
    notice?;
    let retired = router
        .clone()
        .oneshot(
            HttpRequest::builder()
                .uri(format!("/api/v1/footprints/chart-bars?seriesKey={}", s.key))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(retired.status(), 409);
    let invalid=router.clone().oneshot(HttpRequest::builder().method("POST").uri("/api/v1/footprints/chart-series").header("content-type","application/json").body(Body::from(json!({"marketKey":"binance:perpetual:BTCUSDT","chartType":"tick","tradesPerBar":true,"lookbackDays":1}).to_string()))?).await?;
    assert_eq!(invalid.status(), 400);
    // Serve a real WS endpoint to verify trailing-slash compatibility and the
    // subscribe acknowledgement the frontend uses before refreshing HTTP.
    let socket_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = socket_listener.local_addr()?;
    let http = tokio::spawn(async move { axum::serve(socket_listener, router).await });
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;
    let (mut ws, _) =
        tokio_tungstenite::connect_async(format!("ws://{address}/ws/v1/charts/")).await?;
    ws.send(Message::Text(
        json!({"op":"subscribe","seriesKey":new.key})
            .to_string()
            .into(),
    ))
    .await?;
    let response = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(Ok(Message::Text(text))) = ws.next().await {
                return serde_json::from_str::<Value>(&text);
            }
        }
    })
    .await??;
    assert_eq!(response["type"], "subscribed");
    ws.close(None).await?;
    // A resumed checkpoint beyond raw retention must get a new generation,
    // rather than silently joining expired history to today's tape.
    let old_id = new.id;
    let old_key = new.key.clone();
    let mut expired = new.checkpoint.clone();
    expired.cursor.as_mut().unwrap().day = day(now_us()) - 31;
    let checkpoint = postcard::to_allocvec(&expired)?;
    service
        .db
        .call(move |c| {
            c.execute(
                "UPDATE series SET checkpoint=? WHERE id=?",
                rusqlite::params![checkpoint, old_id],
            )?;
            Ok(())
        })
        .await?;
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let current = service
                .request(Request {
                    exchange: Exchange::Binance,
                    kind: Kind::Tick,
                    a: 2,
                    b: 0,
                    lookback: 1,
                })
                .await?;
            if current.key != old_key && current.status == "ready" {
                ensure!(
                    current.checkpoint.cursor.unwrap().day == partition,
                    "stale cursor survived rebase"
                );
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await??;
    http.abort();
    stop.cancel();
    live.await??;
    cold.await??;
    service
        .store
        .sql(&format!("DROP DATABASE {} SYNC", config.database), vec![])
        .await?;
    Ok(())
}
