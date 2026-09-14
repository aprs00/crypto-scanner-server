use footprints::{
    config::{Config, Exchange, now_us},
    health::Health,
    journal::Journal,
    service::{Kind, Request, Service},
};
use tokio_util::sync::CancellationToken;

fn config(dir: &std::path::Path) -> Config {
    Config {
        directory: dir.into(),
        bind: "127.0.0.1:0".parse().unwrap(),
        clickhouse_url: "http://127.0.0.1:1".into(),
        database: "test".into(),
        user: "default".into(),
        password: String::new(),
        journal_bytes: 1_000_000,
        reserve_bytes: 0,
        origins: vec![],
        collector_urls: vec![],
    }
}
#[tokio::test]
async fn promotion_survives_restart_without_promoting_a_new_candidate() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(dir.path());
    let journal = Journal::open(&cfg, Exchange::Bybit).await.unwrap();
    journal.activate("candidate-a".into()).await.unwrap();
    drop(journal);
    assert!(Journal::promoted(dir.path(), "candidate-a").unwrap());
    assert!(!Journal::promoted(dir.path(), "candidate-b").unwrap());
}
#[tokio::test]
async fn replaced_or_rolled_back_journal_cannot_overwrite_stored_ordinals() {
    use footprints::model::RawTrade;
    let dir = tempfile::tempdir().unwrap();
    let journal = Journal::open(&config(dir.path()), Exchange::Bybit)
        .await
        .unwrap();
    journal
        .append(vec![RawTrade {
            id: "first".into(),
            timestamp: now_us(),
            sequence: 1,
            price: 60_000_000_000_000_000,
            quantity: 1_000_000_000,
            buy: true,
        }])
        .await
        .unwrap();
    let stored = journal.first().await.unwrap().unwrap().1.remove(0);
    journal
        .verify_delivery_history(stored.clone())
        .await
        .unwrap();
    let mut conflicting = stored.clone();
    conflicting.raw.id = "different".into();
    assert!(journal.verify_delivery_history(conflicting).await.is_err());
    let mut newer = stored;
    newer.ordinal += 1;
    assert!(journal.verify_delivery_history(newer).await.is_err());
}
#[tokio::test]
async fn lease_capacity_applies_to_reactivated_configs_and_websockets() {
    let dir = tempfile::tempdir().unwrap();
    let service = Service::open(
        config(dir.path()),
        Health::new("test", CancellationToken::new()),
    )
    .await
    .unwrap();
    let request = |a| Request {
        exchange: Exchange::Binance,
        kind: Kind::Tick,
        a,
        b: 0,
        lookback: 1,
    };
    let old = service.request(request(1)).await.unwrap();
    let id = old.id;
    service
        .db
        .call(move |c| {
            c.execute(
                "UPDATE series SET requested=? WHERE id=?",
                rusqlite::params![now_us() - 2_000_000_000, id],
            )?;
            Ok(())
        })
        .await
        .unwrap();
    for a in 2..=17 {
        service.request(request(a)).await.unwrap();
    }
    assert!(service.request(request(1)).await.is_err());
    assert!(service.get(&old.key, true).await.is_err());
    assert!(service.request(request(2)).await.is_ok());
}
#[test]
fn compacted_tape_resume_matches_scalar_engine_across_output_limits() {
    use footprints::{
        engine::Engine,
        model::{Checkpoint, RawTrade, Spec, Trade},
        prepared::Batch,
        service::{Series, project},
    };
    let spec = Spec::initial(Exchange::Binance);
    let time = now_us();
    let mut plain = Engine::new(1, 1, 500, 4000).unwrap();
    let mut expected = Vec::new();
    let rows = (1..=10_000)
        .map(|i| {
            let price = 600_000 + i / 4 % 10;
            plain
                .push(
                    time as u64 + i as u64,
                    price,
                    1,
                    i % 2 == 0,
                    spec.id(),
                    &mut expected,
                )
                .unwrap();
            Trade {
                market: 1,
                day: 1,
                revision: 0,
                ordinal: i as u64,
                segment: 1,
                spec,
                raw: RawTrade {
                    id: i.to_string(),
                    sequence: i as u64,
                    timestamp: time + i,
                    price: price * spec.tick,
                    quantity: spec.step,
                    buy: i % 2 == 0,
                },
            }
        })
        .collect();
    let mut batch = Batch::new(rows).unwrap();
    batch.compact();
    let mut series = Series {
        id: 1,
        key: "test".into(),
        config_key: "test".into(),
        exchange: Exchange::Binance,
        kind: Kind::Trev,
        a: 1,
        b: 1,
        lookback: 1,
        anchor: 0,
        requested: 0,
        status: "building".into(),
        error: None,
        checkpoint: Checkpoint::default(),
        manifest: Default::default(),
        active: true,
        supersedes: None,
    };
    let mut actual = Vec::new();
    let mut steps = 0;
    while series.checkpoint.cursor.is_none_or(|c| c.ordinal < 10_000) {
        let (cp, bars) = project(&series, &batch).unwrap();
        series.checkpoint = cp;
        actual.extend(bars);
        steps += 1;
        assert!(steps < 10);
    }
    assert!(steps > 1);
    assert_eq!(actual.len(), expected.len());
    assert_eq!(series.checkpoint.state, plain.state);
}
