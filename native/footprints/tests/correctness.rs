use footprints::{
    config::{Config, Exchange, day},
    engine::{Bar, Engine, State},
    journal::Journal,
    model::{Checkpoint, RawTrade, Spec, Trade, atoms},
    prepared::Batch,
    service::{Kind, Series, project},
    venues::{self, Frame},
};
use serde::Deserialize;
use serde_json::{Value, json};
use std::collections::BTreeMap;

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
        origins: vec!["http://localhost:5173".into()],
        collector_urls: vec![],
    }
}
fn raw(id: u64) -> RawTrade {
    RawTrade {
        id: id.to_string(),
        sequence: id,
        timestamp: 1_700_000_000_000_000 + id as i64 * 1000,
        price: 10_000_000_000_000,
        quantity: 1_000_000_000,
        buy: id.is_multiple_of(2),
    }
}
#[derive(Deserialize)]
struct Case {
    kind: String,
    a: i64,
    b: i64,
    rows: Vec<Execution>,
    bars: Vec<Value>,
    state: State,
}
#[derive(Deserialize)]
struct Execution {
    timestamp_us: u64,
    price_tick: i64,
    quantity_units: u64,
    is_taker_buy: bool,
    spec_id: u64,
    gap: bool,
}
fn canonical(bar: &Bar) -> Value {
    json!({"bar_index":bar.index,"spec_id":bar.spec,"break_before":bar.gap,"open_tick":bar.open,"high_tick":bar.high,"low_tick":bar.low,"close_tick":bar.close,"buy_units":bar.buy,"sell_units":bar.sell,"trade_count":bar.count,"opened_timestamp_us":bar.opened,"ended_timestamp_us":bar.ended,"levels":bar.levels.iter().map(|(p,(b,s))|json!({"price_tick":p,"buy_units":b,"sell_units":s})).collect::<Vec<_>>()})
}
#[test]
fn frozen_python_oracle_and_restart_parity() {
    let cases: Vec<Case> = serde_json::from_str(include_str!("fixtures/engines.json")).unwrap();
    for (number, case) in cases.iter().enumerate() {
        for checkpoint_every in [1, 7, 97, 1000] {
            let mut engine = Engine::new(case.a, case.b.max(1), 500, 4000).unwrap();
            if case.kind == "tick" {
                engine.tick_count = Some(case.a as u64)
            }
            let mut bars = Vec::new();
            for (index, r) in case.rows.iter().enumerate() {
                if r.gap {
                    engine.truncate(&mut bars).unwrap()
                }
                engine
                    .push(
                        r.timestamp_us,
                        r.price_tick,
                        r.quantity_units,
                        r.is_taker_buy,
                        r.spec_id,
                        &mut bars,
                    )
                    .unwrap();
                if index % checkpoint_every == 0 {
                    engine.state =
                        postcard::from_bytes(&postcard::to_allocvec(&engine.state).unwrap())
                            .unwrap();
                }
            }
            assert_eq!(
                bars.iter().map(canonical).collect::<Vec<_>>(),
                case.bars,
                "case {number}, chunk {checkpoint_every}"
            );
            assert_eq!(engine.state, case.state, "case {number}");
        }
    }
}
#[test]
fn prepared_runs_preserve_boundaries_gaps_specs_and_timestamp_regressions() {
    let cases: Vec<Case> = serde_json::from_str(include_str!("fixtures/engines.json")).unwrap();
    for case in cases {
        let kind = if case.kind == "tick" {
            Kind::Tick
        } else {
            Kind::Trev
        };
        let mut segment = 1;
        let rows: Vec<_> = case
            .rows
            .iter()
            .enumerate()
            .map(|(index, r)| {
                if r.gap {
                    segment += 1
                }
                let spec = Spec {
                    tick: r.spec_id as i64,
                    step: r.spec_id,
                };
                Trade {
                    market: 1,
                    day: day(r.timestamp_us as i64),
                    revision: 0,
                    ordinal: index as u64 + 1,
                    segment,
                    spec,
                    raw: RawTrade {
                        id: index.to_string(),
                        sequence: index as u64,
                        timestamp: r.timestamp_us as i64,
                        price: r.price_tick * spec.tick,
                        quantity: r.quantity_units * spec.step,
                        buy: r.is_taker_buy,
                    },
                }
            })
            .collect();
        let mut expected = Engine::new(case.a, case.b.max(1), 500, 4000).unwrap();
        expected.tick_count = (kind == Kind::Tick).then_some(case.a as u64);
        let mut plain = Vec::new();
        for (trade, r) in rows.iter().zip(&case.rows) {
            if r.gap {
                expected.truncate(&mut plain).unwrap()
            }
            expected
                .push(
                    r.timestamp_us,
                    r.price_tick,
                    r.quantity_units,
                    r.is_taker_buy,
                    trade.spec.id(),
                    &mut plain,
                )
                .unwrap();
        }
        for width in [1, 13, 300] {
            let mut series = Series {
                id: 1,
                key: "x".into(),
                config_key: "x".into(),
                exchange: Exchange::Binance,
                kind,
                a: case.a as u64,
                b: case.b as u64,
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
            let mut actual = Vec::new();
            for chunk in rows.chunks(width) {
                let mut batch = Batch::new(chunk.to_vec()).unwrap();
                if kind == Kind::Trev {
                    batch.compact();
                }
                let encoded = postcard::to_allocvec(&batch).unwrap();
                let batch = postcard::from_bytes(&encoded).unwrap();
                let (cp, bars) = project(&series, &batch).unwrap();
                series.checkpoint = cp;
                actual.extend(bars);
            }
            assert_eq!(series.checkpoint.state, expected.state);
            assert_eq!(actual.len(), plain.len());
            for (a, b) in actual.iter().zip(&plain) {
                assert_eq!(a.value.buy_units as u128, b.buy);
                assert_eq!(a.value.sell_units as u128, b.sell);
                assert_eq!(a.value.trade_count, b.count);
                assert_eq!(a.value.close_tick, b.close);
                assert_eq!(a.value.break_before, b.gap);
            }
        }
    }
}
#[test]
fn exact_decimal_and_wire_validation() {
    assert_eq!(atoms("0.000000000001").unwrap(), 1);
    assert_eq!(atoms("1.230000000000000").unwrap(), 1_230_000_000_000);
    for invalid in [
        "NaN",
        "1e-3",
        "-1",
        ".1",
        "1.0000000000001",
        "18446744073709551615",
    ] {
        assert!(atoms(invalid).is_err(), "{invalid}")
    }
    let frame=venues::frame(Exchange::Binance,r#"{"e":"trade","s":"BTCUSDT","t":100,"T":1700000000000,"p":"10000.1","q":"0.001","m":true}"#).unwrap();
    let Frame::Trades(rows) = frame else { panic!() };
    assert!(!rows[0].buy);
    assert_eq!(rows[0].price, 10_000_100_000_000_000);
    assert!(venues::frame(Exchange::Binance, r#"{"id":1,"code":-1}"#).is_err());
    assert!(matches!(
        venues::frame(Exchange::Bybit, r#"{"op":"ping","ret_msg":"pong"}"#).unwrap(),
        Frame::Pong
    ));
    assert!(matches!(venues::frame(Exchange::Hyperliquid,r#"{"channel":"subscriptionResponse","data":{"method":"subscribe","subscription":{"type":"trades","coin":"BTC"}}}"#).unwrap(),Frame::Ready));
}
#[test]
fn rest_bridge_requires_matching_endpoints_and_public_order() {
    let rows = (10..15).map(raw).collect::<Vec<_>>();
    assert_eq!(
        venues::bridge(Exchange::Binance, &rows[0], &rows[4], &rows)
            .unwrap()
            .len(),
        3
    );
    let mut missing = rows.clone();
    missing.remove(2);
    // Public Binance trades exclude insurance-fund/ADL records. Numeric holes
    // are legitimate; a bounded REST response is the market-trade authority.
    assert_eq!(
        venues::bridge(Exchange::Binance, &rows[0], &rows[4], &missing)
            .unwrap()
            .len(),
        2
    );
    assert!(venues::bridge(Exchange::Binance, &rows[0], &rows[4], &rows[1..]).is_err());
    missing.swap(1, 2);
    assert!(venues::bridge(Exchange::Binance, &rows[0], &rows[4], &missing).is_err());
    let mut conflict = rows[0].clone();
    conflict.quantity += 1;
    assert!(venues::bridge(Exchange::Binance, &conflict, &rows[4], &rows).is_err());
    // Bybit sequence covers multiple messages and is not a per-trade counter.
    let mut same = rows.clone();
    for row in &mut same {
        row.sequence = 99
    }
    assert_eq!(
        venues::bridge(Exchange::Bybit, &same[0], &same[4], &same)
            .unwrap()
            .len(),
        3
    );
}
#[tokio::test]
async fn journal_replay_dedup_gap_and_quota_are_atomic() {
    let directory = tempfile::tempdir().unwrap();
    let mut cfg = config(directory.path());
    let j = Journal::open(&cfg, Exchange::Binance).await.unwrap();
    j.append(vec![raw(1), raw(2)]).await.unwrap();
    j.append(vec![raw(2), raw(3)]).await.unwrap();
    assert_eq!(j.stats().await.unwrap().pending_rows, 3);
    let mut conflict = raw(2);
    conflict.price += 1;
    assert!(j.append(vec![raw(4), conflict]).await.is_err());
    assert_eq!(j.tail().await.unwrap(), Some(raw(3)));
    j.gap(raw(3).timestamp, raw(5).timestamp, "test_disconnect")
        .await
        .unwrap();
    j.append(vec![raw(5)]).await.unwrap();
    drop(j);
    let j = Journal::open(&cfg, Exchange::Binance).await.unwrap();
    let (id, rows) = j.first().await.unwrap().unwrap();
    assert_eq!(rows.len(), 4);
    assert_ne!(rows[2].segment, rows[3].segment);
    assert_eq!(
        rows.iter().map(|r| r.ordinal).collect::<Vec<_>>(),
        vec![1, 2, 3, 4]
    );
    j.ack(id).await.unwrap();
    assert_eq!(j.stats().await.unwrap().pending_rows, 0);
    drop(j);
    cfg.journal_bytes = 1;
    let j = Journal::open(&cfg, Exchange::Binance).await.unwrap();
    assert!(j.append(vec![raw(6)]).await.is_err());
    assert_eq!(j.tail().await.unwrap(), Some(raw(5)));
    assert!(j.first().await.unwrap().is_none());
}
#[tokio::test]
async fn disk_reserve_does_not_advance_cursor() {
    let directory = tempfile::tempdir().unwrap();
    let mut cfg = config(directory.path());
    cfg.reserve_bytes = u64::MAX;
    let j = Journal::open(&cfg, Exchange::Hyperliquid).await.unwrap();
    assert!(j.append(vec![raw(1)]).await.is_err());
    assert_eq!(j.tail().await.unwrap(), None);
}
#[test]
fn single_writer_lock_survives_competition_and_releases_on_exit() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("writer.lock");
    let owner = footprints::db::try_lock(&path).unwrap().unwrap();
    assert!(footprints::db::try_lock(&path).unwrap().is_none());
    drop(owner);
    assert!(footprints::db::try_lock(&path).unwrap().is_some());
}
#[tokio::test]
async fn committed_journal_survives_process_kill() {
    let dir = tempfile::tempdir().unwrap();
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "journal_crash_child", "--nocapture"])
        .env("FOOTPRINT_TEST_CRASH_DIR", dir.path())
        .status()
        .unwrap();
    assert!(!status.success());
    let j = Journal::open(&config(dir.path()), Exchange::Binance)
        .await
        .unwrap();
    assert_eq!(j.tail().await.unwrap(), Some(raw(2)));
    assert_eq!(j.first().await.unwrap().unwrap().1.len(), 2);
}
#[tokio::test]
async fn journal_crash_child() {
    let Ok(path) = std::env::var("FOOTPRINT_TEST_CRASH_DIR") else {
        return;
    };
    let j = Journal::open(&config(std::path::Path::new(&path)), Exchange::Binance)
        .await
        .unwrap();
    j.append(vec![raw(1), raw(2)]).await.unwrap();
    std::process::exit(99);
}
#[test]
fn archive_validation_accepts_number_gaps_but_rejects_duplicates() {
    use footprints::recovery::{parse_csv, timestamp_seconds};
    let dir = tempfile::tempdir().unwrap();
    let partition = day(1_700_000_000_000_000);
    let csv = "id,price,qty,quote_qty,time,is_buyer_maker\n1,10000.1,0.001,10,1700000000000,true\n2,10000.2,0.002,20,1700000000001,false\n";
    let archive = parse_csv(
        csv.as_bytes(),
        &dir.path().join("valid.db"),
        Exchange::Binance,
        partition,
    )
    .unwrap();
    assert_eq!(archive.rows, 2);
    assert_eq!(archive.volume, 3_000_000_000);
    assert!(
        parse_csv(
            csv.replace("\n2,", "\n3,").as_bytes(),
            &dir.path().join("number_gap.db"),
            Exchange::Binance,
            partition
        )
        .is_ok()
    );
    assert!(
        parse_csv(
            csv.replace("\n2,", "\n1,").as_bytes(),
            &dir.path().join("duplicate.db"),
            Exchange::Binance,
            partition
        )
        .is_err()
    );
    assert_eq!(
        timestamp_seconds("1700000000.123999").unwrap(),
        1_700_000_000_123_000
    );
    assert!(footprints::recovery::archive_url(Exchange::Hyperliquid, partition).is_err());
}
#[test]
fn chart_request_rejects_floats_booleans_unknown_markets_and_excessive_work() {
    let valid = json!({"marketKey":"binance:perpetual:BTCUSDT","chartType":"tick","tradesPerBar":17,"lookbackDays":5});
    assert!(footprints::api::parse_request(&valid).is_ok());
    for (key, value) in [
        ("tradesPerBar", json!(true)),
        ("tradesPerBar", json!(17.1)),
        ("tradesPerBar", json!(4001)),
        ("lookbackDays", json!(31)),
        ("marketKey", json!("binance:perpetual:ETHUSDT")),
    ] {
        let mut input = valid.clone();
        input[key] = value;
        assert!(footprints::api::parse_request(&input).is_err());
    }
}
#[test]
fn rowbinary_roundtrip_rejects_partial_records() {
    let trade = Trade {
        market: 1,
        day: 20000,
        revision: 0,
        ordinal: 1,
        segment: 1,
        spec: Spec::initial(Exchange::Binance),
        raw: raw(1),
    };
    let mut bytes = Vec::new();
    footprints::storage::encode_trade(&mut bytes, &trade);
    assert_eq!(
        footprints::storage::decode_trade(&mut footprints::storage::Input::new(&bytes)).unwrap(),
        trade
    );
    for end in 0..bytes.len() {
        assert!(
            footprints::storage::decode_trade(&mut footprints::storage::Input::new(&bytes[..end]))
                .is_err()
        );
    }
}
