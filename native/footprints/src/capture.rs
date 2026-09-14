use crate::{
    config::{Config, Exchange, now_us},
    db::try_lock,
    health::Health,
    journal::Journal,
    model::{RawTrade, Spec},
    storage::Store,
    venues::{self, Frame},
};
use anyhow::{Context, Result, ensure};
use futures_util::{SinkExt, StreamExt};
use std::{collections::VecDeque, sync::atomic::Ordering, time::Duration};
use tokio::{
    sync::{mpsc, watch},
    time::{Instant, interval, sleep},
};
use tokio_tungstenite::{
    connect_async_with_config,
    tungstenite::{Message, protocol::WebSocketConfig},
};
use tokio_util::sync::CancellationToken;

const BUFFER: usize = 50_000;
enum Event {
    Ready(usize),
    Trades(usize, Vec<RawTrade>),
    Down(usize, String),
    Spec(Spec),
}
#[derive(Default)]
struct Slot {
    ready: bool,
    buffer: VecDeque<RawTrade>,
    since: Option<Instant>,
    latest: Option<Instant>,
}
fn retain(slot: &mut Slot, rows: Vec<RawTrade>) {
    for r in rows {
        if slot.buffer.len() == BUFFER {
            slot.buffer.pop_front();
        }
        slot.buffer.push_back(r)
    }
    slot.latest = Some(Instant::now());
}
fn stalled_primary(
    slots: &[Slot; 2],
    current: usize,
    last: Option<&RawTrade>,
    since: &mut Option<Instant>,
    now: Instant,
) -> bool {
    let peer = &slots[1 - current];
    let ahead = peer.ready
        && slots[current]
            .latest
            .is_some_and(|at| now.duration_since(at) >= Duration::from_secs(5))
        && peer
            .latest
            .is_some_and(|at| now.duration_since(at) < Duration::from_secs(2))
        && last.is_some_and(|last| {
            peer.buffer
                .iter()
                .position(|row| row.same_execution(last))
                .is_some_and(|index| index + 1 < peer.buffer.len())
        });
    if !ahead {
        *since = None;
        return false;
    }
    now.duration_since(*since.get_or_insert(now)) >= Duration::from_secs(5)
}
pub fn retry_delay(attempt: u32) -> Duration {
    let cap = (250u64.saturating_mul(1u64 << attempt.min(7))).min(30_000);
    Duration::from_millis(cap / 2 + rand::random_range(0..=cap / 2))
}

async fn feed(
    exchange: Exchange,
    slot: usize,
    tx: mpsc::Sender<Event>,
    mut rotation: watch::Receiver<u64>,
    stop: CancellationToken,
) {
    let mut attempt = 0;
    loop {
        if stop.is_cancelled() {
            return;
        }
        let started = Instant::now();
        let result = connection(exchange, slot, &tx, &mut rotation, &stop).await;
        if stop.is_cancelled() {
            return;
        }
        let reason = result
            .err()
            .map(|e| e.to_string())
            .unwrap_or_else(|| "planned rotation".into());
        if tx.send(Event::Down(slot, reason)).await.is_err() {
            return;
        }
        if started.elapsed() > Duration::from_secs(60) {
            attempt = 0;
        }
        tokio::select! {_=stop.cancelled()=>return,_=sleep(retry_delay(attempt))=>{}}
        attempt = attempt.saturating_add(1);
    }
}
async fn connection(
    exchange: Exchange,
    slot: usize,
    tx: &mpsc::Sender<Event>,
    rotation: &mut watch::Receiver<u64>,
    stop: &CancellationToken,
) -> Result<()> {
    let config = WebSocketConfig::default()
        .max_message_size(Some(1_048_576))
        .max_frame_size(Some(1_048_576));
    let (mut ws, _) = tokio::time::timeout(
        Duration::from_secs(10),
        connect_async_with_config(exchange.ws_url(), Some(config), false),
    )
    .await??;
    ws.send(Message::Text(
        venues::subscription(exchange).to_string().into(),
    ))
    .await?;
    let mut ready = false;
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut heartbeat = interval(Duration::from_secs(if exchange == Exchange::Hyperliquid {
        30
    } else {
        20
    }));
    heartbeat.tick().await;
    let mut timeout = interval(Duration::from_secs(1));
    let mut outstanding: Option<Instant> = None;
    loop {
        tokio::select! {
            _=stop.cancelled()=>{let _=ws.close(None).await;return Ok(())},
            _=rotation.changed()=>{let _=ws.close(None).await;return Ok(())},
            _=timeout.tick()=>{ensure!(ready || Instant::now()<deadline,"subscription timeout");ensure!(outstanding.is_none_or(|at|at.elapsed()<Duration::from_secs(10)),"heartbeat timeout");},
            _=heartbeat.tick()=>{
                let msg=match exchange {Exchange::Binance=>Message::Ping(vec![slot as u8].into()),Exchange::Bybit=>Message::Text("{\"op\":\"ping\"}".into()),Exchange::Hyperliquid=>Message::Text("{\"method\":\"ping\"}".into())};
                ws.send(msg).await?;outstanding=Some(Instant::now());
            },
            item=ws.next()=>match item.context("socket closed")?? {
                Message::Text(text)=>match venues::frame(exchange,&text).map_err(|e|anyhow::anyhow!("invalid exchange frame: {e}"))? {
                    Frame::Ready=>{ready=true;tx.send(Event::Ready(slot)).await?;},
                    Frame::Pong=>outstanding=None,
                    Frame::Trades(rows)=>tx.send(Event::Trades(slot,rows)).await?,
                    Frame::Ignore=>{},
                },
                Message::Ping(bytes)=>ws.send(Message::Pong(bytes)).await?,
                Message::Pong(_)=>outstanding=None,
                Message::Close(_)=>anyhow::bail!("peer closed socket"),
                _=>{},
            }
        }
    }
}

pub async fn run(config: Config, exchange: Exchange, standby: bool, health: Health) -> Result<()> {
    let instance = std::env::var("FOOTPRINT_INSTANCE").unwrap_or_else(|_| "primary".into());
    ensure!(
        !standby || instance != "primary",
        "--standby requires a unique FOOTPRINT_INSTANCE"
    );
    let resumed = standby && Journal::promoted(&config.directory, &instance)?;
    let (tx, mut rx) = mpsc::channel(256);
    let mut rotations = Vec::new();
    let mut workers = tokio::task::JoinSet::new();
    for slot in 0..2 {
        let (rotate, changed) = watch::channel(0);
        rotations.push(rotate);
        workers.spawn(feed(
            exchange,
            slot,
            tx.clone(),
            changed,
            health.stop.clone(),
        ));
    }
    let client = venues::http_client()?;
    let spec_tx = tx.clone();
    let spec_stop = health.stop.clone();
    let spec_client = client.clone();
    workers.spawn(async move {
        loop {
            if let Ok(spec) = venues::specification(exchange, &spec_client).await
                && spec_tx.send(Event::Spec(spec)).await.is_err()
            {
                return;
            }
            tokio::select! {_=spec_stop.cancelled()=>return,_=sleep(Duration::from_secs(3600))=>{}}
        }
    });
    drop(tx);
    let mut slots = [Slot::default(), Slot::default()];
    let mut leader: Option<usize> = None;
    let mut ownership = None;
    let mut journal: Option<Journal> = None;
    let mut drainer = None;
    let mut last: Option<RawTrade> = None;
    let mut pending = Vec::new();
    let mut latest_spec = None;
    let mut tick = interval(Duration::from_millis(10));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut next_health = Instant::now();
    let mut next_rotation = Instant::now() + Duration::from_secs(41_400);
    let mut storage_paused = false;
    let mut stalled_since = None;
    let mut gap_needed = Some("collector_restart".to_owned());
    loop {
        health.beat();
        let mut flush = false;
        tokio::select! {
            _=health.stop.cancelled()=>break,
            event=rx.recv()=>match event.context("all feed workers stopped")? {
                Event::Ready(slot)=>{slots[slot].ready=true;slots[slot].since=Some(Instant::now());},
                Event::Trades(slot,rows)=>{
                    if rows.is_empty() {continue;}
                    if leader==Some(slot) && !storage_paused {
                        // Public Binance IDs legitimately skip excluded trade
                        // types. Continuity comes from overlap and candle audits,
                        // never an assumption that the ID counter is consecutive.
                        for row in rows {
                            if exchange.recoverable() && last.as_ref().is_some_and(|old|row.sequence<old.sequence) {
                                commit_pending(journal.as_ref().unwrap(),&mut pending).await?;
                                journal.as_ref().unwrap().gap(row.timestamp,last.as_ref().unwrap().timestamp,"source_order_regressed").await?;
                            }
                            last=Some(row.clone());pending.push(row);
                        }
                    }else{retain(&mut slots[slot],rows)}
                    slots[slot].latest=Some(Instant::now());
                },
                Event::Down(slot,error)=>{
                    tracing::warn!(exchange=exchange.name(),slot,%error,"execution connection ended");
                    health.update(|s|{s.reconnects+=1;if error.starts_with("invalid exchange frame:"){s.rejected_frames+=1;}});
                    slots[slot].ready=false;slots[slot].buffer.clear();slots[slot].since=None;
                    if leader==Some(slot){leader=None;gap_needed=Some(error)}
                },
                Event::Spec(spec)=>{latest_spec=Some(spec);if let Some(j)=&journal{commit_pending(j,&mut pending).await?;j.spec(spec).await?;}},
            },
            _=tick.tick()=>{flush=true;},
            result=workers.join_next(),if !workers.is_empty()=>{
                if !health.stop.is_cancelled(){anyhow::bail!("critical feed worker exited: {result:?}");}
            }
        }
        if ownership.is_none()
            && (!standby || resumed || health.promote.load(Ordering::Relaxed))
            && slots.iter().any(|s| s.ready)
        {
            ownership = try_lock(&config.directory.join("writer.lock"))?;
            if ownership.is_some() {
                let j = Journal::open(&config, exchange).await?;
                j.activate(instance.clone()).await?;
                last = j.tail().await?;
                if let Some(spec) = latest_spec {
                    j.spec(spec).await?;
                }
                let outbox = j.clone();
                let store = Store::new(&config)?;
                let h = health.clone();
                drainer = Some(tokio::spawn(async move { deliver(outbox, store, h).await }));
                journal = Some(j);
                health.update(|s| s.writer = true);
            }
        }
        if let Some(journal) = journal.as_ref()
            && leader.is_none()
            && !storage_paused
        {
            let matching = slots
                .iter()
                .enumerate()
                .filter(|(_, s)| s.ready && !s.buffer.is_empty())
                .find(|(_, s)| {
                    last.as_ref()
                        .is_some_and(|last| s.buffer.iter().any(|r| r.id == last.id))
                })
                .map(|(i, _)| i);
            let selected =
                matching.or_else(|| slots.iter().position(|s| s.ready && !s.buffer.is_empty()));
            if let Some(slot) = selected {
                let mut rows: Vec<_> = slots[slot].buffer.drain(..).collect();
                let mut overlap = false;
                if let Some(old) = &last {
                    if let Some(index) = rows.iter().position(|r| r.id == old.id) {
                        ensure!(old.same_execution(&rows[index]), "conflicting feed overlap");
                        rows.drain(..=index);
                        overlap = true;
                    } else if exchange.recoverable() {
                        // Remove old observations before choosing REST endpoints.
                        rows.retain(|r| r.sequence >= old.sequence);
                        if let Some(first) = rows.first()
                            && let Ok(snapshot) = venues::recent(exchange, &client).await
                            && let Ok(mut bridge) = venues::bridge(exchange, old, first, &snapshot)
                        {
                            bridge.extend(rows);
                            rows = bridge;
                            overlap = true;
                        }
                    }
                }
                if !overlap && let Some(last) = last.as_ref() {
                    let start = last.timestamp;
                    let end = rows.first().map(|r| r.timestamp).unwrap_or_else(now_us);
                    commit_pending(journal, &mut pending).await?;
                    journal
                        .gap(
                            start,
                            end,
                            gap_needed.as_deref().unwrap_or("unverified_handoff"),
                        )
                        .await?;
                }
                gap_needed = None;
                if let Some(row) = rows.last() {
                    last = Some(row.clone())
                }
                pending.extend(rows);
                leader = Some(slot);
                health.update(|s| {
                    s.overlap = overlap;
                    s.ready = true;
                });
            }
        }
        if let Some(j) = &journal {
            if !pending.is_empty() && (flush || pending.len() >= 1000) {
                match j.append(pending.clone()).await {
                    Ok(_) => {
                        pending.clear();
                        storage_paused = false;
                        health.update(|s| {
                            s.last_journal_at = now_us();
                            s.error = None;
                        });
                    }
                    Err(error) => {
                        // Keep the exact pending batch. Stop taking live input until
                        // storage returns; bounded socket overlap supplies a bridge.
                        let message = error.to_string();
                        health.update(|s| {
                            s.error = Some(message.clone());
                            s.ready = false;
                        });
                        if !message.starts_with("JOURNAL_") {
                            return Err(error);
                        }
                        if !storage_paused {
                            leader = None;
                            gap_needed = Some("storage_exhausted".into());
                            storage_paused = true;
                        }
                        tokio::select! {_=health.stop.cancelled()=>break,_=sleep(Duration::from_secs(1))=>{}}
                    }
                }
            }
            if Instant::now() >= next_health {
                if drainer.as_ref().is_some_and(|task| task.is_finished()) {
                    anyhow::bail!("journal delivery worker exited unexpectedly");
                }
                let stats = j.stats().await?;
                let gaps = j.gaps().await?;
                health.update(|s| {
                    s.journal = stats;
                    s.gaps = gaps;
                });
                next_health = Instant::now() + Duration::from_secs(1);
            }
        }
        // Verify overlap and promote first, then close only the obsolete socket.
        if let Some(current) = leader {
            let other = 1 - current;
            let scheduled = exchange == Exchange::Binance
                && (Instant::now() >= next_rotation
                    || slots.iter().any(|s| {
                        s.since
                            .is_some_and(|t| t.elapsed() >= Duration::from_secs(82_800))
                    }));
            let stalled = stalled_primary(
                &slots,
                current,
                last.as_ref(),
                &mut stalled_since,
                Instant::now(),
            );
            if (scheduled || stalled)
                && slots[other].ready
                && last
                    .as_ref()
                    .is_some_and(|r| slots[other].buffer.iter().any(|b| b.same_execution(r)))
            {
                let retiring = if stalled {
                    current
                } else {
                    slots
                        .iter()
                        .enumerate()
                        .filter_map(|(i, s)| s.since.map(|t| (i, t)))
                        .min_by_key(|(_, t)| *t)
                        .map(|(i, _)| i)
                        .unwrap_or(current)
                };
                if retiring == current {
                    commit_pending(journal.as_ref().unwrap(), &mut pending).await?;
                    let end = slots[other]
                        .buffer
                        .iter()
                        .position(|row| row.same_execution(last.as_ref().unwrap()))
                        .context("handoff overlap vanished")?;
                    slots[other].buffer.drain(..=end);
                    pending.extend(slots[other].buffer.drain(..));
                    if let Some(row) = pending.last() {
                        last = Some(row.clone());
                    }
                    leader = Some(other);
                    gap_needed = None;
                    health.update(|s| s.overlap = true);
                }
                slots[retiring].ready = false;
                stalled_since = None;
                slots[retiring].buffer.clear();
                rotations[retiring].send_modify(|n| *n += 1);
                next_rotation =
                    Instant::now() + Duration::from_secs(41_400 - rand::random_range(0..=300));
            }
        }
        if journal.is_none() && Instant::now() >= next_health {
            let directory = config.directory.clone();
            let tail =
                tokio::task::spawn_blocking(move || Journal::peek_tail(&directory)).await??;
            let overlap = tail.as_ref().is_some_and(|last| {
                slots
                    .iter()
                    .any(|s| s.ready && s.buffer.iter().any(|r| r.same_execution(last)))
            });
            health.update(|s| {
                s.overlap = overlap;
                s.ready = slots.iter().all(|s| s.ready) && overlap;
            });
            next_health = Instant::now() + Duration::from_secs(1);
        }
        health.update(|s| {
            s.feeds = [slots[0].ready, slots[1].ready];
            s.last_trade_at = last.as_ref().map(|r| r.timestamp).unwrap_or(0);
            if journal.is_some() {
                s.ready = leader.is_some() && !storage_paused;
            }
        });
    }
    workers.abort_all();
    if let Some(j) = &journal
        && !pending.is_empty()
    {
        j.append(pending)
            .await
            .context("shutdown journal drain failed")?;
    }
    if let Some(mut task) = drainer
        && tokio::time::timeout(Duration::from_secs(35), &mut task)
            .await
            .is_err()
    {
        task.abort();
        let _ = task.await;
    }
    health.update(|s| {
        s.writer = false;
        s.ready = false;
    });
    drop(journal);
    drop(ownership);
    Ok(())
}
async fn commit_pending(journal: &Journal, pending: &mut Vec<RawTrade>) -> Result<()> {
    if !pending.is_empty() {
        journal.append(pending.clone()).await?;
        pending.clear();
    }
    Ok(())
}
async fn deliver(journal: Journal, store: Store, health: Health) -> Result<()> {
    let mut attempt = 0;
    let mut history_checked = false;
    loop {
        if health.stop.is_cancelled() {
            return Ok(());
        }
        match journal.first().await {
            Ok(Some((id, rows))) => match async {
                if !history_checked {
                    if let Some(latest) = store.latest(rows[0].market).await? {
                        journal.verify_delivery_history(latest).await?;
                    }
                    history_checked = true;
                }
                store.insert_trades(&rows).await
            }
            .await
            {
                Ok(()) => {
                    journal.ack(id).await?;
                    attempt = 0;
                    health.update(|s| s.last_delivery_at = now_us());
                }
                Err(error) => {
                    if error.to_string().starts_with("JOURNAL_HISTORY_MISMATCH") {
                        return Err(error);
                    }
                    tracing::warn!(%error,"ClickHouse delivery delayed; capture remains durable");
                    tokio::select! {_=health.stop.cancelled()=>return Ok(()),_=sleep(retry_delay(attempt))=>{}}
                    attempt += 1;
                }
            },
            Ok(None) => {
                tokio::select! {_=health.stop.cancelled()=>return Ok(()),_=sleep(Duration::from_millis(50))=>{}}
            }
            Err(error) => return Err(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quiet_market_resume_does_not_rotate_a_healthy_primary() {
        let now = Instant::now();
        let trade = RawTrade {
            id: "1".into(),
            timestamp: 1,
            sequence: 1,
            price: 1,
            quantity: 1,
            buy: true,
        };
        let mut next = trade.clone();
        next.id = "2".into();
        let mut slots = [Slot::default(), Slot::default()];
        slots[0].latest = Some(now - Duration::from_secs(20));
        slots[1].ready = true;
        slots[1].latest = Some(now);
        slots[1].buffer = VecDeque::from([trade.clone(), next.clone()]);
        let mut since = None;
        assert!(!stalled_primary(&slots, 0, Some(&trade), &mut since, now));
        slots[0].latest = Some(now + Duration::from_millis(10));
        assert!(!stalled_primary(
            &slots,
            0,
            Some(&next),
            &mut since,
            now + Duration::from_millis(10)
        ));
        assert!(since.is_none());
    }

    #[test]
    fn sustained_peer_progress_retires_a_stuck_primary() {
        let now = Instant::now();
        let trade = RawTrade {
            id: "1".into(),
            timestamp: 1,
            sequence: 1,
            price: 1,
            quantity: 1,
            buy: true,
        };
        let mut next = trade.clone();
        next.id = "2".into();
        let mut slots = [Slot::default(), Slot::default()];
        slots[0].latest = Some(now - Duration::from_secs(6));
        slots[1].ready = true;
        slots[1].latest = Some(now);
        slots[1].buffer = VecDeque::from([trade.clone(), next]);
        let mut since = None;
        assert!(!stalled_primary(&slots, 0, Some(&trade), &mut since, now));
        slots[1].latest = Some(now + Duration::from_secs(5));
        assert!(stalled_primary(
            &slots,
            0,
            Some(&trade),
            &mut since,
            now + Duration::from_secs(5)
        ));
    }
}
