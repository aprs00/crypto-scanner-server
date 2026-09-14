use crate::{
    config::now_us,
    journal::{Gap, JournalStats},
};
use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use std::sync::{
    Arc, RwLock,
    atomic::{AtomicBool, AtomicI64, Ordering},
};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct Status {
    pub role: String,
    pub writer: bool,
    pub feeds: [bool; 2],
    pub overlap: bool,
    pub last_trade_at: i64,
    pub last_journal_at: i64,
    pub last_delivery_at: i64,
    pub chart_lag_ms: i64,
    pub journal: JournalStats,
    pub gaps: Vec<Gap>,
    pub reconnects: u64,
    pub rejected_frames: u64,
    pub error: Option<String>,
    pub ready: bool,
}
#[derive(Clone)]
pub struct Health {
    pub status: Arc<RwLock<Status>>,
    pub progress: Arc<AtomicI64>,
    pub promote: Arc<AtomicBool>,
    pub stop: CancellationToken,
}
impl Health {
    pub fn new(role: &str, stop: CancellationToken) -> Self {
        Self {
            status: Arc::new(RwLock::new(Status {
                role: role.into(),
                ..Status::default()
            })),
            progress: Arc::new(AtomicI64::new(now_us())),
            promote: Arc::new(AtomicBool::new(false)),
            stop,
        }
    }
    pub fn update(&self, f: impl FnOnce(&mut Status)) {
        f(&mut self.status.write().expect("health lock"))
    }
    pub fn snapshot(&self) -> Status {
        self.status.read().expect("health lock").clone()
    }
    pub fn beat(&self) {
        self.progress.store(now_us(), Ordering::Relaxed)
    }
    pub fn live(&self) -> bool {
        now_us() - self.progress.load(Ordering::Relaxed) < 30_000_000
    }
    pub fn watchdog(&self) {
        let h = self.clone();
        std::thread::spawn(move || {
            while !h.stop.is_cancelled() {
                std::thread::sleep(std::time::Duration::from_secs(5));
                if now_us() - h.progress.load(Ordering::Relaxed) > 60_000_000 {
                    eprintln!(
                        "critical event loop stalled; restarting with durable journal intact"
                    );
                    std::process::exit(1);
                }
            }
        });
    }
    pub fn router(&self) -> Router {
        Router::new()
        .route("/health/live",get(|State(h):State<Health>|async move{(if h.live(){StatusCode::OK}else{StatusCode::SERVICE_UNAVAILABLE},Json(h.snapshot()))}))
        .route("/health/ready",get(|State(h):State<Health>|async move{(if h.live() && h.snapshot().ready{StatusCode::OK}else{StatusCode::SERVICE_UNAVAILABLE},Json(h.snapshot()))}))
        .route("/health/status",get(|State(h):State<Health>|async move{Json(h.snapshot())}))
        .route("/admin/promote",post(|State(h):State<Health>|async move{h.promote.store(true,Ordering::Relaxed);StatusCode::ACCEPTED}))
        .route("/admin/drain",post(|State(h):State<Health>|async move{h.stop.cancel();StatusCode::ACCEPTED}))
        .route("/metrics",get(|State(h):State<Health>|async move{let s=h.snapshot();format!("footprint_writer {}\nfootprint_feed_ready{{slot=\"0\"}} {}\nfootprint_feed_ready{{slot=\"1\"}} {}\nfootprint_journal_pending_rows {}\nfootprint_journal_pending_bytes {}\nfootprint_delivery_lag_seconds {}\nfootprint_chart_lag_seconds {}\nfootprint_reconnects_total {}\nfootprint_rejected_frames_total {}\n",s.writer as u8,s.feeds[0] as u8,s.feeds[1] as u8,s.journal.pending_rows,s.journal.pending_bytes,s.journal.oldest_pending_at.map(|v|(now_us()-v).max(0)/1_000_000).unwrap_or(0),s.chart_lag_ms/1000,s.reconnects,s.rejected_frames)}))
        .with_state(self.clone())
    }
}
