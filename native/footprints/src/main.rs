use anyhow::{Context, Result, ensure};
use clap::{Parser, Subcommand};
use footprints::{
    config::{Config, Exchange},
    health::Health,
    service::Service,
    storage::Store,
};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

#[derive(Parser)]
#[command(version, about = "Durable BTC execution capture and Tick/TRev charts")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}
#[derive(Subcommand)]
enum Command {
    Collect {
        #[arg(long)]
        exchange: Exchange,
        #[arg(long)]
        standby: bool,
    },
    Serve,
    Migrate,
    /// Validate a downloaded public daily archive without writing to ClickHouse.
    ArchiveCheck {
        #[arg(long)]
        exchange: Exchange,
        #[arg(long)]
        date: chrono::NaiveDate,
        #[arg(long)]
        file: std::path::PathBuf,
    },
    /// Offline prepared/scalar Rust benchmark with exact result parity.
    Bench {
        #[arg(long, default_value_t = 1_000_000)]
        rows: usize,
    },
    /// Read-only replay of existing prepared chunks; no database or cache writes.
    BenchPrepared {
        #[arg(long)]
        directory: std::path::PathBuf,
        #[arg(long)]
        exchange: Exchange,
        #[arg(long, default_value_t = 3)]
        samples: usize,
        #[arg(long, default_value_t = 504)]
        continuation: u64,
        #[arg(long, default_value_t = 704)]
        reversal: u64,
    },
    /// Precompute shared TRev trade tapes for verified recent archives.
    WarmPrepared {
        #[arg(long, default_value_t = 5)]
        days: u32,
    },
    /// Record ready-state, queues, gaps and delivery lag every five seconds.
    Soak {
        #[arg(long, default_value_t = 72)]
        hours: u32,
        #[arg(long)]
        output: std::path::PathBuf,
    },
    Health {
        #[arg(long, default_value = "http://127.0.0.1:8010")]
        url: String,
        #[arg(long)]
        ready: bool,
        #[arg(long)]
        overlap: bool,
        #[arg(long)]
        writer: bool,
    },
    Control {
        #[arg(value_parser=["promote","drain"])]
        action: String,
        #[arg(long, default_value = "http://127.0.0.1:8010")]
        url: String,
    },
}
#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "footprints=info".into()),
        )
        .json()
        .init();
    let cli = Cli::parse();
    match cli.command {
        Command::Health {
            url,
            ready,
            overlap,
            writer,
        } => {
            let response = reqwest::Client::builder()
                .timeout(Duration::from_secs(4))
                .build()?
                .get(format!(
                    "{url}/health/{}",
                    if ready || overlap || writer {
                        "ready"
                    } else {
                        "live"
                    }
                ))
                .send()
                .await?
                .error_for_status()?;
            let status: footprints::health::Status = response.json().await?;
            ensure!(!overlap || status.overlap, "standby overlap not ready");
            ensure!(
                !writer || status.writer,
                "collector is not the active writer"
            );
            println!("{}", serde_json::to_string(&status)?);
            return Ok(());
        }
        Command::Control { action, url } => {
            reqwest::Client::builder()
                .timeout(Duration::from_secs(5))
                .build()?
                .post(format!("{url}/admin/{action}"))
                .send()
                .await?
                .error_for_status()?;
            return Ok(());
        }
        Command::Bench { rows } => {
            println!("{}", footprints::ops::benchmark(rows)?);
            return Ok(());
        }
        Command::BenchPrepared {
            directory,
            exchange,
            samples,
            continuation,
            reversal,
        } => {
            println!(
                "{}",
                footprints::ops::benchmark_prepared(
                    &directory,
                    exchange,
                    samples,
                    continuation,
                    reversal
                )?
            );
            return Ok(());
        }
        Command::ArchiveCheck {
            exchange,
            date,
            file,
        } => {
            let temp = tempfile::NamedTempFile::new()?;
            let partition = footprints::config::day(
                date.and_hms_opt(0, 0, 0)
                    .context("invalid archive date")?
                    .and_utc()
                    .timestamp_micros(),
            );
            let archive =
                footprints::recovery::parse_archive(&file, temp.path(), exchange, partition)?;
            println!(
                "{}",
                serde_json::json!({"exchange":exchange.name(),"date":date,"rows":archive.rows,"volumeAtoms":archive.volume.to_string(),"firstId":archive.first.id,"lastId":archive.last.id})
            );
            return Ok(());
        }
        _ => {}
    }
    let config = Config::load()?;
    if let Command::Soak { hours, ref output } = cli.command {
        return footprints::ops::soak(&config, hours, output).await;
    }
    if matches!(cli.command, Command::Migrate) {
        return Store::new(&config)?.migrate().await;
    }
    if let Command::WarmPrepared { days } = cli.command {
        let service = Service::open(
            config,
            Health::new("warm-prepared", CancellationToken::new()),
        )
        .await?;
        return service.warm_recent_archives(days).await;
    }
    let stop = CancellationToken::new();
    let signals = stop.clone();
    tokio::spawn(async move {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("SIGTERM handler");
        tokio::select! {_=tokio::signal::ctrl_c()=>{},_=term.recv()=>{}}
        signals.cancel();
    });
    let role = match &cli.command {
        Command::Collect { exchange, .. } => exchange.name(),
        _ => "serve",
    };
    let health = Health::new(role, stop.clone());
    health.watchdog();
    let listener = tokio::net::TcpListener::bind(config.bind).await?;
    let mut tasks = tokio::task::JoinSet::new();
    let mut ownership = None;
    let routes = match cli.command {
        Command::Collect { exchange, standby } => {
            let h = health.clone();
            tasks.spawn(footprints::capture::run(
                config.clone(),
                exchange,
                standby,
                h,
            ));
            health.router()
        }
        Command::Serve => {
            ownership = Some(
                footprints::db::try_lock(&config.directory.join("server.lock"))?
                    .context("another chart writer owns this volume")?,
            );
            Store::new(&config)?.migrate().await?;
            let service = Service::open(config, health.clone()).await?;
            service.verify_storage_history().await?;
            for live in [true, false] {
                let s = service.clone();
                tasks.spawn(async move { s.worker(live).await });
            }
            let s = service.clone();
            tasks.spawn(async move { s.monitor().await });
            tasks.spawn(footprints::recovery::run(service.clone()));
            tasks.spawn(footprints::recovery::audit_loop(service.clone()));
            footprints::api::router(service).merge(health.router())
        }
        _ => unreachable!(),
    };
    let shutdown = stop.clone();
    tasks.spawn(async move {
        axum::serve(listener, routes)
            .with_graceful_shutdown(shutdown.cancelled_owned())
            .await
            .map_err(Into::into)
    });
    let mut result = tokio::select! {
        _=stop.cancelled()=>Ok(()),
        result=tasks.join_next()=>match result {
            Some(Ok(Ok(()))) if stop.is_cancelled()=>Ok(()),
            Some(Ok(Ok(())))=>Err(anyhow::anyhow!("critical worker exited unexpectedly")),
            Some(Ok(Err(e)))=>Err(e),Some(Err(e))=>Err(e.into()),
            None=>Err(anyhow::anyhow!("all workers exited"))
        }
    };
    stop.cancel();
    let drain = async {
        while let Some(result) = tasks.join_next().await {
            result??;
        }
        Ok::<_, anyhow::Error>(())
    };
    match tokio::time::timeout(Duration::from_secs(40), drain).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            if result.is_ok() {
                result = Err(error)
            }
        }
        Err(_) => {
            tasks.abort_all();
            result = Err(anyhow::anyhow!("shutdown drain timed out"));
        }
    }
    drop(ownership);
    result
}
