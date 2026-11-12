//! rIOd daemon entry point.
//!
//! Starts the resource observer loop and Unix socket server.
//! Designed to run under dinit supervision (no PID 1 responsibilities).

use chrono::Utc;
use clap::{Parser, Subcommand};
use riod::broker::ResourceBroker;
use riod::ledger::{EventLedger, LedgerConfig, LedgerEvent};
use riod::observe::{IdleDetector, ObserveConfig};
use riod::socket::{self};
use riod::state::ServerState;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::signal;

/// rIOd — System Resource Supervisor
#[derive(Parser, Debug)]
#[command(name = "riod", version, about)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Run the rIOd daemon (default)
    Daemon {
        /// Path to Unix socket for client API
        #[arg(long, default_value = "/run/riod.sock")]
        socket_path: PathBuf,

        /// Path to event ledger JSONL file
        #[arg(long, default_value = "/var/log/riod/events.jsonl")]
        ledger_path: PathBuf,

        /// Resource polling interval in seconds
        #[arg(long, default_value_t = 30)]
        poll_interval_s: u64,

        /// CPU usage threshold for idle detection (percent)
        #[arg(long, default_value_t = 5.0)]
        idle_cpu_percent: f32,

        /// GPU utilization threshold for idle detection (percent)
        #[arg(long, default_value_t = 10)]
        idle_gpu_percent: u8,

        /// Consecutive idle samples required to declare system idle
        #[arg(long, default_value_t = 20)]
        idle_sample_count: usize,

        /// Enable cgroup v2 enforcement for allocations (advisory if not set)
        #[arg(long)]
        cgroups: bool,

        /// Unix socket file permissions (octal, e.g. "777" or "660")
        #[arg(long, default_value = "777")]
        socket_perm: String,
    },
    /// Query the event ledger directly (no daemon required)
    Ledger {
        #[command(subcommand)]
        action: LedgerAction,
    },
}

#[derive(Subcommand, Debug)]
enum LedgerAction {
    /// Show most recent N events
    Tail {
        /// Number of events to show
        #[arg(short, long, default_value_t = 20)]
        n: usize,
        /// Path to ledger file
        #[arg(long, default_value = "/var/log/riod/events.jsonl")]
        path: PathBuf,
    },
    /// Show ledger statistics
    Stats {
        /// Path to ledger file
        #[arg(long, default_value = "/var/log/riod/events.jsonl")]
        path: PathBuf,
    },
}

/// Daemon configuration (extracted from CLI args).
#[derive(Debug)]
struct DaemonConfig {
    socket_path: PathBuf,
    ledger_path: PathBuf,
    poll_interval_s: u64,
    idle_cpu_percent: f32,
    idle_gpu_percent: u8,
    idle_sample_count: usize,
    cgroups_enabled: bool,
    socket_perm: u32,
}

impl Default for DaemonConfig {
    fn default() -> Self {
        Self {
            socket_path: PathBuf::from("/run/riod.sock"),
            ledger_path: PathBuf::from("/var/log/riod/events.jsonl"),
            poll_interval_s: 30,
            idle_cpu_percent: 5.0,
            idle_gpu_percent: 10,
            idle_sample_count: 20,
            cgroups_enabled: false,
            socket_perm: 0o777,
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing (RUST_LOG=riod=debug for verbose)
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("riod=info")),
        )
        .with_target(false)
        .init();

    let cli = Cli::parse();

    match cli.command {
        Some(Command::Ledger { action }) => run_ledger_cli(action),
        Some(Command::Daemon {
            socket_path,
            ledger_path,
            poll_interval_s,
            idle_cpu_percent,
            idle_gpu_percent,
            idle_sample_count,
            cgroups,
            socket_perm,
        }) => {
            let socket_perm = u32::from_str_radix(&socket_perm, 8).unwrap_or(0o777);
            let config = DaemonConfig {
                socket_path,
                ledger_path,
                poll_interval_s,
                idle_cpu_percent,
                idle_gpu_percent,
                idle_sample_count,
                cgroups_enabled: cgroups,
                socket_perm,
            };
            run_daemon(config).await
        }
        None => run_daemon(DaemonConfig::default()).await,
    }
}

fn run_ledger_cli(action: LedgerAction) -> Result<(), Box<dyn std::error::Error>> {
    match action {
        LedgerAction::Tail { n, path } => {
            let config = LedgerConfig {
                path: path.clone(),
                retention_days: 30,
            };
            let ledger = EventLedger::open(config)?;
            let events = ledger.tail(n)?;
            for event in &events {
                println!(
                    "{} {} {}",
                    event.timestamp.format("%Y-%m-%dT%H:%M:%SZ"),
                    event.id,
                    serde_json::to_string(&event.event)?
                );
            }
            Ok(())
        }
        LedgerAction::Stats { path } => {
            let config = LedgerConfig {
                path: path.clone(),
                retention_days: 30,
            };
            let ledger = EventLedger::open(config)?;
            let count = ledger.entry_count()?;
            println!("entries: {}", count);
            println!("path: {}", path.display());
            Ok(())
        }
    }
}

async fn run_daemon(config: DaemonConfig) -> Result<(), Box<dyn std::error::Error>> {
    tracing::info!(
        version = env!("CARGO_PKG_VERSION"),
        socket = %config.socket_path.display(),
        ledger = %config.ledger_path.display(),
        poll_interval_s = config.poll_interval_s,
        "starting rIOd daemon"
    );

    // Initialize components
    let observe_config = ObserveConfig {
        idle_cpu_percent: config.idle_cpu_percent,
        idle_gpu_percent: config.idle_gpu_percent,
        idle_sample_count: config.idle_sample_count,
    };

    let mut detector = IdleDetector::new(observe_config);
    let ledger = EventLedger::open(LedgerConfig {
        path: config.ledger_path.clone(),
        retention_days: 30,
    })?;

    // Do an initial resource poll to get hardware inventory for the broker
    let initial_snapshot = detector.poll();
    let (gpus, total_ram_mb) = match &initial_snapshot {
        Ok(snap) => (snap.gpus.clone(), snap.ram_total_mb),
        Err(e) => {
            tracing::warn!(error = %e, "initial resource poll failed, broker will have empty inventory");
            (vec![], 0u64)
        }
    };
    let mut broker = ResourceBroker::new(gpus, total_ram_mb);
    if config.cgroups_enabled {
        tracing::info!("cgroup v2 enforcement enabled");
        broker.enable_cgroups();
    } else {
        tracing::info!("cgroup enforcement disabled (advisory mode)");
    }

    // Wrap in mutexes for shared access between observer loop and socket server
    let detector_mutex = std::sync::Mutex::new(detector);
    let ledger_mutex = std::sync::Mutex::new(ledger);
    let broker_mutex = std::sync::Mutex::new(broker);

    // Log daemon start event
    {
        let mut ledger = ledger_mutex.lock().unwrap();
        ledger.append(LedgerEvent::DaemonStarted {
            version: env!("CARGO_PKG_VERSION").to_string(),
            socket_path: config.socket_path.to_string_lossy().to_string(),
            ledger_path: config.ledger_path.to_string_lossy().to_string(),
            poll_interval_s: config.poll_interval_s,
        })?;
    }

    let started_at = Utc::now();
    let state = Arc::new(ServerState {
        detector: detector_mutex,
        ledger: ledger_mutex,
        broker: broker_mutex,
        started_at,
        poll_interval_s: config.poll_interval_s,
    });

    // Spawn the resource observer loop
    let observer_state = Arc::clone(&state);
    let poll_interval = config.poll_interval_s;
    let observer_handle = tokio::spawn(async move {
        run_observer_loop(observer_state, poll_interval).await;
    });

    // Spawn the stale allocation reclamation task (checks every 30s)
    let reclaim_state = Arc::clone(&state);
    let reclaim_handle = tokio::spawn(async move {
        run_reclaim_loop(reclaim_state).await;
    });

    // Spawn the socket server
    let socket_state = Arc::clone(&state);
    let socket_path_clone = config.socket_path.clone();
    let socket_perm = config.socket_perm;
    let socket_handle = tokio::spawn(async move {
        if let Err(e) = socket::serve(socket_path_clone, socket_perm, socket_state).await {
            tracing::error!(error = %e, "socket server failed");
        }
    });

    // Wait for shutdown signal (SIGTERM/SIGINT from dinit)
    shutdown_signal().await;

    tracing::info!("shutdown signal received, stopping daemon");

    // Log daemon stop event
    {
        let mut ledger = state.ledger.lock().unwrap();
        let _ = ledger.append(LedgerEvent::DaemonStopped {
            reason: "shutdown signal".to_string(),
        });
    }

    // Cancel background tasks
    observer_handle.abort();
    reclaim_handle.abort();
    socket_handle.abort();

    // Clean up socket file
    let _ = std::fs::remove_file(&config.socket_path);

    tracing::info!("riod daemon stopped");
    Ok(())
}

/// The resource observation loop. Polls resources at the configured interval
/// and logs snapshots to the event ledger.
async fn run_observer_loop(state: Arc<ServerState>, interval_s: u64) {
    let mut last_idle_state = false;

    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(interval_s)).await;

        // Poll resources (synchronous, short-lived lock)
        let snapshot_result = {
            let mut detector = state.detector.lock().unwrap();
            detector.poll()
        };

        match snapshot_result {
            Ok(snapshot) => {
                // Log resource snapshot to ledger
                {
                    let mut ledger = state.ledger.lock().unwrap();
                    if let Err(e) = ledger.append(LedgerEvent::ResourceSnapshot {
                        data: serde_json::to_value(&snapshot).unwrap_or_default(),
                    }) {
                        tracing::warn!(error = %e, "failed to log resource snapshot");
                    }
                }

                // Detect idle state transitions
                let is_idle = {
                    let detector = state.detector.lock().unwrap();
                    detector.is_system_idle
                };

                if is_idle != last_idle_state {
                    last_idle_state = is_idle;
                    let mut ledger = state.ledger.lock().unwrap();
                    if is_idle {
                        let idle_since = {
                            let detector = state.detector.lock().unwrap();
                            detector.idle_since.unwrap_or(Utc::now())
                        };
                        let gpu_utils: Vec<u8> = snapshot
                            .gpus
                            .iter()
                            .map(|g| g.utilization_percent)
                            .collect();
                        if let Err(e) = ledger.append(LedgerEvent::IdleDetected {
                            idle_since,
                            cpu_percent: snapshot.cpu_usage_percent,
                            gpu_utilization: gpu_utils,
                        }) {
                            tracing::warn!(error = %e, "failed to log idle detection");
                        }
                    } else {
                        if let Err(e) = ledger.append(LedgerEvent::IdleEnded {
                            idle_duration_secs: 0,
                            trigger: "activity resumed".to_string(),
                        }) {
                            tracing::warn!(error = %e, "failed to log idle end");
                        }
                    }
                }

                tracing::debug!(
                    cpu = snapshot.cpu_usage_percent,
                    ram_mb = snapshot.ram_used_mb,
                    gpus = snapshot.gpus.len(),
                    idle = is_idle,
                    "resource snapshot"
                );
            }
            Err(e) => {
                tracing::warn!(error = %e, "resource polling failed");
            }
        }
    }
}

async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => tracing::info!("received Ctrl+C"),
        _ = terminate => tracing::info!("received SIGTERM"),
    }
}

/// Background task that periodically reclaims expired/stale allocations.
async fn run_reclaim_loop(state: Arc<ServerState>) {
    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;

        let reclaimed = {
            let mut broker = state.broker.lock().unwrap();
            let mut ledger = state.ledger.lock().unwrap();
            broker.reclaim_expired(&mut ledger)
        };

        if reclaimed > 0 {
            tracing::info!(count = reclaimed, "reclaimed stale allocations");
        }
    }
}
