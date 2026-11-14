//! riod-run: Resource-aware service launcher for riod-managed dinit services.
//!
//! Reads a service profile from RIOD_SERVICES_DIR/<name>.toml, declares intent
//! and allocates resources via the riod daemon (if available), then runs the
//! command with heartbeats and cleanup on exit.
//!
//! Fail-open: if riod is down or unresponsive, the command runs directly without
//! resource allocation. The service is never blocked by supervisor failure.
//!
//! Usage:
//!   riod-run <service-name>

use clap::Parser;
use riod_client::{AllocateRequest, RiodClient};
use serde::Deserialize;
use std::io::{self, Write};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

#[derive(Parser, Debug)]
#[command(name = "riod-run", about = "Resource-aware service launcher for riod-managed services")]
struct Args {
    /// Service name (matches profile file in RIOD_SERVICES_DIR).
    service_name: String,
}

/// Service profile loaded from TOML.
#[derive(Debug, Deserialize)]
struct Profile {
    name: String,
    purpose: String,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    resources: Resources,
    command: CommandSpec,
}

#[derive(Debug, Default, Deserialize)]
struct Resources {
    #[serde(default)]
    gpu_count: usize,
    #[serde(default)]
    ram_mb: Option<u64>,
    #[serde(default)]
    cpu_percent: Option<f32>,
    #[serde(default = "default_ttl")]
    ttl_secs: u64,
}

fn default_ttl() -> u64 {
    3600
}

#[derive(Debug, Deserialize)]
struct CommandSpec {
    binary: String,
    #[serde(default)]
    args: Vec<String>,
}

fn log(msg: &str) {
    eprintln!("[riod-run] {}", msg);
    let _ = io::stderr().flush();
}

fn load_profile(service_name: &str) -> Profile {
    let services_dir = std::env::var("RIOD_SERVICES_DIR")
        .unwrap_or_else(|_| "/etc/riod/services".into());
    let profile_path = PathBuf::from(services_dir).join(format!("{}.toml", service_name));

    if !profile_path.exists() {
        log(&format!(
            "profile not found: {} (set RIOD_SERVICES_DIR to override)",
            profile_path.display()
        ));
        std::process::exit(1);
    }

    let content = match std::fs::read_to_string(&profile_path) {
        Ok(c) => c,
        Err(e) => {
            log(&format!("failed to read profile {}: {}", profile_path.display(), e));
            std::process::exit(1);
        }
    };

    match toml::from_str(&content) {
        Ok(p) => p,
        Err(e) => {
            log(&format!("failed to parse profile: {}", e));
            std::process::exit(1);
        }
    }
}

fn main() {
    let args = Args::parse();
    let profile = load_profile(&args.service_name);

    let socket_path = std::env::var("RIOD_SOCKET").unwrap_or_else(|_| "/run/riod.sock".into());
    let client_id = format!("dinit:{}", args.service_name);

    log(&format!(
        "starting {} (purpose={}, ttl={}s)",
        profile.name, profile.purpose, profile.resources.ttl_secs
    ));

    // Try to connect to riod and allocate resources (fail-open).
    // RiodClient::connect() is lazy — verify with an actual RPC call.
    let mut allocation_id: Option<String> = None;
    let mut client: Option<RiodClient> = None;

    match RiodClient::connect(&socket_path) {
        Ok(c) => match c.system_status() {
            Ok(_) => {
                log("riod is reachable, requesting resources");
                client = Some(c);
            }
            Err(e) => log(&format!(
                "riod unreachable ({}) — running in fail-open mode (no allocation)",
                e
            )),
        },
        Err(e) => log(&format!(
            "failed to create riod client: {} — fail-open mode",
            e
        )),
    }

    if let Some(ref client) = client {
        // Declare intent (advisory — proceed even if it fails)
        let mut intent_id: Option<String> = None;
        match client.declare_intent(&client_id, &profile.purpose, profile.description.as_deref()) {
            Ok(intent) => {
                if intent.status == "granted" {
                    intent_id = Some(intent.id.clone());
                    log(&format!("intent granted: {}", intent.id));
                } else {
                    log(&format!(
                        "intent status={} — allocating without intent",
                        intent.status
                    ));
                }
            }
            Err(e) => {
                log(&format!("intent declaration failed: {} — continuing", e));
            }
        }

        // Build allocation request from profile
        let mut req = AllocateRequest::new(&client_id, &profile.purpose)
            .with_ttl(profile.resources.ttl_secs);

        if profile.resources.gpu_count > 0 {
            req = req.with_gpu_count(profile.resources.gpu_count);
        } else {
            // Explicitly CPU-only: empty gpus vector means no GPU allocation
            req = req.with_gpus(vec![]);
        }

        if let Some(ram) = profile.resources.ram_mb {
            req = req.with_ram_mb(ram);
        }
        if let Some(cpu) = profile.resources.cpu_percent {
            req = req.with_cpu_percent(cpu);
        }
        if let Some(ref iid) = intent_id {
            req = req.with_intent_id(iid);
        }

        // Allocate resources
        match client.allocate(&req) {
            Ok(alloc) => {
                allocation_id = Some(alloc.id.clone());
                log(&format!(
                    "allocation granted: {} (gpus={:?}, ram={}MB)",
                    alloc.id, alloc.gpus, alloc.ram_mb
                ));
            }
            Err(e) => {
                log(&format!(
                    "allocation failed: {} — running without riod resource tracking",
                    e
                ));
            }
        }
    }

    // Spawn child process (inherit stdio so dinit captures logs normally)
    let mut child = match Command::new(&profile.command.binary)
        .args(&profile.command.args)
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
    {
        Ok(c) => c,
        Err(e) => {
            log(&format!(
                "failed to start command '{}': {}",
                profile.command.binary, e
            ));
            release_allocation(&allocation_id, &client_id, &socket_path);
            std::process::exit(127);
        }
    };

    let child_pid = child.id();
    log(&format!("child process started (pid={})", child_pid));

    // Start heartbeat thread if we have an allocation
    let hb_stop = Arc::new(AtomicBool::new(false));
    let mut hb_handle = None;

    if let Some(ref alloc_id) = allocation_id {
        let hb_stop_clone = hb_stop.clone();
        let client_clone = client.clone();
        let alloc_id_clone = alloc_id.clone();
        let client_id_clone = client_id.clone();

        // Heartbeat at 1/3 of TTL, minimum 60s interval
        let interval = Duration::from_secs(profile.resources.ttl_secs.saturating_div(3).max(60));

        hb_handle = Some(thread::spawn(move || {
            while !hb_stop_clone.load(Ordering::Relaxed) {
                thread::sleep(interval);
                if hb_stop_clone.load(Ordering::Relaxed) {
                    break;
                }
                if let Some(c) = &client_clone {
                    match c.heartbeat(&alloc_id_clone, &client_id_clone) {
                        Ok(_) => {}
                        Err(e) => log(&format!("heartbeat failed: {}", e)),
                    }
                }
            }
        }));
    }

    // Set up signal handling: catch SIGTERM/SIGINT so we can clean up before exiting.
    // dinit sends SIGTERM to the process group, so the child also receives it directly.
    // We intercept here to release the allocation before our process exits.
    let signal_received = Arc::new(AtomicBool::new(false));
    {
        let flag = signal_received.clone();
        thread::spawn(move || {
            use signal_hook::consts::{SIGINT, SIGTERM};
            use signal_hook::iterator::Signals;
            if let Ok(mut signals) = Signals::new([SIGINT, SIGTERM]) {
                for sig in signals.forever() {
                    log(&format!("received signal {}", sig));
                    flag.store(true, Ordering::SeqCst);
                    break;
                }
            }
        });
    }

    // Wait for child to exit (either normally or from dinit's SIGTERM to the process group).
    // Poll with try_wait so we can detect signals and force-kill a stubborn child.
    let status = loop {
        match child.try_wait() {
            Ok(Some(s)) => break s, // Child exited naturally
            Ok(None) => {
                if signal_received.load(Ordering::SeqCst) {
                    log("signal received — terminating child");
                    let _ = child.kill();
                    match child.wait() {
                        Ok(s) => break s,
                        Err(e) => {
                            log(&format!("error waiting for killed child: {}", e));
                            std::process::exit(1);
                        }
                    }
                }
                thread::sleep(Duration::from_millis(200));
            }
            Err(e) => {
                log(&format!("wait error: {} — retrying", e));
                thread::sleep(Duration::from_millis(500));
            }
        }
    };

    let exit_code = status.code().unwrap_or(1);

    // Stop heartbeat
    hb_stop.store(true, Ordering::Relaxed);
    if let Some(h) = hb_handle {
        let _ = h.join();
    }

    // Release allocation
    release_allocation(&allocation_id, &client_id, &socket_path);

    log(&format!("{} exited with code {}", profile.name, exit_code));
    std::process::exit(exit_code);
}

fn release_allocation(allocation_id: &Option<String>, client_id: &str, socket_path: &str) {
    let Some(alloc_id) = allocation_id else {
        return;
    };

    match RiodClient::connect(socket_path) {
        Ok(client) => match client.release(alloc_id, client_id) {
            Ok(_) => log(&format!("allocation released: {}", alloc_id)),
            Err(e) => log(&format!(
                "failed to release allocation {}: {} (will expire via TTL)",
                alloc_id, e
            )),
        },
        Err(_) => log("riod unreachable during cleanup — allocation will expire via TTL"),
    }
}
