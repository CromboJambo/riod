//! riod-dream-consumer: Picks up queued dream tasks when the scheduler triggers.
//!
//! Watches `dream.status` via the riod socket API. When a dream cycle is triggered,
//! reads the next task from the queue directory, allocates resources with the cycle ID,
//! executes the task command, and reports completion.
//!
//! Usage:
//!   riod-dream-consumer [--queue-dir ~/.riod/dream-queue] [--socket /run/riod.sock] [--poll-interval 30]

use clap::Parser;
use serde::{Deserialize, Serialize};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

#[derive(Parser, Debug)]
#[command(name = "riod-dream-consumer", about = "Execute queued dream tasks on riod idle cycles")]
struct Args {
    /// Directory containing task definition JSON files.
    #[arg(long, default_value = "~/.riod/dream-queue")]
    queue_dir: String,

    /// Path to the riod Unix socket.
    #[arg(long, default_value = "/run/riod.sock")]
    socket: String,

    /// How often to poll dream status (seconds).
    #[arg(long, default_value_t = 30)]
    poll_interval: u64,

    /// Run once and exit (for testing).
    #[arg(long)]
    once: bool,
}

/// A queued dream task definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct DreamTask {
    name: String,
    description: String,
    workdir: String,
    #[serde(default)]
    context_file: Option<String>,
    command: String,
    estimated_duration_mins: u64,
    #[serde(default)]
    requires_gpu: bool,
    #[serde(default = "default_priority")]
    priority: u32,
    #[serde(default)]
    created_at: Option<String>,
}

fn default_priority() -> u32 {
    10
}

/// Minimal dream status response from riod.
#[derive(Debug, Deserialize)]
struct DreamStatus {
    state: String,
    #[serde(default)]
    cycle_id: Option<String>,
}

/// Raw JSON-RPC client for the riod socket (avoids pulling in full riod-client deps).
struct RiodRpc {
    socket_path: PathBuf,
}

impl RiodRpc {
    fn new(socket_path: impl Into<PathBuf>) -> Self {
        Self {
            socket_path: socket_path.into(),
        }
    }

    /// Send a JSON-RPC request and return the parsed response.
    fn call(
        &self,
        method: &str,
        params: Option<serde_json::Value>,
    ) -> Result<serde_json::Value, String> {
        let id = std::process::id();
        let request = serde_json::json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params.unwrap_or(serde_json::Value::Null),
        });

        // Build payload (newline-terminated)
        let mut payload = serde_json::to_string(&request)
            .map_err(|e| format!("serialize request: {}", e))?;
        payload.push('\n');

        // Connect and send
        let mut stream = std::os::unix::net::UnixStream::connect(&self.socket_path)
            .map_err(|e| format!("failed to connect to {}: {}", self.socket_path.display(), e))?;
        stream.set_read_timeout(Some(Duration::from_secs(30)))
            .map_err(|e| format!("set_read_timeout: {}", e))?;

        stream.write_all(payload.as_bytes())
            .map_err(|e| format!("write: {}", e))?;
        stream.flush().map_err(|e| format!("flush: {}", e))?;

        // Read response line-by-line from the same connection
        let mut buffer = Vec::new();
        let mut chunk = [0u8; 4096];
        loop {
            match stream.read(&mut chunk) {
                Ok(0) => break,
                Ok(n) => {
                    buffer.extend_from_slice(&chunk[..n]);
                    if let Some(pos) = buffer.iter().position(|&b| b == b'\n') {
                        buffer.truncate(pos + 1);
                        break;
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
                Err(e) => return Err(format!("read response: {}", e)),
            }
        }

        let response_str = String::from_utf8_lossy(&buffer);
        if response_str.trim().is_empty() {
            return Err("empty response from riod".to_string());
        }

        let response: serde_json::Value = serde_json::from_str(response_str.trim())
            .map_err(|e| format!("invalid JSON response: {}", e))?;

        if let Some(error) = response.get("error") {
            return Err(format!("RPC error: {}", error));
        }

        Ok(response.get("result").cloned().unwrap_or(serde_json::Value::Null))
    }

    /// Query dream status.
    fn dream_status(&self) -> Result<DreamStatus, String> {
        let result = self.call("dream.status", None)?;
        // Daemon wraps in {success, data: {...}}
        let data = result.get("data").cloned().unwrap_or(result);
        serde_json::from_value(data).map_err(|e| format!("failed to parse dream status: {}", e))
    }

    /// Allocate resources for a dream task.
    fn allocate(
        &self,
        client_id: &str,
        purpose: &str,
        gpu_count: u32,
        ram_mib: u64,
        ttl_secs: u64,
    ) -> Result<serde_json::Value, String> {
        let params = serde_json::json!({
            "client_id": client_id,
            "purpose": purpose,
            "gpu_count": gpu_count,
            "ram_mib": ram_mib,
            "ttl_secs": ttl_secs,
        });
        self.call("resources.allocate", Some(params))
    }

    /// Release an allocation.
    fn release(&self, allocation_id: &str, client_id: &str) -> Result<(), String> {
        let params = serde_json::json!({
            "allocation_id": allocation_id,
            "client_id": client_id,
        });
        self.call("resources.release", Some(params))?;
        Ok(())
    }

    /// Report dream task completion.
    fn dream_report(&self, report: &serde_json::Value) -> Result<(), String> {
        self.call("dream.report", Some(report.clone()))?;
        Ok(())
    }

    /// Register a dream task with the daemon.
    fn register_task(&self, task_info: &serde_json::Value) -> Result<(), String> {
        self.call("dream.register_task", Some(task_info.clone()))?;
        Ok(())
    }
}

/// Load all tasks from the queue directory, sorted by priority (lower = higher priority).
fn load_tasks(queue_dir: &Path) -> Vec<(PathBuf, DreamTask)> {
    let mut tasks = Vec::new();
    if let Ok(entries) = std::fs::read_dir(queue_dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) == Some("json") {
                if let Ok(content) = std::fs::read_to_string(&path) {
                    if let Ok(task) = serde_json::from_str::<DreamTask>(&content) {
                        tasks.push((path, task));
                    } else {
                        eprintln!(
                            "warning: failed to parse task file {}: invalid JSON",
                            path.display()
                        );
                    }
                }
            }
        }
    }
    tasks.sort_by_key(|(_, t)| t.priority);
    tasks
}

/// Move a completed task file to the completed subdirectory.
fn archive_task(task_path: &Path, queue_dir: &Path) {
    let completed_dir = queue_dir.join("completed");
    std::fs::create_dir_all(&completed_dir).ok();
    let filename = task_path.file_name().unwrap_or_default();
    let dest = completed_dir.join(filename);
    if std::fs::rename(task_path, &dest).is_err() {
        // Fallback: copy and delete
        if std::fs::copy(task_path, &dest).is_ok() {
            std::fs::remove_file(task_path).ok();
        }
    }
}

fn main() {
    let args = Args::parse();

    // Expand ~ in paths
    let queue_dir = expand_tilde(&args.queue_dir);
    let socket_path = expand_tilde(&args.socket);

    std::fs::create_dir_all(&queue_dir).ok();

    let rpc = RiodRpc::new(&socket_path);

    // Verify we can connect to riod
    match rpc.dream_status() {
        Ok(status) => println!("connected to riod, dream state: {}", status.state),
        Err(e) => {
            eprintln!(
                "error: cannot connect to riod at {}: {}",
                socket_path.display(),
                e
            );
            std::process::exit(1);
        }
    }

    let poll_duration = Duration::from_secs(args.poll_interval);
    let mut last_state = String::new();

    loop {
        // Poll dream status
        let status = match rpc.dream_status() {
            Ok(s) => s,
            Err(e) => {
                eprintln!("warning: failed to query dream status: {}", e);
                std::thread::sleep(poll_duration);
                continue;
            }
        };

        println!(
            "dream status: state={}, cycle={}",
            status.state,
            status.cycle_id.as_deref().unwrap_or("none")
        );

        // Detect transition to triggered state
        if status.state == "triggered" && last_state != "triggered" {
            let cycle_id = match &status.cycle_id {
                Some(id) => id.clone(),
                None => {
                    eprintln!("error: dream triggered but no cycle_id in response");
                    std::thread::sleep(poll_duration);
                    continue;
                }
            };

            println!("dream cycle triggered: {}", cycle_id);

            // Load and process queued tasks
            let tasks = load_tasks(&queue_dir);
            if tasks.is_empty() {
                println!("no queued tasks, nothing to do");
            } else {
                for (task_path, task) in tasks {
                    println!(
                        "processing task: {} (priority {})",
                        task.name, task.priority
                    );

                    // Execute the task
                    let result = execute_task(&rpc, &task, &cycle_id);

                    match result {
                        Ok(report) => {
                            println!("task completed: {}", task.name);
                            if let Err(e) = rpc.dream_report(&report) {
                                eprintln!("warning: failed to report dream result: {}", e);
                            }
                            archive_task(&task_path, &queue_dir);
                        }
                        Err(e) => {
                            eprintln!("error executing task {}: {}", task.name, e);
                            // Don't archive failed tasks — they stay in queue for retry
                            break;
                        }
                    }

                    if args.once {
                        break;
                    }
                }
            }

            if args.once {
                println!("--once flag set, exiting after processing");
                break;
            }
        }

        last_state = status.state.clone();
        std::thread::sleep(poll_duration);
    }
}

/// Execute a single dream task: allocate resources, run command, report.
fn execute_task(
    rpc: &RiodRpc,
    task: &DreamTask,
    cycle_id: &str,
) -> Result<serde_json::Value, String> {
    let client_id = format!("dream-consumer:{}", task.name);
    let ttl_secs = task.estimated_duration_mins * 60 + 300; // Add 5 min buffer

    println!(
        "allocating resources for {} (TTL {}s)",
        task.name, ttl_secs
    );
    let alloc_result = rpc.allocate(
        &client_id,
        "dream",
        if task.requires_gpu { 1 } else { 0 },
        2048, // Default 2GB RAM for dream tasks
        ttl_secs,
    )?;

    let allocation_id = alloc_result
        .get("data")
        .and_then(|d| d.get("allocation"))
        .and_then(|a| a.get("id"))
        .and_then(|v| v.as_str())
        .ok_or_else(|| "allocation response missing id".to_string())?
        .to_string();

    println!("allocated: {}", allocation_id);

    // Build the command to execute
    let workdir = expand_tilde(&task.workdir);
    let mut cmd = Command::new("bash");
    cmd.arg("-c")
        .arg(&task.command)
        .current_dir(&workdir)
        .env("RIOD_DREAM_CYCLE", cycle_id)
        .env("RIOD_ALLOCATION_ID", &allocation_id);

    if let Some(context_file) = &task.context_file {
        let full_context = PathBuf::from(&workdir).join(context_file);
        if full_context.exists() {
            cmd.env("RIOD_CONTEXT_FILE", full_context.to_str().unwrap_or(""));
        }
    }

    println!(
        "executing: {} (in {})",
        task.command,
        workdir.display()
    );
    let output = cmd
        .output()
        .map_err(|e| format!("failed to execute command: {}", e))?;

    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    let success = output.status.success();

    println!(
        "task finished: exit={}, stdout_len={}, stderr_len={}",
        output.status.code().unwrap_or(-1),
        stdout.len(),
        stderr.len()
    );

    // Release allocation
    if let Err(e) = rpc.release(&allocation_id, &client_id) {
        eprintln!(
            "warning: failed to release allocation {}: {}",
            allocation_id, e
        );
    }

    // Build report (truncate large outputs)
    let stdout_trunc: String = if stdout.len() > 10000 {
        stdout.chars().take(10000).collect()
    } else {
        stdout.clone()
    };
    let stderr_trunc: String = if stderr.len() > 5000 {
        stderr.chars().take(5000).collect()
    } else {
        stderr.clone()
    };

    let report = serde_json::json!({
        "task_name": task.name,
        "cycle_id": cycle_id,
        "success": success,
        "stdout": stdout_trunc,
        "stderr": stderr_trunc,
        "observations": [],
    });

    Ok(report)
}

/// Expand ~ to the user's home directory.
fn expand_tilde(path: &str) -> PathBuf {
    if let Some(rest) = path.strip_prefix('~') {
        if let Ok(home) = std::env::var("HOME") {
            return PathBuf::from(home).join(rest);
        }
    }
    PathBuf::from(path)
}
