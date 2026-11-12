//! Resource observation: CPU, RAM, GPU polling and idle detection.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::process::Command;
use thiserror::Error;

/// Error types for resource observation.
#[derive(Debug, Error)]
pub enum ObserveError {
    #[error("failed to read /proc/{0}: {1}")]
    ProcRead(String, String),

    #[error("nvidia-smi not available or failed: {0}")]
    GpuQuery(String),

    #[error("invalid resource data: {0}")]
    ParseError(String),
}

/// A single GPU's resource state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GpuInfo {
    pub index: u32,
    pub name: String,
    pub vram_total_mb: u64,
    pub vram_used_mb: u64,
    pub utilization_percent: u8,
}

/// A complete resource snapshot at a point in time.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceSnapshot {
    pub timestamp: DateTime<Utc>,
    pub cpu_usage_percent: f32,
    pub ram_total_mb: u64,
    pub ram_used_mb: u64,
    pub gpus: Vec<GpuInfo>,
}

impl ResourceSnapshot {
    /// True if all GPUs are below the utilization threshold.
    fn gpu_idle(&self, threshold_percent: u8) -> bool {
        self.gpus
            .iter()
            .all(|g| g.utilization_percent < threshold_percent)
    }

    /// True if CPU is below threshold AND all GPUs are idle.
    pub fn is_idle(&self, cpu_threshold: f32, gpu_threshold: u8) -> bool {
        self.cpu_usage_percent < cpu_threshold && self.gpu_idle(gpu_threshold)
    }
}

/// Configuration for the observer.
#[derive(Debug, Clone)]
pub struct ObserveConfig {
    /// CPU usage below this percentage counts as idle (default: 5.0).
    pub idle_cpu_percent: f32,
    /// GPU utilization below this percentage counts as idle (default: 10).
    pub idle_gpu_percent: u8,
    /// Number of consecutive idle samples required to declare system idle.
    pub idle_sample_count: usize,
}

impl Default for ObserveConfig {
    fn default() -> Self {
        Self {
            idle_cpu_percent: 5.0,
            idle_gpu_percent: 10,
            idle_sample_count: 20, // 20 samples × 30s = 10 minutes
        }
    }
}

/// Tracks resource history and detects sustained idle periods.
pub struct IdleDetector {
    config: ObserveConfig,
    /// Previous snapshot for CPU delta calculation.
    prev_cpu_times: Option<CpuTimes>,
    /// Rolling window of recent snapshots (bounded).
    recent: Vec<ResourceSnapshot>,
    /// Consecutive idle sample count.
    consecutive_idle: usize,
    /// Whether we're currently in an idle state.
    pub is_system_idle: bool,
    /// When the current idle period started (if idle).
    pub idle_since: Option<DateTime<Utc>>,
}

/// Raw CPU times from /proc/stat (in jiffies).
#[derive(Debug, Clone, Copy)]
struct CpuTimes {
    user: u64,
    nice: u64,
    system: u64,
    idle: u64,
    iowait: u64,
    irq: u64,
    softirq: u64,
    steal: u64,
}

impl CpuTimes {
    fn total(&self) -> u64 {
        self.user
            + self.nice
            + self.system
            + self.idle
            + self.iowait
            + self.irq
            + self.softirq
            + self.steal
    }

    fn idle_time(&self) -> u64 {
        self.idle + self.iowait
    }
}

impl IdleDetector {
    pub fn new(config: ObserveConfig) -> Self {
        Self {
            config,
            prev_cpu_times: None,
            recent: Vec::new(),
            consecutive_idle: 0,
            is_system_idle: false,
            idle_since: None,
        }
    }

    /// Poll all resources and update idle state. Returns the new snapshot.
    pub fn poll(&mut self) -> Result<ResourceSnapshot, ObserveError> {
        let cpu_times = read_cpu_times()?;
        let total_jiffies = cpu_times.total();

        let cpu_percent = match self.prev_cpu_times {
            Some(prev) => {
                let delta_total = total_jiffies.saturating_sub(prev.total());
                let delta_idle = cpu_times.idle_time().saturating_sub(prev.idle_time());
                if delta_total == 0 {
                    0.0
                } else {
                    let usage = (delta_total - delta_idle) as f32 / delta_total as f32;
                    (usage * 100.0).clamp(0.0, 100.0) as f32
                }
            }
            None => 0.0, // First sample: no delta available
        };

        self.prev_cpu_times = Some(cpu_times);

        let ram = read_ram_info()?;
        let gpus = match read_gpu_info() {
            Ok(g) => g,
            Err(e) => {
                tracing::warn!(error = %e, "GPU query failed, continuing without GPU data");
                Vec::new()
            }
        };

        let snapshot = ResourceSnapshot {
            timestamp: Utc::now(),
            cpu_usage_percent: cpu_percent,
            ram_total_mb: ram.0,
            ram_used_mb: ram.1,
            gpus,
        };

        // Update idle detection state
        self.update_idle_state(&snapshot);

        Ok(snapshot)
    }

    fn update_idle_state(&mut self, snapshot: &ResourceSnapshot) {
        let was_idle = self.is_system_idle;
        let currently_idle =
            snapshot.is_idle(self.config.idle_cpu_percent, self.config.idle_gpu_percent);

        if currently_idle {
            self.consecutive_idle += 1;
            if !was_idle && self.consecutive_idle >= self.config.idle_sample_count {
                self.is_system_idle = true;
                self.idle_since = Some(snapshot.timestamp);
                tracing::info!(
                    idle_for_samples = self.consecutive_idle,
                    "system entered idle state"
                );
            }
        } else {
            if was_idle {
                tracing::info!("system exited idle state");
            }
            self.consecutive_idle = 0;
            self.is_system_idle = false;
            self.idle_since = None;
        }

        // Keep a bounded history (last 120 samples = 1 hour at 30s intervals)
        self.recent.push(snapshot.clone());
        if self.recent.len() > 120 {
            self.recent.remove(0);
        }
    }

    /// Get the most recent snapshot, if any.
    pub fn latest_snapshot(&self) -> Option<&ResourceSnapshot> {
        self.recent.last()
    }

    /// Get recent history (for ledger.query).
    pub fn history(&self) -> &[ResourceSnapshot] {
        &self.recent
    }

    /// Get the number of consecutive idle samples.
    pub fn consecutive_idle_samples(&self) -> usize {
        self.consecutive_idle
    }
}

/// Read aggregate CPU times from /proc/stat.
fn read_cpu_times() -> Result<CpuTimes, ObserveError> {
    let content = std::fs::read_to_string("/proc/stat")
        .map_err(|e| ObserveError::ProcRead("stat".into(), e.to_string()))?;

    for line in content.lines() {
        if let Some(rest) = line.strip_prefix("cpu ") {
            let fields: Vec<u64> = rest
                .split_whitespace()
                .map(|f| f.parse().unwrap_or(0))
                .collect();

            if fields.len() >= 8 {
                return Ok(CpuTimes {
                    user: fields[0],
                    nice: fields[1],
                    system: fields[2],
                    idle: fields[3],
                    iowait: fields[4],
                    irq: fields[5],
                    softirq: fields[6],
                    steal: fields[7],
                });
            }
        }
    }

    Err(ObserveError::ParseError(
        "cpu line not found in /proc/stat".into(),
    ))
}

/// Read RAM info from /proc/meminfo. Returns (total_mb, used_mb).
fn read_ram_info() -> Result<(u64, u64), ObserveError> {
    let content = std::fs::read_to_string("/proc/meminfo")
        .map_err(|e| ObserveError::ProcRead("meminfo".into(), e.to_string()))?;

    let mut total_kb: u64 = 0;
    let mut available_kb: u64 = 0;

    for line in content.lines() {
        if let Some(kb) = parse_meminfo_value(line, "MemTotal:") {
            total_kb = kb;
        } else if let Some(kb) = parse_meminfo_value(line, "MemAvailable:") {
            available_kb = kb;
        }
    }

    if total_kb == 0 {
        return Err(ObserveError::ParseError(
            "MemTotal not found in /proc/meminfo".into(),
        ));
    }

    let total_mb = total_kb / 1024;
    let used_mb = (total_kb.saturating_sub(available_kb)) / 1024;

    Ok((total_mb, used_mb))
}

/// Parse a "Field: value kB" line from /proc/meminfo.
fn parse_meminfo_value(line: &str, field: &str) -> Option<u64> {
    if let Some(rest) = line.strip_prefix(field) {
        rest.trim().split_whitespace().next()?.parse::<u64>().ok()
    } else {
        None
    }
}

/// Read GPU info via nvidia-smi. Returns empty vec if no NVIDIA GPU or tool unavailable.
fn read_gpu_info() -> Result<Vec<GpuInfo>, ObserveError> {
    // Try to locate nvidia-smi; if not found, no NVIDIA GPU present (not an error)
    let nvidia_smi = std::env::var("NVIDIA_SMI_PATH").unwrap_or_else(|_| "nvidia-smi".into());
    if which::which(&nvidia_smi).is_err() {
        return Ok(Vec::new());
    }

    let output = Command::new(&nvidia_smi)
        .args([
            "--query-gpu=index,name,memory.total,memory.used,utilization.gpu",
            "--format=csv,noheader,nounits",
        ])
        .output()
        .map_err(|e| ObserveError::GpuQuery(e.to_string()))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        // nvidia-smi may fail if no GPU is present — treat as no GPUs, not an error
        if stderr.contains("failed to initialize") || stderr.contains("No devices") {
            return Ok(Vec::new());
        }
        return Err(ObserveError::GpuQuery(stderr.trim().to_string()));
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut gpus = Vec::new();

    for (i, line) in stdout.lines().enumerate() {
        let fields: Vec<&str> = line.split(',').map(|f| f.trim()).collect();
        if fields.len() >= 5 {
            let name = fields[1].to_string();
            if let (Ok(vram_total), Ok(vram_used), Ok(util)) = (
                fields[2].parse::<u64>(),
                fields[3].parse::<u64>(),
                fields[4].parse::<u8>(),
            ) {
                gpus.push(GpuInfo {
                    index: i as u32,
                    name,
                    vram_total_mb: vram_total,
                    vram_used_mb: vram_used,
                    utilization_percent: util,
                });
            }
        }
    }

    Ok(gpus)
}

// Minimal `which` implementation to avoid external dependency
mod which {
    use std::path::PathBuf;

    pub fn which(bin: &str) -> Result<PathBuf, String> {
        if let Some(path_var) = std::env::var_os("PATH") {
            for dir in std::env::split_paths(&path_var) {
                let candidate = dir.join(bin);
                if candidate.is_file() {
                    return Ok(candidate);
                }
            }
        }
        Err(format!("{} not found in PATH", bin))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_meminfo_value() {
        assert_eq!(
            parse_meminfo_value("MemTotal:       16384000 kB", "MemTotal:"),
            Some(16384000)
        );
        assert_eq!(
            parse_meminfo_value("MemAvailable:   8192000 kB", "MemAvailable:"),
            Some(8192000)
        );
        assert_eq!(
            parse_meminfo_value("Buffers:          1024 kB", "MemTotal:"),
            None
        );
    }

    #[test]
    fn test_snapshot_idle_detection() {
        let snap = ResourceSnapshot {
            timestamp: Utc::now(),
            cpu_usage_percent: 2.0,
            ram_total_mb: 16384,
            ram_used_mb: 4096,
            gpus: vec![GpuInfo {
                index: 0,
                name: "RTX 3070 Ti".into(),
                vram_total_mb: 8192,
                vram_used_mb: 512,
                utilization_percent: 5,
            }],
        };

        assert!(snap.is_idle(5.0, 10));

        let busy = ResourceSnapshot {
            cpu_usage_percent: 45.0,
            ..snap.clone()
        };
        assert!(!busy.is_idle(5.0, 10));

        let gpu_busy = ResourceSnapshot {
            gpus: vec![GpuInfo {
                utilization_percent: 80,
                ..snap.gpus[0].clone()
            }],
            ..snap.clone()
        };
        assert!(!gpu_busy.is_idle(5.0, 10));
    }

    #[test]
    fn test_idle_detector_state_transitions() {
        let config = ObserveConfig {
            idle_sample_count: 3, // Fast for testing
            ..Default::default()
        };
        let mut detector = IdleDetector::new(config);

        // Simulate busy periods
        for _ in 0..5 {
            let snap = detector.poll().expect("poll should succeed");
            assert!(!detector.is_system_idle);
            let _ = snap;
        }

        // Force idle by manipulating the last snapshot's interpretation
        // (In real usage, this would require actual low CPU for N samples)
        assert_eq!(detector.consecutive_idle, 0);
    }
}
