//! Types shared between riod-client and the riod daemon wire format.
//!
//! These structs mirror the JSON-RPC response shapes from the riod daemon.
//! They are intentionally separate from the daemon's internal types to keep
//! the client library decoupled from daemon implementation details.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Information about a single GPU on the system.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GpuInfo {
    pub index: u32,
    pub name: String,
    pub vram_total_mb: u64,
    pub vram_used_mb: u64,
    pub utilization_percent: u8,
}

/// A request for resources from a client.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AllocateRequest {
    /// Unique identifier for the requesting client (e.g., "pesti-ftw3", "crabjar-worker-7").
    pub client_id: String,
    /// Purpose of the allocation (e.g., "training", "inference", "dreaming").
    pub purpose: String,
    /// Specific GPU indices to allocate. If None, auto-assign based on gpu_count.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gpus: Option<Vec<u32>>,
    /// Number of GPUs to allocate if `gpus` is None. Defaults to 1.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gpu_count: Option<usize>,
    /// Maximum CPU percentage this allocation may use (advisory).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cpu_percent: Option<f32>,
    /// Maximum RAM in MB this allocation may use (advisory).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ram_mb: Option<u64>,
    /// Time-to-live in seconds. Client must heartbeat within this window.
    /// Defaults to 300s if not specified.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ttl_secs: Option<u64>,
}

impl AllocateRequest {
    /// Create a new allocation request with minimal fields.
    pub fn new(client_id: impl Into<String>, purpose: impl Into<String>) -> Self {
        Self {
            client_id: client_id.into(),
            purpose: purpose.into(),
            gpus: None,
            gpu_count: None,
            cpu_percent: None,
            ram_mb: None,
            ttl_secs: None,
        }
    }

    /// Request specific GPU indices.
    pub fn with_gpus(mut self, gpus: Vec<u32>) -> Self {
        self.gpus = Some(gpus);
        self
    }

    /// Request N GPUs (auto-assigned).
    pub fn with_gpu_count(mut self, count: usize) -> Self {
        self.gpu_count = Some(count);
        self
    }

    /// Set maximum CPU percentage.
    pub fn with_cpu_percent(mut self, percent: f32) -> Self {
        self.cpu_percent = Some(percent);
        self
    }

    /// Set maximum RAM in MB.
    pub fn with_ram_mb(mut self, mb: u64) -> Self {
        self.ram_mb = Some(mb);
        self
    }

    /// Set time-to-live in seconds.
    pub fn with_ttl(mut self, secs: u64) -> Self {
        self.ttl_secs = Some(secs);
        self
    }
}

/// An active resource allocation granted by the broker.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Allocation {
    /// Unique allocation identifier (e.g., "alloc-0x00000001").
    pub id: String,
    /// Client that owns this allocation.
    pub client_id: String,
    /// Purpose of the allocation.
    pub purpose: String,
    /// GPU indices allocated to this client.
    pub gpus: Vec<u32>,
    /// Maximum CPU percentage (advisory).
    pub cpu_percent: f32,
    /// Maximum RAM in MB (advisory).
    pub ram_mb: u64,
    /// When the allocation was granted.
    pub granted_at: DateTime<Utc>,
    /// When the allocation expires without a heartbeat.
    pub expires_at: DateTime<Utc>,
    /// Last time the client sent a heartbeat.
    pub last_heartbeat: DateTime<Utc>,
    /// Cgroup path for resource enforcement (set when cgroup mode is enabled).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cgroup_path: Option<String>,
}

/// Current availability of resources on the system.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Availability {
    /// GPU indices that are currently free.
    pub available_gpus: Vec<u32>,
    /// Total number of GPUs on the system.
    pub total_gpus: usize,
    /// Number of GPUs currently allocated.
    pub allocated_gpus: usize,
    /// Total system RAM in MB.
    pub total_ram_mb: u64,
    /// Currently allocated RAM in MB.
    pub allocated_ram_mb: u64,
    /// Available RAM in MB.
    pub available_ram_mb: u64,
    /// Number of active allocations.
    pub active_allocations: usize,
}

/// Result of releasing an allocation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReleaseResult {
    /// The allocation ID that was released.
    pub released: String,
    /// GPU indices that were freed.
    pub gpus_freed: Vec<u32>,
}

/// Current system status (CPU, RAM, GPU).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemStatus {
    pub timestamp: DateTime<Utc>,
    pub cpu_usage_percent: f32,
    pub ram_total_mb: u64,
    pub ram_used_mb: u64,
    pub gpus: Vec<GpuInfo>,
}

/// A single event from the riod event ledger.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LedgerEntry {
    pub id: String,
    pub timestamp: DateTime<Utc>,
    #[serde(flatten)]
    pub event: serde_json::Value,
}

/// Statistics about the event ledger.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LedgerStats {
    pub total_entries: u64,
    pub oldest_timestamp: Option<DateTime<Utc>>,
    pub newest_timestamp: Option<DateTime<Utc>>,
    pub file_size_bytes: u64,
}
