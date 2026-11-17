//! Resource broker: allocation requests, GPU slot management, heartbeat tracking.
//!
//! Phase 1 of rIOd. Clients request resources (GPU slots, CPU, RAM) via the
//! JSON-RPC API. The broker tracks allocations, enforces capacity limits, and
//! automatically releases stale allocations when heartbeats stop arriving.
//!
//! Design: coordinates but never owns lifecycle. The broker grants permission
//! to use resources; it does not start or stop processes.

use crate::cgroup::{CgroupError, CgroupManager};
use crate::ledger::{EventLedger, LedgerEvent};
use crate::observe::{GpuInfo, ResourceSnapshot};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use thiserror::Error;

/// Errors from the resource broker.
#[derive(Debug, Error)]
pub enum BrokerError {
    #[error("Insufficient resources: {0}")]
    InsufficientResources(String),

    #[error("GPU {index} already allocated to allocation {allocation_id}")]
    GpuAlreadyAllocated { index: u32, allocation_id: String },

    #[error("Allocation not found: {0}")]
    AllocationNotFound(String),

    #[error("Client {client_id} does not own allocation {allocation_id}")]
    NotAllocationOwner {
        client_id: String,
        allocation_id: String,
    },

    #[error("Invalid request: {0}")]
    InvalidRequest(String),

    #[error("Cgroup enforcement is not enabled on this daemon")]
    CgroupNotEnabled,

    #[error("Cgroup operation failed: {0}")]
    CgroupError(String),

    #[error("Ledger error: {0}")]
    Ledger(#[from] crate::ledger::LedgerError),
}

/// A request for resources from a client.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AllocationRequest {
    /// Unique identifier for the requesting client (e.g., "pesti-ftw3", "crabjar-worker-7").
    pub client_id: String,
    /// Purpose of the allocation (e.g., "training", "inference", "dreaming", "batch-processing").
    pub purpose: String,
    /// Specific GPU indices to allocate. If None, auto-assign based on gpu_count.
    pub gpus: Option<Vec<u32>>,
    /// Number of GPUs to allocate if `gpus` is None. Defaults to 1.
    pub gpu_count: Option<usize>,
    /// Maximum CPU percentage this allocation may use (advisory in Phase 1).
    #[serde(default)]
    pub cpu_percent: Option<f32>,
    /// Maximum RAM in MB this allocation may use (advisory in Phase 1).
    #[serde(default)]
    pub ram_mb: Option<u64>,
    /// Time-to-live in seconds. Client must heartbeat within this window or the
    /// allocation is automatically released. Defaults to 300s (5 minutes).
    #[serde(default = "default_ttl")]
    pub ttl_secs: u64,
    /// Optional intent ID from `intent.declare`. When provided and policies are
    /// loaded, the intent must be Granted and not expired for allocation to proceed.
    /// The intent is marked Consumed on successful allocation.
    #[serde(default)]
    pub intent_id: Option<String>,
    /// Optional dream cycle ID. When a dream cycle is preempted or completed,
    /// all allocations tagged with this cycle ID are automatically released.
    #[serde(default)]
    pub dream_cycle_id: Option<String>,
}

fn default_ttl() -> u64 {
    300
}

/// An active resource allocation tracked by the broker.
///
/// This is a **projection** of ledger events, not an independent source of truth.
/// On daemon restart, these are rebuilt from the event log via
/// [`crate::projection::rehydrate_allocations`]. The fields here mirror what can be
/// derived from `AllocationGranted`, `AllocationHeartbeat`, and terminal events.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Allocation {
    /// Unique allocation identifier.
    pub id: String,
    /// Client that owns this allocation.
    pub client_id: String,
    /// Purpose of the allocation.
    pub purpose: String,
    /// GPU indices allocated to this client.
    pub gpus: Vec<u32>,
    /// Maximum CPU percentage (advisory). 100.0 = full machine access.
    #[serde(default = "default_cpu_percent")]
    pub cpu_percent: f32,
    /// Maximum RAM in MB (advisory).
    pub ram_mb: u64,
    /// Time-to-live in seconds. Each heartbeat resets the expiry clock
    /// (sliding window). Stored so rehydration can reconstruct expiry.
    pub ttl_secs: u64,
    /// When the allocation was granted.
    pub granted_at: DateTime<Utc>,
    /// When the allocation expires without a heartbeat.
    pub expires_at: DateTime<Utc>,
    /// Last time the client sent a heartbeat.
    pub last_heartbeat: DateTime<Utc>,
    /// Cgroup path for resource enforcement (set when cgroup mode is enabled).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cgroup_path: Option<String>,
    /// Dream cycle ID if this allocation was made during a dream cycle.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dream_cycle_id: Option<String>,
    /// Set when the allocation reaches a terminal state (released/expired).
    /// `None` means still active from rIOd's perspective.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end_reason: Option<String>,
    /// Timestamp of the last observed resource activity for this allocation.
    /// Updated on heartbeat and when the broker detects GPU utilization above
    /// the idle threshold for this allocation's GPUs. Used to determine if an
    /// allocation is "dormant" (allocated but not doing work) and therefore
    /// preemptible by new requests. `None` means activity has never been observed
    /// since allocation (or the field was added after the allocation was created).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_activity_at: Option<DateTime<Utc>>,
}

/// Default CPU percentage for backward-compatible deserialization.
fn default_cpu_percent() -> f32 {
    100.0
}

/// Current state of the resource pool and active allocations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolStatus {
    /// All GPUs visible on this machine.
    pub total_gpus: Vec<GpuInfo>,
    /// Map of GPU index -> allocation ID (only allocated GPUs appear here).
    pub allocated_gpus: HashMap<u32, String>,
    /// Total system RAM in MB.
    pub total_ram_mb: u64,
    /// Currently allocated RAM in MB (sum of all active allocations).
    pub allocated_ram_mb: u64,
    /// Number of active allocations.
    pub active_allocations: usize,
}

/// Thresholds for determining if a resource is "effectively available"
/// considering both riod's own allocations and actual system utilization.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AvailabilityThresholds {
    /// Minimum free VRAM (MB) a GPU must have to be considered available.
    /// Default: 1024 MB (1 GB).
    pub min_free_vram_mb: u64,
    /// Maximum GPU utilization (%) for a GPU to be considered available.
    /// Default: 80%.
    pub max_gpu_utilization_percent: u8,
    /// Minimum free RAM (MB) that must remain after accounting for both
    /// system usage and riod allocations. Default: 2048 MB (2 GB).
    pub min_free_ram_mb: u64,
    /// GPU utilization (%) below which an allocation's GPU is considered idle.
    /// Used to determine if an allocation is "dormant" (allocated but not doing work).
    /// Default: 5%.
    #[serde(default = "default_idle_util_threshold")]
    pub idle_utilization_threshold: u8,
    /// Seconds of inactivity before a dormant allocation becomes preemptible.
    /// An allocation whose GPUs have all been below `idle_utilization_threshold`
    /// for this many seconds can be released to make room for new requests.
    /// Default: 600 (10 minutes).
    #[serde(default = "default_dormant_timeout")]
    pub dormant_timeout_secs: u64,
}

fn default_idle_util_threshold() -> u8 {
    5
}

fn default_dormant_timeout() -> u64 {
    600
}

impl Default for AvailabilityThresholds {
    fn default() -> Self {
        Self {
            min_free_vram_mb: 1024,
            max_gpu_utilization_percent: 80,
            min_free_ram_mb: 2048,
            idle_utilization_threshold: default_idle_util_threshold(),
            dormant_timeout_secs: default_dormant_timeout(),
        }
    }
}

/// Utilization-aware availability report. Combines riod's allocation ledger
/// with actual system resource usage to give a true picture of what can be
/// allocated without OOM or contention.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EffectiveAvailability {
    /// GPUs that are both unallocated by riod AND have sufficient free VRAM
    /// and utilization headroom based on the latest system snapshot.
    pub available_gpus: Vec<u32>,
    /// Per-GPU availability details (why each GPU is or isn't available).
    pub gpu_details: Vec<GpuAvailability>,
    /// Total GPUs visible on this machine.
    pub total_gpus: usize,
    /// GPUs allocated by riod (regardless of actual utilization).
    pub riod_allocated_gpus: Vec<u32>,
    /// RAM available after accounting for BOTH system usage and riod allocations.
    pub available_ram_mb: u64,
    /// Total system RAM.
    pub total_ram_mb: u64,
    /// RAM currently in use by the system (all processes, from /proc/meminfo).
    pub system_used_ram_mb: u64,
    /// RAM allocated through riod's broker.
    pub riod_allocated_ram_mb: u64,
    /// Number of active allocations in riod's ledger.
    pub active_allocations: usize,
    /// Whether this report is based on fresh data (snapshot < 2x poll interval old).
    pub data_fresh: bool,
}

/// Detailed availability status for a single GPU.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GpuAvailability {
    pub index: u32,
    pub name: String,
    /// True if this GPU is allocatable right now.
    pub available: bool,
    /// Reason why unavailable (None if available).
    pub reason: Option<String>,
    /// Allocated by riod to this allocation ID (if any).
    pub riod_allocation: Option<String>,
    /// Free VRAM in MB (total - system used).
    pub free_vram_mb: u64,
    /// Current utilization percentage.
    pub utilization_percent: u8,
}

/// The resource broker. Manages allocation state and enforces capacity limits.
///
/// Thread-safe: wrap in `Arc<Mutex<>>` for shared access between the socket
/// server and the heartbeat monitor.
pub struct ResourceBroker {
    /// All known GPUs on this machine (refreshed from observation).
    pub gpus: Vec<GpuInfo>,
    /// Active allocations keyed by allocation ID.
    pub allocations: HashMap<String, Allocation>,
    /// Map of GPU index -> allocation ID for O(1) lookup during allocation.
    pub gpu_to_allocation: HashMap<u32, String>,
    /// Total system RAM in MB (set at startup from observation).
    pub total_ram_mb: u64,
    /// Counter for generating unique allocation IDs.
    next_id: u64,
    /// Optional cgroup manager for resource enforcement.
    cgroups: Option<CgroupManager>,
}

impl ResourceBroker {
    /// Create a new broker with the given hardware inventory.
    pub fn new(gpus: Vec<GpuInfo>, total_ram_mb: u64) -> Self {
        Self {
            gpus,
            allocations: HashMap::new(),
            gpu_to_allocation: HashMap::new(),
            total_ram_mb,
            next_id: 1,
            cgroups: None,
        }
    }

    /// Enable cgroup enforcement. Call before accepting allocations.
    pub fn enable_cgroups(&mut self) {
        self.cgroups = Some(CgroupManager::new(true));
    }

    /// Generate the next allocation ID.
    fn next_allocation_id(&mut self) -> String {
        let id = format!("alloc-{:#010x}", self.next_id);
        self.next_id += 1;
        id
    }

    /// Check which GPUs are currently available (not allocated).
    pub fn available_gpus(&self) -> Vec<u32> {
        self.gpus
            .iter()
            .map(|g| g.index)
            .filter(|idx| !self.gpu_to_allocation.contains_key(idx))
            .collect()
    }

    /// Check how much RAM is currently allocated.
    pub fn allocated_ram(&self) -> u64 {
        self.allocations.values().map(|a| a.ram_mb).sum()
    }

    /// Try to fulfill an allocation request.
    ///
    /// On success, returns the new `Allocation` and records a `LedgerEvent`.
    /// On failure, returns a `BrokerError` explaining why.
    ///
    /// When `snapshot` is provided, this method checks actual GPU utilization
    /// and free VRAM before granting — preventing allocation of GPUs that are
    /// already busy with external work. It also preempts dormant allocations
    /// (allocated but idle beyond `thresholds.dormant_timeout_secs`) to make
    /// room for new requests. This is the core mechanism for "not reserving
    /// resources if no work": riod only holds reservations that correspond to
    /// actual resource usage.
    pub fn allocate(
        &mut self,
        request: &AllocationRequest,
        ledger: &mut EventLedger,
        snapshot: Option<&ResourceSnapshot>,
        thresholds: &AvailabilityThresholds,
    ) -> Result<Allocation, BrokerError> {
        // Validate client_id and purpose are non-empty
        if request.client_id.trim().is_empty() {
            return Err(BrokerError::InvalidRequest(
                "client_id cannot be empty".into(),
            ));
        }
        if request.purpose.trim().is_empty() {
            return Err(BrokerError::InvalidRequest(
                "purpose cannot be empty".into(),
            ));
        }

        // Determine which GPUs to allocate (with utilization-aware checks)
        let gpu_indices = match &request.gpus {
            Some(indices) => {
                // Client requested specific GPUs — verify each one
                for idx in indices {
                    if !self.gpus.iter().any(|g| g.index == *idx) {
                        return Err(BrokerError::InvalidRequest(format!(
                            "GPU {} does not exist on this machine",
                            idx
                        )));
                    }

                    // Check if already allocated by riod — try to preempt if dormant
                    if let Some(existing_alloc_id) = self.gpu_to_allocation.get(idx) {
                        let existing_alloc_id_owned = existing_alloc_id.clone();

                        // Check dormancy (immutable borrow of self)
                        let is_dormant = self
                            .allocations
                            .get(&existing_alloc_id_owned)
                            .map(|a| self.is_allocation_dormant(a, snapshot, thresholds))
                            .unwrap_or(false);

                        if is_dormant {
                            tracing::info!(
                                "Preempting dormant allocation {} for GPU {} — idle beyond {}s threshold",
                                existing_alloc_id_owned,
                                idx,
                                thresholds.dormant_timeout_secs
                            );
                            self.remove_allocation(
                                &existing_alloc_id_owned,
                                ledger,
                                "preempted_dormant",
                            )?;
                        } else {
                            return Err(BrokerError::GpuAlreadyAllocated {
                                index: *idx,
                                allocation_id: existing_alloc_id_owned,
                            });
                        }
                    }

                    // Check actual utilization from snapshot (if available)
                    if let Some(snap) = snapshot {
                        if let Some(snap_gpu) = snap.gpus.iter().find(|g| g.index == *idx) {
                            let free_vram =
                                snap_gpu.vram_total_mb.saturating_sub(snap_gpu.vram_used_mb);
                            if free_vram < thresholds.min_free_vram_mb {
                                return Err(BrokerError::InsufficientResources(format!(
                                    "GPU {} has insufficient free VRAM: {}MB < {}MB required",
                                    idx, free_vram, thresholds.min_free_vram_mb
                                )));
                            }
                            if snap_gpu.utilization_percent
                                >= thresholds.max_gpu_utilization_percent
                            {
                                return Err(BrokerError::InsufficientResources(format!(
                                    "GPU {} utilization too high: {}% >= {}% threshold",
                                    idx,
                                    snap_gpu.utilization_percent,
                                    thresholds.max_gpu_utilization_percent
                                )));
                            }
                        }
                    }
                }
                indices.clone()
            }
            None => {
                // Auto-assign: use utilization-aware availability
                let count = request.gpu_count.unwrap_or(1);
                if count == 0 {
                    return Err(BrokerError::InvalidRequest(
                        "gpu_count must be at least 1".into(),
                    ));
                }

                // First, try to free dormant allocations to make room
                self.preempt_dormant_allocations(snapshot, thresholds, ledger);

                let avail = self.effective_availability(snapshot, thresholds);
                if avail.available_gpus.len() < count {
                    return Err(BrokerError::InsufficientResources(format!(
                        "Requested {} GPUs but only {} available (utilization-aware)",
                        count,
                        avail.available_gpus.len()
                    )));
                }
                avail.available_gpus.into_iter().take(count).collect()
            }
        };

        // Check RAM capacity
        let requested_ram = request.ram_mb.unwrap_or(0);
        if self.allocated_ram() + requested_ram > self.total_ram_mb {
            return Err(BrokerError::InsufficientResources(format!(
                "Requested {}MB RAM but only {}MB available of {}MB total",
                requested_ram,
                self.total_ram_mb.saturating_sub(self.allocated_ram()),
                self.total_ram_mb
            )));
        }

        // Create the allocation
        let now = Utc::now();
        let id = self.next_allocation_id();
        let mut allocation = Allocation {
            id: id.clone(),
            client_id: request.client_id.clone(),
            purpose: request.purpose.clone(),
            gpus: gpu_indices.clone(),
            cpu_percent: request.cpu_percent.unwrap_or(100.0),
            ram_mb: requested_ram,
            ttl_secs: request.ttl_secs,
            granted_at: now,
            expires_at: now + chrono::Duration::seconds(request.ttl_secs as i64),
            last_heartbeat: now,
            cgroup_path: None,
            dream_cycle_id: request.dream_cycle_id.clone(),
            end_reason: None,
            last_activity_at: Some(now), // Allocation time counts as activity
        };

        // Create cgroup for enforcement (if enabled)
        if let Some(cgroups) = &self.cgroups {
            match cgroups.create_for_allocation(&allocation, request) {
                Ok(path) => {
                    allocation.cgroup_path = Some(path.to_string_lossy().to_string());
                }
                Err(CgroupError::Disabled) => {} // Advisory mode — skip.
                Err(e) => {
                    tracing::warn!("cgroup creation failed for {}: {}", id, e);
                    // Non-fatal: allocation proceeds without enforcement.
                }
            }
        }

        // Record GPU mappings
        for idx in &gpu_indices {
            self.gpu_to_allocation.insert(*idx, id.clone());
        }

        // Store the allocation
        self.allocations.insert(id.clone(), allocation.clone());

        // Log to ledger
        let _ = ledger.append(LedgerEvent::AllocationGranted {
            allocation_id: id.clone(),
            client_id: request.client_id.clone(),
            purpose: request.purpose.clone(),
            gpus: gpu_indices,
            ram_mb: requested_ram,
            ttl_secs: request.ttl_secs,
            cpu_percent: request.cpu_percent.unwrap_or(100.0),
        });

        Ok(allocation)
    }

    /// Check if an allocation is dormant (allocated but not actively using resources).
    /// An allocation is dormant if:
    /// 1. It has GPUs, AND
    /// 2. All its GPUs show utilization below `idle_utilization_threshold` in the latest snapshot, AND
    /// 3. No observed activity for at least `dormant_timeout_secs` seconds
    pub fn is_allocation_dormant(
        &self,
        allocation: &Allocation,
        snapshot: Option<&ResourceSnapshot>,
        thresholds: &AvailabilityThresholds,
    ) -> bool {
        if allocation.gpus.is_empty() {
            return false; // CPU-only allocations can't be observed as dormant via GPU util
        }

        let now = Utc::now();

        // Check last activity timestamp
        match allocation.last_activity_at {
            Some(last_active) => {
                if (now - last_active).num_seconds() < thresholds.dormant_timeout_secs as i64 {
                    return false; // Recently active
                }
            }
            None => {
                // No activity observed — check if it's been allocated long enough to matter
                if (now - allocation.granted_at).num_seconds()
                    < thresholds.dormant_timeout_secs as i64
                {
                    return false; // Too new to be dormant
                }
            }
        }

        // Check current GPU utilization for all GPUs in this allocation
        if let Some(snap) = snapshot {
            for gpu_idx in &allocation.gpus {
                if let Some(snap_gpu) = snap.gpus.iter().find(|g| g.index == *gpu_idx) {
                    if snap_gpu.utilization_percent >= thresholds.idle_utilization_threshold {
                        return false; // At least one GPU is actively used
                    }
                }
            }
        }

        true
    }

    /// Scan all active allocations and preempt (release) any that are dormant.
    /// Called during auto-assignment in `allocate()` to free up resources for new requests.
    /// Returns the number of allocations preempted.
    pub fn preempt_dormant_allocations(
        &mut self,
        snapshot: Option<&ResourceSnapshot>,
        thresholds: &AvailabilityThresholds,
        ledger: &mut EventLedger,
    ) -> usize {
        // Collect dormant allocation IDs first (can't mutate while iterating)
        let dormant_ids: Vec<String> = self
            .allocations
            .iter()
            .filter(|(_, a)| a.end_reason.is_none())
            .filter(|(_, a)| self.is_allocation_dormant(a, snapshot, thresholds))
            .map(|(id, _)| id.clone())
            .collect();

        let mut count = 0;
        for id in dormant_ids {
            if self
                .remove_allocation(&id, ledger, "preempted_dormant")
                .is_ok()
            {
                tracing::info!("Preempted dormant allocation {}", id);
                count += 1;
            }
        }
        count
    }

    /// Release an allocation. Only the owning client can release it.
    pub fn release(
        &mut self,
        allocation_id: &str,
        client_id: &str,
        ledger: &mut EventLedger,
    ) -> Result<Allocation, BrokerError> {
        let allocation = self
            .allocations
            .get(allocation_id)
            .ok_or(BrokerError::AllocationNotFound(allocation_id.to_string()))?;

        if allocation.client_id != client_id {
            return Err(BrokerError::NotAllocationOwner {
                client_id: client_id.to_string(),
                allocation_id: allocation_id.to_string(),
            });
        }

        self.remove_allocation(allocation_id, ledger, "released_by_client")
    }

    /// Force-release an allocation regardless of owner (admin operation).
    pub fn force_release(
        &mut self,
        allocation_id: &str,
        ledger: &mut EventLedger,
    ) -> Result<Allocation, BrokerError> {
        self.remove_allocation(allocation_id, ledger, "force_released")
    }

    /// Remove an allocation and free its resources.
    fn remove_allocation(
        &mut self,
        allocation_id: &str,
        ledger: &mut EventLedger,
        reason: &str,
    ) -> Result<Allocation, BrokerError> {
        let allocation = self
            .allocations
            .remove(allocation_id)
            .ok_or(BrokerError::AllocationNotFound(allocation_id.to_string()))?;

        // Free GPU mappings
        for idx in &allocation.gpus {
            self.gpu_to_allocation.remove(idx);
        }

        // Clean up cgroup (if any)
        if let Some(cgroups) = &self.cgroups {
            if let Err(e) = cgroups.remove(allocation_id) {
                tracing::warn!("cgroup cleanup failed for {}: {}", allocation_id, e);
            }
        }

        // Log to ledger
        let _ = ledger.append(LedgerEvent::AllocationReleased {
            allocation_id: allocation.id.clone(),
            client_id: allocation.client_id.clone(),
            reason: reason.to_string(),
            gpus: allocation.gpus.clone(),
        });

        Ok(allocation)
    }

    /// Extend an allocation's TTL via heartbeat. Only the owning client can heartbeat.
    pub fn heartbeat(
        &mut self,
        allocation_id: &str,
        client_id: &str,
        ledger: &mut EventLedger,
    ) -> Result<Allocation, BrokerError> {
        let allocation = self
            .allocations
            .get_mut(allocation_id)
            .ok_or(BrokerError::AllocationNotFound(allocation_id.to_string()))?;

        if allocation.client_id != client_id {
            return Err(BrokerError::NotAllocationOwner {
                client_id: client_id.to_string(),
                allocation_id: allocation_id.to_string(),
            });
        }

        let now = Utc::now();
        allocation.last_heartbeat = now;
        // A heartbeat from the client is evidence of activity — update last_activity_at.
        // This resets the dormant timer, preventing preemption while the client is alive.
        allocation.last_activity_at = Some(now);
        // Sliding window: each heartbeat extends the TTL from now.
        allocation.expires_at = now + chrono::Duration::seconds(allocation.ttl_secs as i64);

        let _ = ledger.append(LedgerEvent::AllocationHeartbeat {
            allocation_id: allocation.id.clone(),
            client_id: allocation.client_id.clone(),
        });

        Ok(allocation.clone())
    }

    /// Find and release all expired allocations (stale heartbeats).
    /// Returns the number of allocations reclaimed.
    ///
    /// Logs `AllocationExpired` events (not `AllocationReleased`) to preserve
    /// the semantic distinction: expiry means rIOd reclaimed resources due to
    /// lack of evidence the client is alive; release means the client explicitly
    /// returned them. Both are terminal, but they mean different things for
    /// audit and supervisor decision-making.
    pub fn reclaim_expired(&mut self, ledger: &mut EventLedger) -> usize {
        let now = Utc::now();
        let expired: Vec<String> = self
            .allocations
            .iter()
            .filter(|(_, a)| a.expires_at < now && a.end_reason.is_none())
            .map(|(id, _)| id.clone())
            .collect();

        let mut count = 0;
        for id in expired {
            if let Some(allocation) = self.allocations.get(&id).cloned() {
                // Log the expiry event BEFORE removing (ledger is source of truth)
                let _ = ledger.append(LedgerEvent::AllocationExpired {
                    allocation_id: allocation.id.clone(),
                    client_id: allocation.client_id.clone(),
                    last_heartbeat: allocation.last_heartbeat,
                    expired_at: now,
                    gpus: allocation.gpus.clone(),
                });

                // Free GPU mappings
                for idx in &allocation.gpus {
                    self.gpu_to_allocation.remove(idx);
                }

                // Clean up cgroup (if any)
                if let Some(cgroups) = &self.cgroups {
                    if let Err(e) = cgroups.remove(&id) {
                        tracing::warn!("cgroup cleanup failed for expired {}: {}", id, e);
                    }
                }

                self.allocations.remove(&id);
                count += 1;
            }
        }
        count
    }

    /// Release all allocations tagged with a dream cycle ID.
    /// Called when a dream cycle is preempted or completed to ensure clean teardown.
    /// Returns the number of allocations reclaimed.
    pub fn reclaim_dream_cycle(&mut self, cycle_id: &str, ledger: &mut EventLedger) -> usize {
        let cycle_allocations: Vec<String> = self
            .allocations
            .iter()
            .filter(|(_, a)| a.dream_cycle_id.as_deref() == Some(cycle_id))
            .map(|(id, _)| id.clone())
            .collect();

        let mut count = 0;
        for id in cycle_allocations {
            if self.force_release(&id, ledger).is_ok() {
                count += 1;
            }
        }
        count
    }

    /// Scan all active allocations and update `last_activity_at` for any whose
    /// GPUs show utilization above the idle threshold in the given snapshot.
    /// This provides objective evidence of resource usage, independent of client
    /// heartbeats. An allocation with a busy GPU is "active" regardless of whether
    /// the client has sent a recent heartbeat.
    ///
    /// Returns the number of allocations whose activity was updated.
    pub fn update_allocation_activity(
        &mut self,
        snapshot: &ResourceSnapshot,
        thresholds: &AvailabilityThresholds,
    ) -> usize {
        let now = Utc::now();
        let mut updated = 0;

        for allocation in self.allocations.values_mut() {
            if allocation.gpus.is_empty() || allocation.end_reason.is_some() {
                continue;
            }

            // Check if any of this allocation's GPUs are actively used
            let has_activity = allocation.gpus.iter().any(|gpu_idx| {
                snapshot
                    .gpus
                    .iter()
                    .find(|g| g.index == *gpu_idx)
                    .map(|g| g.utilization_percent >= thresholds.idle_utilization_threshold)
                    .unwrap_or(false)
            });

            if has_activity {
                // Update last_activity_at only if the new time is more recent
                // (prevents clock skew from moving it backwards)
                match allocation.last_activity_at {
                    Some(last) if last > now => {} // Keep existing (clock safety)
                    _ => {
                        allocation.last_activity_at = Some(now);
                        updated += 1;
                    }
                }
            }
        }

        updated
    }

    /// Get the current pool status.
    pub fn status(&self) -> PoolStatus {
        PoolStatus {
            total_gpus: self.gpus.clone(),
            allocated_gpus: self.gpu_to_allocation.clone(),
            total_ram_mb: self.total_ram_mb,
            allocated_ram_mb: self.allocated_ram(),
            active_allocations: self.allocations.len(),
        }
    }

    /// Compute utilization-aware availability by combining riod's allocation
    /// ledger with the latest system resource snapshot.
    ///
    /// A GPU is "available" only if:
    /// 1. It is not allocated by riod, AND
    /// 2. It has at least `thresholds.min_free_vram_mb` free VRAM, AND
    /// 3. Its utilization is below `thresholds.max_gpu_utilization_percent`
    ///
    /// RAM available = total - system_used - riod_allocated (with a floor
    /// of `thresholds.min_free_ram_mb` that must remain unallocated).
    ///
    /// Returns `None` if no snapshot is provided (falls back to allocation-only view).
    pub fn effective_availability(
        &self,
        snapshot: Option<&ResourceSnapshot>,
        thresholds: &AvailabilityThresholds,
    ) -> EffectiveAvailability {
        let riod_allocated_gpus: Vec<u32> = self.gpu_to_allocation.keys().cloned().collect();
        let riod_allocated_ram = self.allocated_ram();

        // Compute per-GPU availability
        let mut gpu_details = Vec::new();
        let mut available_gpus = Vec::new();

        for gpu in &self.gpus {
            // Check if allocated by riod
            if let Some(alloc_id) = self.gpu_to_allocation.get(&gpu.index) {
                gpu_details.push(GpuAvailability {
                    index: gpu.index,
                    name: gpu.name.clone(),
                    available: false,
                    reason: Some(format!("allocated by riod ({})", alloc_id)),
                    riod_allocation: Some(alloc_id.clone()),
                    free_vram_mb: gpu.vram_total_mb.saturating_sub(gpu.vram_used_mb),
                    utilization_percent: gpu.utilization_percent,
                });
                continue;
            }

            // Check actual utilization from snapshot (if available)
            let snap_gpu = snapshot.and_then(|s| s.gpus.iter().find(|g| g.index == gpu.index));

            let free_vram = match snap_gpu {
                Some(sg) => sg.vram_total_mb.saturating_sub(sg.vram_used_mb),
                None => gpu.vram_total_mb.saturating_sub(gpu.vram_used_mb),
            };
            let utilization = snap_gpu
                .map(|sg| sg.utilization_percent)
                .unwrap_or(gpu.utilization_percent);

            // Check free VRAM threshold
            if free_vram < thresholds.min_free_vram_mb {
                gpu_details.push(GpuAvailability {
                    index: gpu.index,
                    name: gpu.name.clone(),
                    available: false,
                    reason: Some(format!(
                        "insufficient free VRAM: {}MB < {}MB required",
                        free_vram, thresholds.min_free_vram_mb
                    )),
                    riod_allocation: None,
                    free_vram_mb: free_vram,
                    utilization_percent: utilization,
                });
                continue;
            }

            // Check utilization threshold
            if utilization >= thresholds.max_gpu_utilization_percent {
                gpu_details.push(GpuAvailability {
                    index: gpu.index,
                    name: gpu.name.clone(),
                    available: false,
                    reason: Some(format!(
                        "high utilization: {}% >= {}% threshold",
                        utilization, thresholds.max_gpu_utilization_percent
                    )),
                    riod_allocation: None,
                    free_vram_mb: free_vram,
                    utilization_percent: utilization,
                });
                continue;
            }

            // GPU is available
            gpu_details.push(GpuAvailability {
                index: gpu.index,
                name: gpu.name.clone(),
                available: true,
                reason: None,
                riod_allocation: None,
                free_vram_mb: free_vram,
                utilization_percent: utilization,
            });
            available_gpus.push(gpu.index);
        }

        // Compute RAM availability: total - system_used - riod_allocated
        let (system_used_ram, data_fresh) = match snapshot {
            Some(s) => (s.ram_used_mb, true),
            None => (0, false),
        };

        let available_ram = self
            .total_ram_mb
            .saturating_sub(system_used_ram)
            .saturating_sub(riod_allocated_ram);

        // If we don't have fresh data, be conservative and report 0 available RAM
        // (better to under-report than over-allocate).
        let effective_available_ram = if data_fresh { available_ram } else { 0 };

        EffectiveAvailability {
            available_gpus,
            gpu_details,
            total_gpus: self.gpus.len(),
            riod_allocated_gpus,
            available_ram_mb: effective_available_ram,
            total_ram_mb: self.total_ram_mb,
            system_used_ram_mb: system_used_ram,
            riod_allocated_ram_mb: riod_allocated_ram,
            active_allocations: self.allocations.len(),
            data_fresh,
        }
    }

    /// List all active allocations.
    pub fn list_allocations(&self) -> Vec<&Allocation> {
        self.allocations.values().collect()
    }

    /// Get a specific allocation by ID.
    pub fn get_allocation(&self, id: &str) -> Option<&Allocation> {
        self.allocations.get(id)
    }

    /// Update the GPU inventory (called when observation detects hardware changes).
    pub fn update_gpus(&mut self, gpus: Vec<GpuInfo>) {
        self.gpus = gpus;
    }

    /// Add a process PID to an allocation's cgroup (if cgroup enforcement is enabled).
    /// Clients use this to register their own processes for resource tracking.
    pub fn add_process(
        &self,
        allocation_id: &str,
        client_id: &str,
        pid: u32,
    ) -> Result<(), BrokerError> {
        let allocation = self
            .allocations
            .get(allocation_id)
            .ok_or(BrokerError::AllocationNotFound(allocation_id.to_string()))?;

        if allocation.client_id != client_id {
            return Err(BrokerError::NotAllocationOwner {
                client_id: client_id.to_string(),
                allocation_id: allocation_id.to_string(),
            });
        }

        match &self.cgroups {
            Some(cgroups) => cgroups
                .add_process(allocation_id, pid)
                .map_err(|e| BrokerError::CgroupError(e.to_string())),
            None => Err(BrokerError::CgroupNotEnabled),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::observe::{GpuInfo, ResourceSnapshot};

    fn test_gpu(index: u32, vram_total: u64, vram_used: u64, util: u8) -> GpuInfo {
        GpuInfo {
            index,
            name: format!("Test GPU {}", index),
            vram_total_mb: vram_total,
            vram_used_mb: vram_used,
            utilization_percent: util,
        }
    }

    fn test_snapshot(gpus: Vec<GpuInfo>, ram_used_mb: u64) -> ResourceSnapshot {
        ResourceSnapshot {
            timestamp: Utc::now(),
            cpu_usage_percent: 10.0,
            ram_total_mb: 32768,
            ram_used_mb,
            gpus,
        }
    }

    #[test]
    fn test_effective_availability_no_snapshot() {
        let gpus = vec![test_gpu(0, 16384, 512, 5)];
        let mut broker = ResourceBroker::new(gpus, 32768);
        let thresholds = AvailabilityThresholds::default();

        let avail = broker.effective_availability(None, &thresholds);

        // Without a snapshot, data_fresh is false and RAM is conservatively 0
        assert!(!avail.data_fresh);
        assert_eq!(avail.available_ram_mb, 0);
        // GPU should still be available based on allocation-only check
        assert_eq!(avail.available_gpus, vec![0]);
    }

    #[test]
    fn test_effective_availability_gpu_high_utilization() {
        let gpus = vec![test_gpu(0, 16384, 512, 95)]; // 95% utilized
        let mut broker = ResourceBroker::new(gpus, 32768);
        let snap = test_snapshot(vec![test_gpu(0, 16384, 512, 95)], 8192);
        let thresholds = AvailabilityThresholds::default();

        let avail = broker.effective_availability(Some(&snap), &thresholds);

        // GPU should NOT be available due to high utilization (95% >= 80%)
        assert!(avail.available_gpus.is_empty());
        assert_eq!(avail.gpu_details[0].index, 0);
        assert!(!avail.gpu_details[0].available);
        assert!(avail.gpu_details[0]
            .reason
            .as_ref()
            .unwrap()
            .contains("high utilization"));
    }

    #[test]
    fn test_effective_availability_gpu_low_vram() {
        let gpus = vec![test_gpu(0, 16384, 16000, 10)]; // Only 384MB free
        let mut broker = ResourceBroker::new(gpus, 32768);
        let snap = test_snapshot(vec![test_gpu(0, 16384, 16000, 10)], 8192);
        let thresholds = AvailabilityThresholds {
            min_free_vram_mb: 1024,
            ..Default::default()
        };

        let avail = broker.effective_availability(Some(&snap), &thresholds);

        // GPU should NOT be available due to low free VRAM (384MB < 1024MB)
        assert!(avail.available_gpus.is_empty());
        assert!(avail.gpu_details[0]
            .reason
            .as_ref()
            .unwrap()
            .contains("insufficient free VRAM"));
    }

    #[test]
    fn test_effective_availability_gpu_allocated_by_riod() {
        let gpus = vec![test_gpu(0, 16384, 512, 5)];
        let mut broker = ResourceBroker::new(gpus, 32768);

        // Allocate GPU 0 through riod
        let request = AllocationRequest {
            client_id: "test-client".into(),
            purpose: "testing".into(),
            gpus: Some(vec![0]),
            gpu_count: None,
            cpu_percent: None,
            ram_mb: Some(1024),
            ttl_secs: 300,
            intent_id: None,
            dream_cycle_id: None,
        };
        let ledger_path =
            std::env::temp_dir().join(format!("riod-test-{}.jsonl", std::process::id()));
        let _ = std::fs::remove_file(&ledger_path);
        let mut ledger = EventLedger::open(crate::ledger::LedgerConfig {
            path: ledger_path.clone(),
            retention_days: 1,
        })
        .unwrap();
        let thresholds = AvailabilityThresholds::default();
        broker
            .allocate(&request, &mut ledger, None, &thresholds)
            .unwrap();

        let snap = test_snapshot(vec![test_gpu(0, 16384, 512, 5)], 8192);

        let avail = broker.effective_availability(Some(&snap), &thresholds);

        // GPU should NOT be available because riod allocated it
        assert!(avail.available_gpus.is_empty());
        assert_eq!(avail.riod_allocated_gpus, vec![0]);
        assert!(avail.gpu_details[0]
            .reason
            .as_ref()
            .unwrap()
            .contains("allocated by riod"));

        let _ = std::fs::remove_file(&ledger_path);
    }

    #[test]
    fn test_effective_availability_gpu_available() {
        let gpus = vec![test_gpu(0, 16384, 512, 5)]; // Plenty of free VRAM, low util
        let mut broker = ResourceBroker::new(gpus, 32768);
        let snap = test_snapshot(vec![test_gpu(0, 16384, 512, 5)], 8192);
        let thresholds = AvailabilityThresholds::default();

        let avail = broker.effective_availability(Some(&snap), &thresholds);

        // GPU should be available
        assert_eq!(avail.available_gpus, vec![0]);
        assert!(avail.gpu_details[0].available);
        assert!(avail.gpu_details[0].reason.is_none());
        assert!(avail.data_fresh);

        // RAM: 32768 total - 8192 system used - 0 riod allocated = 24576
        assert_eq!(avail.available_ram_mb, 24576);
    }

    #[test]
    fn test_effective_availability_mixed_gpus() {
        let gpus = vec![
            test_gpu(0, 16384, 512, 5),    // Available
            test_gpu(1, 16384, 16000, 95), // Busy (low VRAM + high util)
            test_gpu(2, 16384, 512, 50),   // Available (moderate util)
        ];
        let mut broker = ResourceBroker::new(gpus, 32768);
        let snap = test_snapshot(
            vec![
                test_gpu(0, 16384, 512, 5),
                test_gpu(1, 16384, 16000, 95),
                test_gpu(2, 16384, 512, 50),
            ],
            16384,
        );
        let thresholds = AvailabilityThresholds::default();

        let avail = broker.effective_availability(Some(&snap), &thresholds);

        // GPUs 0 and 2 should be available, GPU 1 should not
        assert_eq!(avail.available_gpus, vec![0, 2]);
        assert!(!avail.gpu_details[1].available);
    }

    #[test]
    fn test_allocate_denied_when_gpu_busy_external() {
        // Simulate a GPU that's busy with external work (not allocated by riod)
        let gpus = vec![test_gpu(0, 16384, 512, 95)]; // 95% util — external workload
        let mut broker = ResourceBroker::new(gpus, 32768);
        let snap = test_snapshot(vec![test_gpu(0, 16384, 512, 95)], 8192);
        let thresholds = AvailabilityThresholds::default();

        let request = AllocationRequest {
            client_id: "new-client".into(),
            purpose: "inference".into(),
            gpus: Some(vec![0]),
            gpu_count: None,
            cpu_percent: None,
            ram_mb: Some(1024),
            ttl_secs: 300,
            intent_id: None,
            dream_cycle_id: None,
        };

        let ledger_path =
            std::env::temp_dir().join(format!("riod-test-busy-{}.jsonl", std::process::id()));
        let _ = std::fs::remove_file(&ledger_path);
        let mut ledger = EventLedger::open(crate::ledger::LedgerConfig {
            path: ledger_path.clone(),
            retention_days: 1,
        })
        .unwrap();

        // Allocation should be denied because GPU 0 is at 95% utilization
        let result = broker.allocate(&request, &mut ledger, Some(&snap), &thresholds);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("utilization too high"));

        let _ = std::fs::remove_file(&ledger_path);
    }

    #[test]
    fn test_allocate_denied_when_gpu_low_vram_external() {
        // Simulate a GPU with low free VRAM due to external work
        let gpus = vec![test_gpu(0, 16384, 16200, 10)]; // Only 184MB free
        let mut broker = ResourceBroker::new(gpus, 32768);
        let snap = test_snapshot(vec![test_gpu(0, 16384, 16200, 10)], 8192);
        let thresholds = AvailabilityThresholds {
            min_free_vram_mb: 1024,
            ..Default::default()
        };

        let request = AllocationRequest {
            client_id: "new-client".into(),
            purpose: "inference".into(),
            gpus: Some(vec![0]),
            gpu_count: None,
            cpu_percent: None,
            ram_mb: Some(1024),
            ttl_secs: 300,
            intent_id: None,
            dream_cycle_id: None,
        };

        let ledger_path =
            std::env::temp_dir().join(format!("riod-test-vram-{}.jsonl", std::process::id()));
        let _ = std::fs::remove_file(&ledger_path);
        let mut ledger = EventLedger::open(crate::ledger::LedgerConfig {
            path: ledger_path.clone(),
            retention_days: 1,
        })
        .unwrap();

        // Allocation should be denied because only 184MB free < 1024MB threshold
        let result = broker.allocate(&request, &mut ledger, Some(&snap), &thresholds);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("insufficient free VRAM"));

        let _ = std::fs::remove_file(&ledger_path);
    }

    #[test]
    fn test_allocate_granted_when_gpu_idle() {
        // Simulate an idle GPU — allocation should succeed
        let gpus = vec![test_gpu(0, 16384, 512, 3)]; // Low util, plenty of VRAM
        let mut broker = ResourceBroker::new(gpus, 32768);
        let snap = test_snapshot(vec![test_gpu(0, 16384, 512, 3)], 8192);
        let thresholds = AvailabilityThresholds::default();

        let request = AllocationRequest {
            client_id: "new-client".into(),
            purpose: "inference".into(),
            gpus: Some(vec![0]),
            gpu_count: None,
            cpu_percent: None,
            ram_mb: Some(1024),
            ttl_secs: 300,
            intent_id: None,
            dream_cycle_id: None,
        };

        let ledger_path =
            std::env::temp_dir().join(format!("riod-test-idle-{}.jsonl", std::process::id()));
        let _ = std::fs::remove_file(&ledger_path);
        let mut ledger = EventLedger::open(crate::ledger::LedgerConfig {
            path: ledger_path.clone(),
            retention_days: 1,
        })
        .unwrap();

        // Allocation should succeed
        let alloc = broker
            .allocate(&request, &mut ledger, Some(&snap), &thresholds)
            .unwrap();
        assert_eq!(alloc.gpus, vec![0]);
        assert_eq!(alloc.client_id, "new-client");
        assert!(alloc.last_activity_at.is_some());

        let _ = std::fs::remove_file(&ledger_path);
    }

    #[test]
    fn test_dormant_allocation_preempted() {
        // Simulate a dormant allocation (idle GPU, no recent activity)
        let gpus = vec![test_gpu(0, 16384, 512, 2)]; // GPU idle at 2%
        let mut broker = ResourceBroker::new(gpus, 32768);
        let snap = test_snapshot(vec![test_gpu(0, 16384, 512, 2)], 8192);

        // Use a short dormant timeout so we can trigger preemption quickly
        let thresholds = AvailabilityThresholds {
            dormant_timeout_secs: 1, // 1 second — for testing
            idle_utilization_threshold: 5,
            ..Default::default()
        };

        let ledger_path =
            std::env::temp_dir().join(format!("riod-test-dormant-{}.jsonl", std::process::id()));
        let _ = std::fs::remove_file(&ledger_path);
        let mut ledger = EventLedger::open(crate::ledger::LedgerConfig {
            path: ledger_path.clone(),
            retention_days: 1,
        })
        .unwrap();

        // First allocation takes GPU 0
        let req1 = AllocationRequest {
            client_id: "sleepy-client".into(),
            purpose: "idle-task".into(),
            gpus: Some(vec![0]),
            gpu_count: None,
            cpu_percent: None,
            ram_mb: Some(512),
            ttl_secs: 3600, // Long TTL — won't expire naturally
            intent_id: None,
            dream_cycle_id: None,
        };
        let alloc1 = broker
            .allocate(&req1, &mut ledger, Some(&snap), &thresholds)
            .unwrap();

        // Wait for the dormant timeout to pass
        std::thread::sleep(std::time::Duration::from_millis(1500));

        // Second allocation requests GPU 0 — should preempt the dormant one
        let req2 = AllocationRequest {
            client_id: "urgent-client".into(),
            purpose: "real-work".into(),
            gpus: Some(vec![0]),
            gpu_count: None,
            cpu_percent: None,
            ram_mb: Some(512),
            ttl_secs: 300,
            intent_id: None,
            dream_cycle_id: None,
        };
        let alloc2 = broker
            .allocate(&req2, &mut ledger, Some(&snap), &thresholds)
            .unwrap();

        // The new allocation should have GPU 0
        assert_eq!(alloc2.gpus, vec![0]);
        assert_eq!(alloc2.client_id, "urgent-client");
        // The old allocation should be gone
        assert!(!broker.allocations.contains_key(&alloc1.id));
        // Only one allocation active now
        assert_eq!(broker.allocations.len(), 1);

        let _ = std::fs::remove_file(&ledger_path);
    }

    #[test]
    fn test_active_allocation_not_preempted() {
        // Simulate an active allocation (GPU busy with real work)
        let gpus = vec![test_gpu(0, 16384, 512, 90)]; // GPU at 90% — actively working
        let mut broker = ResourceBroker::new(gpus, 32768);
        let snap = test_snapshot(vec![test_gpu(0, 16384, 512, 90)], 8192);

        let thresholds = AvailabilityThresholds {
            dormant_timeout_secs: 1, // Short for testing
            idle_utilization_threshold: 5,
            ..Default::default()
        };

        let ledger_path =
            std::env::temp_dir().join(format!("riod-test-active-{}.jsonl", std::process::id()));
        let _ = std::fs::remove_file(&ledger_path);
        let mut ledger = EventLedger::open(crate::ledger::LedgerConfig {
            path: ledger_path.clone(),
            retention_days: 1,
        })
        .unwrap();

        // First allocation takes GPU 0 while it's busy (simulating work in progress)
        let req1 = AllocationRequest {
            client_id: "working-client".into(),
            purpose: "training".into(),
            gpus: Some(vec![0]),
            gpu_count: None,
            cpu_percent: None,
            ram_mb: Some(512),
            ttl_secs: 3600,
            intent_id: None,
            dream_cycle_id: None,
        };
        let alloc1 = broker
            .allocate(&req1, &mut ledger, None, &thresholds)
            .unwrap();

        // Wait past dormant timeout
        std::thread::sleep(std::time::Duration::from_millis(1500));

        // Second allocation requests GPU 0 — should FAIL because GPU is actively used
        let req2 = AllocationRequest {
            client_id: "other-client".into(),
            purpose: "inference".into(),
            gpus: Some(vec![0]),
            gpu_count: None,
            cpu_percent: None,
            ram_mb: Some(512),
            ttl_secs: 300,
            intent_id: None,
            dream_cycle_id: None,
        };

        // GPU is at 90% util — allocation should fail (not preempt active work)
        let result = broker.allocate(&req2, &mut ledger, Some(&snap), &thresholds);
        assert!(result.is_err());
        // The original allocation should still be active
        assert!(broker.allocations.contains_key(&alloc1.id));

        let _ = std::fs::remove_file(&ledger_path);
    }

    #[test]
    fn test_update_allocation_activity() {
        let gpus = vec![test_gpu(0, 16384, 512, 90)]; // GPU busy
        let mut broker = ResourceBroker::new(gpus, 32768);
        let snap = test_snapshot(vec![test_gpu(0, 16384, 512, 90)], 8192);
        let thresholds = AvailabilityThresholds::default();

        let ledger_path =
            std::env::temp_dir().join(format!("riod-test-activity-{}.jsonl", std::process::id()));
        let _ = std::fs::remove_file(&ledger_path);
        let mut ledger = EventLedger::open(crate::ledger::LedgerConfig {
            path: ledger_path.clone(),
            retention_days: 1,
        })
        .unwrap();

        // Allocate GPU 0
        let request = AllocationRequest {
            client_id: "client".into(),
            purpose: "training".into(),
            gpus: Some(vec![0]),
            gpu_count: None,
            cpu_percent: None,
            ram_mb: Some(512),
            ttl_secs: 3600,
            intent_id: None,
            dream_cycle_id: None,
        };
        let alloc = broker
            .allocate(&request, &mut ledger, None, &thresholds)
            .unwrap();

        // Simulate time passing without heartbeats (but GPU is busy)
        std::thread::sleep(std::time::Duration::from_millis(100));

        // Update activity based on snapshot — should mark allocation as active
        let updated = broker.update_allocation_activity(&snap, &thresholds);
        assert_eq!(updated, 1);

        // Verify last_activity_at was updated
        let current = broker.get_allocation(&alloc.id).unwrap();
        assert!(current.last_activity_at.is_some());

        let _ = std::fs::remove_file(&ledger_path);
    }

    #[test]
    fn test_heartbeat_updates_activity() {
        let gpus = vec![test_gpu(0, 16384, 512, 5)];
        let mut broker = ResourceBroker::new(gpus, 32768);
        let thresholds = AvailabilityThresholds::default();

        let ledger_path = std::env::temp_dir().join(format!(
            "riod-test-hb-activity-{}.jsonl",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&ledger_path);
        let mut ledger = EventLedger::open(crate::ledger::LedgerConfig {
            path: ledger_path.clone(),
            retention_days: 1,
        })
        .unwrap();

        // Allocate GPU 0
        let request = AllocationRequest {
            client_id: "client".into(),
            purpose: "inference".into(),
            gpus: Some(vec![0]),
            gpu_count: None,
            cpu_percent: None,
            ram_mb: Some(512),
            ttl_secs: 3600,
            intent_id: None,
            dream_cycle_id: None,
        };
        let alloc = broker
            .allocate(&request, &mut ledger, None, &thresholds)
            .unwrap();
        let activity_at_alloc = alloc.last_activity_at.unwrap();

        // Wait a bit then heartbeat
        std::thread::sleep(std::time::Duration::from_millis(100));
        let updated = broker.heartbeat(&alloc.id, "client", &mut ledger).unwrap();

        // Heartbeat should update last_activity_at to be more recent than allocation time
        assert!(updated.last_activity_at.is_some());
        assert!(updated.last_activity_at.unwrap() >= activity_at_alloc);

        let _ = std::fs::remove_file(&ledger_path);
    }
}
