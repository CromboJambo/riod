//! High-level typed API for communicating with the riod daemon.
//!
//! This module provides a synchronous, ergonomic interface over the raw
//! JSON-RPC transport. Each method corresponds to a daemon RPC method and
//! returns typed results instead of raw `serde_json::Value`.
//!
//! # Example
//!
//! ```no_run
//! use riod_client::{RiodClient, AllocateRequest};
//!
//! fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let mut client = RiodClient::default_connect()?;
//!
//!     // Check what's available
//!     let avail = client.available_resources()?;
//!     println!("Free GPUs: {:?}", avail.available_gpus);
//!
//!     // Request a GPU for training
//!     let req = AllocateRequest::new("my-training-job", "training")
//!         .with_gpu_count(1)
//!         .with_ttl(3600);
//!     let alloc = client.allocate(&req)?;
//!     println!("Got allocation {} on GPUs {:?}", alloc.id, alloc.gpus);
//!
//!     // ... do work ...
//!
//!     // Release when done
//!     client.release(&alloc.id, "my-training-job")?;
//!     Ok(())
//! }
//! ```

use crate::transport::{ClientError, Transport};
use crate::types::{Allocation, AllocateRequest, Availability, GpuInfo, LedgerEntry, ReleaseResult, SystemStatus};
use serde_json::json;
use std::path::Path;

/// The default Unix socket path where the riod daemon listens.
pub const DEFAULT_SOCKET: &str = "/run/riod.sock";

/// Result type for client operations.
pub type Result<T> = std::result::Result<T, ClientError>;

/// High-level client for the riod daemon API.
///
/// Wraps the low-level [`Transport`] with typed methods that match the
/// daemon's JSON-RPC interface. Each call opens a new connection, sends
/// one request, reads one response, and closes the connection — safe for
/// concurrent use from multiple threads.
#[derive(Clone)]
pub struct RiodClient {
    transport: Transport,
}

impl RiodClient {
    /// Create a new client connected to the given Unix socket path.
    pub fn connect(socket_path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            transport: Transport::new(socket_path.as_ref()),
        })
    }

    /// Connect to the default riod socket at `/run/riod.sock`.
    pub fn default_connect() -> Result<Self> {
        Self::connect(DEFAULT_SOCKET)
    }

    /// Get the socket path this client connects to.
    pub fn socket_path(&self) -> &Path {
        self.transport.socket_path()
    }

    // ── System Status ────────────────────────────────────────────────

    /// Query current system resource status (CPU, RAM, GPUs).
    ///
    /// Corresponds to `system.status`. Returns the latest resource snapshot.
    pub fn system_status(&self) -> Result<SystemStatus> {
        let result = self.transport.call("system.status", None)?;

        // Daemon wraps in {success, data: {snapshot, idle, uptime_secs}, doubt}
        let data = &result["data"];
        let snapshot = &data["snapshot"];

        if snapshot.is_null() {
            return Err(ClientError::NoData(
                "system.status returned null snapshot — observer may still be initializing"
                    .into(),
            ));
        }

        serde_json::from_value(snapshot.clone()).map_err(|e| ClientError::Deserialize(e.to_string()))
    }

    /// Query current GPU information.
    pub fn gpus(&self) -> Result<Vec<GpuInfo>> {
        Ok(self.system_status()?.gpus)
    }

    // ── Resource Broker ──────────────────────────────────────────────

    /// Request resources from the broker.
    ///
    /// Corresponds to `resources.allocate`. The daemon assigns GPUs and/or RAM
    /// according to the request, creates a cgroup for enforcement, and returns
    /// an allocation handle with an expiry timestamp.
    pub fn allocate(&self, request: &AllocateRequest) -> Result<Allocation> {
        let params = serde_json::to_value(request)?;
        let result = self.transport.call("resources.allocate", Some(params))?;

        // Daemon wraps in {success, data: {allocation, message}}
        let allocation = &result["data"]["allocation"];
        serde_json::from_value(allocation.clone()).map_err(|e| ClientError::Deserialize(e.to_string()))
    }

    /// Release a previously granted allocation.
    ///
    /// Corresponds to `resources.release`. Frees the GPUs and RAM back to the pool
    /// and removes the cgroup. The client_id must match the one used in allocate.
    pub fn release(&self, allocation_id: &str, client_id: &str) -> Result<ReleaseResult> {
        let params = json!({
            "allocation_id": allocation_id,
            "client_id": client_id,
        });
        let result = self.transport.call("resources.release", Some(params))?;

        // Daemon returns {success, data: {released, client_id, gpus_freed}}
        let data = &result["data"];
        Ok(ReleaseResult {
            released: data["released"]
                .as_str()
                .unwrap_or(allocation_id)
                .to_string(),
            gpus_freed: serde_json::from_value(data["gpus_freed"].clone())
                .map_err(|e| ClientError::Deserialize(e.to_string()))?,
        })
    }

    /// Send a heartbeat to extend an allocation's TTL.
    ///
    /// Corresponds to `resources.heartbeat`. Returns the updated expiry time.
    pub fn heartbeat(&self, allocation_id: &str, client_id: &str) -> Result<String> {
        let params = json!({
            "allocation_id": allocation_id,
            "client_id": client_id,
        });
        let result = self.transport.call("resources.heartbeat", Some(params))?;

        // Daemon returns {success, data: {allocation_id, expires_at}}
        let expires_at = result["data"]["expires_at"]
            .as_str()
            .ok_or_else(|| ClientError::NoData("heartbeat response missing expires_at".into()))?;
        Ok(expires_at.to_string())
    }

    /// List all active allocations.
    ///
    /// Corresponds to `resources.list`.
    pub fn list_allocations(&self) -> Result<Vec<Allocation>> {
        let result = self.transport.call("resources.list", None)?;

        // Daemon returns {success, data: {allocations, count}}
        let allocations = &result["data"]["allocations"];
        serde_json::from_value(allocations.clone()).map_err(|e| ClientError::Deserialize(e.to_string()))
    }

    /// Query currently available resources.
    ///
    /// Corresponds to `resources.available`. Shows free GPUs and RAM capacity.
    pub fn available_resources(&self) -> Result<Availability> {
        let result = self.transport.call("resources.available", None)?;

        // Daemon returns {success, data: {available_gpus, total_gpus, ...}}
        let data = &result["data"];
        serde_json::from_value(data.clone()).map_err(|e| ClientError::Deserialize(e.to_string()))
    }

    /// Add a process to an allocation's cgroup for resource enforcement.
    ///
    /// Corresponds to `resources.add_process`. The PID must belong to the calling
    /// user (or be root). Once added, the process is subject to the allocation's
    /// CPU/RAM limits.
    pub fn add_process(&self, allocation_id: &str, client_id: &str, pid: u32) -> Result<()> {
        let params = json!({
            "allocation_id": allocation_id,
            "client_id": client_id,
            "pid": pid,
        });
        self.transport.call("resources.add_process", Some(params))?;
        Ok(())
    }

    // ── Event Ledger ─────────────────────────────────────────────────

    /// Get the most recent N events from the ledger.
    ///
    /// Corresponds to `ledger.tail`. Returns events in reverse chronological order.
    pub fn ledger_tail(&self, n: usize) -> Result<Vec<LedgerEntry>> {
        let params = json!({ "n": n });
        let result = self.transport.call("ledger.tail", Some(params))?;

        // Daemon returns {success, data: {events, count}}
        let events = &result["data"]["events"];
        serde_json::from_value(events.clone()).map_err(|e| ClientError::Deserialize(e.to_string()))
    }
}

impl Default for RiodClient {
    fn default() -> Self {
        Self::default_connect().expect("failed to connect to riod daemon at /run/riod.sock")
    }
}

/// Async wrapper around [`RiodClient`] for use in async runtimes.
///
/// Each method spawns the blocking call on a thread pool via `tokio::task::spawn_blocking`,
/// so it can be awaited from an async context without blocking the runtime.
pub struct RiodClientAsync {
    inner: std::sync::Arc<RiodClient>,
}

impl Clone for RiodClientAsync {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl RiodClientAsync {
    /// Create a new async client connected to the given socket path.
    pub fn connect(socket_path: impl AsRef<Path>) -> Result<Self> {
        let client = RiodClient::connect(socket_path)?;
        Ok(Self {
            inner: std::sync::Arc::new(client),
        })
    }

    /// Connect to the default riod socket.
    pub fn default_connect() -> Result<Self> {
        Self::connect(DEFAULT_SOCKET)
    }

    /// Get current system status.
    pub async fn system_status(&self) -> Result<SystemStatus> {
        let client = self.inner.clone();
        tokio::task::spawn_blocking(move || client.system_status())
            .await
            .map_err(|e| ClientError::Async(e.to_string()))?
    }

    /// Request resources.
    pub async fn allocate(&self, request: AllocateRequest) -> Result<Allocation> {
        let client = self.inner.clone();
        tokio::task::spawn_blocking(move || client.allocate(&request))
            .await
            .map_err(|e| ClientError::Async(e.to_string()))?
    }

    /// Release an allocation.
    pub async fn release(&self, allocation_id: String, client_id: String) -> Result<ReleaseResult> {
        let client = self.inner.clone();
        tokio::task::spawn_blocking(move || client.release(&allocation_id, &client_id))
            .await
            .map_err(|e| ClientError::Async(e.to_string()))?
    }

    /// Send a heartbeat.
    pub async fn heartbeat(&self, allocation_id: String, client_id: String) -> Result<String> {
        let client = self.inner.clone();
        tokio::task::spawn_blocking(move || client.heartbeat(&allocation_id, &client_id))
            .await
            .map_err(|e| ClientError::Async(e.to_string()))?
    }

    /// List active allocations.
    pub async fn list_allocations(&self) -> Result<Vec<Allocation>> {
        let client = self.inner.clone();
        tokio::task::spawn_blocking(move || client.list_allocations())
            .await
            .map_err(|e| ClientError::Async(e.to_string()))?
    }

    /// Query available resources.
    pub async fn available_resources(&self) -> Result<Availability> {
        let client = self.inner.clone();
        tokio::task::spawn_blocking(move || client.available_resources())
            .await
            .map_err(|e| ClientError::Async(e.to_string()))?
    }

    /// Add a process to an allocation's cgroup.
    pub async fn add_process(&self, allocation_id: String, client_id: String, pid: u32) -> Result<()> {
        let client = self.inner.clone();
        tokio::task::spawn_blocking(move || client.add_process(&allocation_id, &client_id, pid))
            .await
            .map_err(|e| ClientError::Async(e.to_string()))?
    }

    /// Get recent ledger events.
    pub async fn ledger_tail(&self, n: usize) -> Result<Vec<LedgerEntry>> {
        let client = self.inner.clone();
        tokio::task::spawn_blocking(move || client.ledger_tail(n))
            .await
            .map_err(|e| ClientError::Async(e.to_string()))?
    }
}

impl Default for RiodClientAsync {
    fn default() -> Self {
        Self::default_connect().expect("failed to connect to riod daemon at /run/riod.sock")
    }
}
