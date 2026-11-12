//! # riod-client
//!
//! Client library for the [rIOd](https://github.com/crombojambo/riod) system resource supervisor.
//!
//! Communicates with the riod daemon over Unix socket JSON-RPC to request, release,
//! and manage resource allocations (GPU, RAM, CPU).
//!
//! ## Quick Start
//!
//! ```no_run
//! use riod_client::{AllocateRequest, RiodClient};
//!
//! fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Connect to the daemon (default socket: /run/riod.sock)
//!     let client = RiodClient::default_connect()?;
//!
//!     // Check what's available
//!     let avail = client.available_resources()?;
//!     println!("Available GPUs: {:?}", avail.available_gpus);
//!
//!     // Request an allocation using the builder API
//!     let req = AllocateRequest::new("my-app", "inference")
//!         .with_gpu_count(1)       // request 1 GPU (auto-selected)
//!         .with_ram_mb(4096)       // up to 4GB RAM
//!         .with_ttl(3600);         // 1 hour TTL
//!
//!     let alloc = client.allocate(&req)?;
//!     println!("Got allocation {} with GPUs {:?}", alloc.id, alloc.gpus);
//!
//!     // If cgroups are enabled, register your process
//!     let pid = std::process::id();
//!     client.add_process(&alloc.id, "my-app", pid)?;
//!
//!     // Release when done
//!     client.release(&alloc.id, "my-app")?;
//!
//!     Ok(())
//! }
//! ```
//!
//! ## Async Usage
//!
//! ```no_run
//! use riod_client::{AllocateRequest, RiodClientAsync};
//!
//! #[tokio::main(flavor = "current_thread")]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let client = RiodClientAsync::default_connect()?;
//!
//!     let req = AllocateRequest::new("worker", "training")
//!         .with_gpu_count(1)
//!         .with_ram_mb(8192);
//!
//!     let alloc = client.allocate(req).await?;
//!     println!("Training on GPUs {:?}", alloc.gpus);
//!
//!     // ... do work ...
//!
//!     client.release(alloc.id, "worker".to_string()).await?;
//!     Ok(())
//! }
//! ```

pub mod client;
pub mod transport;
pub mod types;

pub use client::{RiodClient, RiodClientAsync, DEFAULT_SOCKET};
pub use transport::ClientError;
pub use types::{
    AllocateRequest, Allocation, Availability, GpuInfo, LedgerEntry, LedgerStats, ReleaseResult,
    SystemStatus,
};
