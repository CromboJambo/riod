//! rIOd — System Resource Supervisor
//!
//! Observes, classifies, authorizes, and coordinates existing processes
//! without owning their lifecycle. Phase 0: observation and reporting.
//! Phase 1: resource brokerage (allocation, heartbeats, reclamation).

pub mod api_broker;
pub mod broker;
pub mod cgroup;
pub mod ledger;
pub mod observe;
pub mod socket;
pub mod state;
