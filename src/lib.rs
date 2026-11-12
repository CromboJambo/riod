//! rIOd — System Resource Supervisor
//!
//! Observes, classifies, authorizes, and coordinates existing processes
//! without owning their lifecycle. Phase 0: observation and reporting.
//! Phase 1: resource brokerage (allocation, heartbeats, reclamation).
//! Phase 2: purpose-based authorization via declarative TOML policies.

pub mod api_broker;
pub mod broker;
pub mod cgroup;
pub mod dream;
pub mod dream_tasks;
pub mod ledger;
pub mod observe;
pub mod policy;
pub mod risk;
pub mod socket;
pub mod state;
