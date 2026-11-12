//! Shared server state for rIOd daemon components.
//!
//! This module holds the `ServerState` struct that is shared between
//! the observer loop, socket server, and background tasks via Arc<Mutex>.

use crate::broker::ResourceBroker;
use crate::dream::DreamScheduler;
use crate::ledger::EventLedger;
use crate::observe::IdleDetector;
use crate::policy::PolicyEngine;
use chrono::{DateTime, Utc};
use std::sync::Mutex;

/// Shared state between the observer loop and the socket server.
pub struct ServerState {
    pub detector: Mutex<IdleDetector>,
    pub ledger: Mutex<EventLedger>,
    /// Resource broker for allocation management (Phase 1).
    pub broker: Mutex<ResourceBroker>,
    /// Policy engine for purpose-based authorization (Phase 2).
    pub policy_engine: Mutex<PolicyEngine>,
    /// Dream cycle scheduler (Phase 3).
    pub dream_scheduler: Mutex<DreamScheduler>,
    /// Daemon start time (for uptime reporting).
    pub started_at: DateTime<Utc>,
    /// Configuration snapshot for status reporting.
    pub poll_interval_s: u64,
}
