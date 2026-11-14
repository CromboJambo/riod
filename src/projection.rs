//! Allocation lifecycle projections.
//!
//! The core principle: **state is derived from events, never stored independently.**
//!
//! The event ledger is the source of truth. Everything else — in-memory broker
//! state, API responses, supervisor beliefs — is a projection that can be rebuilt
//! by replaying events. This module provides:
//!
//! - [`ResourceState`]: the derived lifecycle state of an allocation
//! - [`Allocation::current_state`]: compute state from event history + time
//! - [`rehydrate_allocations`]: rebuild broker state from the ledger on startup
//!
//! The four-layer invariant:
//! 1. Configuration describes intent.
//! 2. Events record what happened.
//! 3. Projections describe what we currently believe.
//! 4. The broker enforces what is allowed.

use crate::broker::Allocation;
use crate::ledger::{EventLedger, LedgerEvent};
use chrono::{DateTime, Duration, Utc};
use std::collections::HashMap;

/// Derived lifecycle state of a resource allocation.
///
/// This is NEVER stored as a mutable field. It is computed on demand from:
/// - The event history (granted, heartbeats, released/expired)
/// - The current time (for TTL expiry)
///
/// The distinction between `Unknown` and `Expired` matters:
/// - `Unknown`: within TTL but no recent heartbeat. We lack evidence of liveness,
///   but haven't given up yet. A supervisor might wait or probe.
/// - `Expired`: TTL elapsed without renewal. rIOd has reclaimed the resources.
///   The allocation is over from rIOd's perspective, whether the client knows it or not.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ResourceState {
    /// Within TTL and recent heartbeat evidence. Allocation is confirmed active.
    Active,
    /// Within TTL but no recent heartbeat. Insufficient evidence of liveness.
    /// The allocation still holds resources; rIOd hasn't reclaimed them yet.
    Unknown,
    /// TTL elapsed without renewal. Resources reclaimed by rIOd.
    Expired,
    /// Client explicitly released the allocation. Terminal.
    Released,
}

impl std::fmt::Display for ResourceState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ResourceState::Active => write!(f, "active"),
            ResourceState::Unknown => write!(f, "unknown"),
            ResourceState::Expired => write!(f, "expired"),
            ResourceState::Released => write!(f, "released"),
        }
    }
}

impl Allocation {
    /// Derive the current lifecycle state from event history + time.
    ///
    /// This is a pure function of the allocation's fields (which are themselves
    /// derived from ledger events) and the current time. It never mutates anything.
    ///
    /// State machine:
    /// ```text
    ///   Granted ──► Active ──(no heartbeat > TTL/2)──► Unknown ──(TTL elapsed)──► Expired
    ///                │                                    │
    ///                └────(client release)─────────────────┴──► Released
    /// ```
    pub fn current_state(&self, now: DateTime<Utc>) -> ResourceState {
        // Terminal: was explicitly released
        if self.end_reason.is_some() && self.end_reason.as_deref() != Some("expired") {
            return ResourceState::Released;
        }

        // Terminal: expired (TTL elapsed without renewal)
        let expires_at = self.granted_at + Duration::seconds(self.ttl_secs as i64);
        if now >= expires_at {
            return ResourceState::Expired;
        }

        // Within TTL: check evidence recency
        // Warning threshold: half the TTL. If we haven't heard from the client
        // in more than half their allocated time, we're uncertain but not done.
        let since_heartbeat = now.signed_duration_since(self.last_heartbeat);
        let warning_threshold = Duration::seconds((self.ttl_secs as i64).max(2) / 2);

        if since_heartbeat > warning_threshold {
            return ResourceState::Unknown;
        }

        ResourceState::Active
    }
}

/// Rebuild all active allocations from the event ledger.
///
/// This is the rehydration function — called on daemon startup to restore
/// broker state after a crash or restart. It replays allocation events in
/// chronological order and returns only non-terminal allocations (those
/// still holding resources).
///
/// **Backward compatibility**: pre-refactor ledger entries that lack
/// `cpu_percent` in `AllocationGranted` will deserialize with the default
/// value (100.0) via serde's `#[serde(default)]`.
///
/// **What this recovers**:
/// - Allocation identity, purpose, resource claims
/// - TTL and timing (granted_at, last_heartbeat) → can compute expiry
/// - Terminal state (released/expired)
///
/// **What this does NOT recover** (operational, not durable):
/// - `cgroup_path`: cgroups are per-boot; a restarted daemon creates new ones
/// - Whether the client process is actually still running (that's what heartbeats prove)
pub fn rehydrate_allocations(ledger: &EventLedger) -> Result<HashMap<String, Allocation>, String> {
    let events = ledger.all_events().map_err(|e| e.to_string())?;

    let mut projections: HashMap<String, Allocation> = HashMap::new();

    for entry in &events {
        match &entry.event {
            LedgerEvent::AllocationGranted {
                allocation_id,
                client_id,
                purpose,
                gpus,
                ram_mb,
                ttl_secs,
                cpu_percent,
            } => {
                let granted_at = entry.timestamp;
                projections.insert(
                    allocation_id.clone(),
                    Allocation {
                        id: allocation_id.clone(),
                        client_id: client_id.clone(),
                        purpose: purpose.clone(),
                        gpus: gpus.clone(),
                        cpu_percent: *cpu_percent,
                        ram_mb: *ram_mb,
                        ttl_secs: *ttl_secs,
                        granted_at,
                        expires_at: granted_at + Duration::seconds(*ttl_secs as i64),
                        last_heartbeat: granted_at,
                        cgroup_path: None, // Not recoverable — cgroups are per-boot
                        dream_cycle_id: None, // Could be recovered if added to event
                        end_reason: None,
                    },
                );
            }

            LedgerEvent::AllocationHeartbeat { allocation_id, .. } => {
                if let Some(alloc) = projections.get_mut(allocation_id) {
                    alloc.last_heartbeat = entry.timestamp;
                    // Sliding window TTL: each heartbeat extends from now
                    alloc.expires_at = entry.timestamp + Duration::seconds(alloc.ttl_secs as i64);
                }
            }

            LedgerEvent::AllocationReleased {
                allocation_id, reason, ..
            } => {
                if let Some(alloc) = projections.get_mut(allocation_id) {
                    alloc.end_reason = Some(reason.clone());
                }
            }

            LedgerEvent::AllocationExpired {
                allocation_id,
                last_heartbeat,
                expired_at,
                ..
            } => {
                if let Some(alloc) = projections.get_mut(allocation_id) {
                    alloc.end_reason = Some("expired".to_string());
                    alloc.last_heartbeat = *last_heartbeat;
                    alloc.expires_at = *expired_at;
                }
            }

            // Ignore non-allocation events (snapshots, idle detection, dreams, etc.)
            _ => {}
        }
    }

    // Filter to only non-terminal allocations — these are the ones still holding resources
    let active: HashMap<String, Allocation> = projections
        .into_iter()
        .filter(|(_, a)| a.end_reason.is_none())
        .collect();

    Ok(active)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ledger::{LedgerConfig, LedgerEvent};
    use tempfile::tempdir;

    struct TestLedger {
        ledger: EventLedger,
        _dir: tempfile::TempDir, // Keep alive for the test duration
    }

    fn test_ledger_with_events(events: Vec<LedgerEvent>) -> TestLedger {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.jsonl");
        let config = LedgerConfig {
            path,
            retention_days: 30,
        };
        let mut ledger = EventLedger::open(config).unwrap();
        for event in events {
            ledger.append(event).unwrap();
        }
        TestLedger {
            ledger,
            _dir: dir,
        }
    }

    #[test]
    fn test_rehydration_empty_ledger() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("empty.jsonl");
        let config = LedgerConfig {
            path,
            retention_days: 30,
        };
        let ledger = EventLedger::open(config).unwrap();

        let allocations = rehydrate_allocations(&ledger).unwrap();
        assert!(allocations.is_empty());
    }

    #[test]
    fn test_rehydration_single_grant() {
        let now = Utc::now();
        let test_ledger = test_ledger_with_events(vec![LedgerEvent::AllocationGranted {
            allocation_id: "alloc-1".into(),
            client_id: "client-a".into(),
            purpose: "training".into(),
            gpus: vec![0],
            ram_mb: 4096,
            ttl_secs: 3600,
            cpu_percent: 50.0,
        }]);

        let allocations = rehydrate_allocations(&test_ledger.ledger).unwrap();
        assert_eq!(allocations.len(), 1);

        let alloc = &allocations["alloc-1"];
        assert_eq!(alloc.client_id, "client-a");
        assert_eq!(alloc.purpose, "training");
        assert_eq!(alloc.gpus, vec![0]);
        assert_eq!(alloc.ram_mb, 4096);
        assert_eq!(alloc.ttl_secs, 3600);
        assert_eq!(alloc.cpu_percent, 50.0);
        assert!(alloc.end_reason.is_none());

        // Should be Active right after grant
        assert_eq!(alloc.current_state(Utc::now()), ResourceState::Active);
    }

    #[test]
    fn test_rehydration_with_heartbeat_extends_ttl() {
        let events = vec![
            LedgerEvent::AllocationGranted {
                allocation_id: "alloc-1".into(),
                client_id: "client-a".into(),
                purpose: "inference".into(),
                gpus: vec![],
                ram_mb: 2048,
                ttl_secs: 60,
                cpu_percent: 100.0,
            },
            LedgerEvent::AllocationHeartbeat {
                allocation_id: "alloc-1".into(),
                client_id: "client-a".into(),
            },
        ];
        let test_ledger = test_ledger_with_events(events);

        let allocations = rehydrate_allocations(&test_ledger.ledger).unwrap();
        let alloc = &allocations["alloc-1"];

        // Heartbeat should have updated last_heartbeat and extended expires_at
        assert!(alloc.last_heartbeat >= alloc.granted_at);
        assert_eq!(alloc.current_state(Utc::now()), ResourceState::Active);
    }

    #[test]
    fn test_rehydration_excludes_released() {
        let events = vec![
            LedgerEvent::AllocationGranted {
                allocation_id: "alloc-1".into(),
                client_id: "client-a".into(),
                purpose: "training".into(),
                gpus: vec![0],
                ram_mb: 4096,
                ttl_secs: 3600,
                cpu_percent: 100.0,
            },
            LedgerEvent::AllocationReleased {
                allocation_id: "alloc-1".into(),
                client_id: "client-a".into(),
                reason: "done".into(),
                gpus: vec![0],
            },
        ];
        let test_ledger = test_ledger_with_events(events);

        let allocations = rehydrate_allocations(&test_ledger.ledger).unwrap();
        // Released allocation should be filtered out
        assert!(allocations.is_empty());
    }

    #[test]
    fn test_rehydration_excludes_expired() {
        let now = Utc::now();
        let events = vec![
            LedgerEvent::AllocationGranted {
                allocation_id: "alloc-1".into(),
                client_id: "client-a".into(),
                purpose: "training".into(),
                gpus: vec![0],
                ram_mb: 4096,
                ttl_secs: 3600,
                cpu_percent: 100.0,
            },
            LedgerEvent::AllocationExpired {
                allocation_id: "alloc-1".into(),
                client_id: "client-a".into(),
                last_heartbeat: now,
                expired_at: now + Duration::seconds(3600),
                gpus: vec![0],
            },
        ];
        let test_ledger = test_ledger_with_events(events);

        let allocations = rehydrate_allocations(&test_ledger.ledger).unwrap();
        // Expired allocation should be filtered out
        assert!(allocations.is_empty());
    }

    #[test]
    fn test_current_state_transitions() {
        let now = Utc::now();
        let mut alloc = Allocation {
            id: "alloc-1".into(),
            client_id: "client-a".into(),
            purpose: "test".into(),
            gpus: vec![],
            cpu_percent: 100.0,
            ram_mb: 1024,
            ttl_secs: 100,
            granted_at: now,
            expires_at: now + Duration::seconds(100),
            last_heartbeat: now,
            cgroup_path: None,
            dream_cycle_id: None,
            end_reason: None,
        };

        // Fresh grant: Active
        assert_eq!(alloc.current_state(now), ResourceState::Active);

        // After 40s (less than TTL/2 = 50s): still Active
        assert_eq!(alloc.current_state(now + Duration::seconds(40)), ResourceState::Active);

        // After 60s (more than TTL/2, less than TTL): Unknown
        assert_eq!(alloc.current_state(now + Duration::seconds(60)), ResourceState::Unknown);

        // After 110s (past TTL): Expired
        assert_eq!(alloc.current_state(now + Duration::seconds(110)), ResourceState::Expired);

        // Released: terminal
        alloc.end_reason = Some("done".into());
        assert_eq!(alloc.current_state(now), ResourceState::Released);
    }

    #[test]
    fn test_backward_compat_missing_cpu_percent() {
        // Simulate a pre-refactor ledger entry without cpu_percent
        let dir = tempdir().unwrap();
        let path = dir.path().join("legacy.jsonl");
        
        let legacy_entry = r#"{"id":"evt-0001","timestamp":"2026-11-14T15:00:00Z","event":{"type":"allocation_granted","allocation_id":"alloc-legacy","client_id":"old-client","purpose":"test","gpus":[],"ram_mb":1024,"ttl_secs":300}}"#;
        std::fs::write(&path, legacy_entry).unwrap();

        let config = LedgerConfig {
            path,
            retention_days: 30,
        };
        let ledger = EventLedger::open(config).unwrap();

        // Should deserialize with default cpu_percent (100.0)
        let allocations = rehydrate_allocations(&ledger).unwrap();
        assert_eq!(allocations.len(), 1);
        assert_eq!(allocations["alloc-legacy"].cpu_percent, 100.0);
    }
}
