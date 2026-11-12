//! Append-only event ledger with JSONL storage and query support.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use thiserror::Error;

/// Error types for ledger operations.
#[derive(Debug, Error)]
pub enum LedgerError {
    #[error("failed to open ledger file: {0}")]
    OpenFailed(String),

    #[error("failed to write event: {0}")]
    WriteFailed(String),

    #[error("failed to read event: {0}")]
    ReadFailed(String),

    #[error("invalid JSON in ledger: {0}")]
    InvalidJson(String),
}

/// Types of events that can be recorded in the ledger.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum LedgerEvent {
    /// Periodic resource snapshot from the observer.
    ResourceSnapshot { data: serde_json::Value },

    /// System entered idle state after sustained low utilization.
    IdleDetected {
        idle_since: DateTime<Utc>,
        cpu_percent: f32,
        gpu_utilization: Vec<u8>,
    },

    /// System exited idle state (activity resumed).
    IdleEnded {
        idle_duration_secs: u64,
        trigger: String,
    },

    /// Daemon started.
    DaemonStarted {
        version: String,
        socket_path: String,
        ledger_path: String,
        poll_interval_s: u64,
    },

    /// Daemon shutting down.
    DaemonStopped { reason: String },

    /// Generic informational event (for future phases).
    Info {
        message: String,
        data: Option<serde_json::Value>,
    },

    /// A resource allocation was granted to a client.
    AllocationGranted {
        allocation_id: String,
        client_id: String,
        purpose: String,
        gpus: Vec<u32>,
        ram_mb: u64,
        ttl_secs: u64,
    },

    /// A resource allocation was released (by client or force).
    AllocationReleased {
        allocation_id: String,
        client_id: String,
        reason: String,
        gpus: Vec<u32>,
    },

    /// A client sent a heartbeat to extend its allocation TTL.
    AllocationHeartbeat {
        allocation_id: String,
        client_id: String,
    },
}

impl LedgerEvent {
    pub fn timestamp(&self) -> DateTime<Utc> {
        // All events are stamped at creation; this is a convenience accessor
        // that would be populated by the ledger wrapper. For now, return current time
        // as a placeholder — the actual timestamp is stored in the envelope.
        Utc::now()
    }
}

/// Envelope wrapping every ledger entry with metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LedgerEntry {
    pub id: String,
    pub timestamp: DateTime<Utc>,
    pub event: LedgerEvent,
}

/// Configuration for the event ledger.
#[derive(Debug, Clone)]
pub struct LedgerConfig {
    /// Path to the JSONL ledger file.
    pub path: PathBuf,
    /// Maximum age of events to retain (in days). Older entries are pruned on rotation.
    pub retention_days: u32,
}

impl Default for LedgerConfig {
    fn default() -> Self {
        Self {
            path: PathBuf::from("/var/log/riod/events.jsonl"),
            retention_days: 30,
        }
    }
}

/// Append-only event ledger backed by a JSONL file.
pub struct EventLedger {
    config: LedgerConfig,
    /// Monotonic counter for entry IDs (rehydrated from file on startup).
    next_id: u64,
}

impl EventLedger {
    /// Open or create the ledger at the configured path.
    pub fn open(config: LedgerConfig) -> Result<Self, LedgerError> {
        // Ensure parent directory exists
        if let Some(parent) = config.path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                LedgerError::OpenFailed(format!("create dir {}: {}", parent.display(), e))
            })?;
        }

        // Rehydrate ID counter from existing entries
        let next_id = if config.path.exists() {
            let max_id = read_max_entry_id(&config.path)?;
            max_id + 1
        } else {
            1
        };

        tracing::info!(path = %config.path.display(), next_id, "ledger opened");

        Ok(Self { config, next_id })
    }

    /// Append an event to the ledger. Blocks on disk I/O (fsync for durability).
    pub fn append(&mut self, event: LedgerEvent) -> Result<LedgerEntry, LedgerError> {
        let entry = LedgerEntry {
            id: format!("evt-{:012x}", self.next_id),
            timestamp: Utc::now(),
            event,
        };

        let line = serde_json::to_string(&entry)
            .map_err(|e| LedgerError::WriteFailed(format!("serialize: {}", e)))?;

        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.config.path)
            .map_err(|e| LedgerError::OpenFailed(e.to_string()))?;

        file.write_all(line.as_bytes())
            .and_then(|()| file.write_all(b"\n"))
            .and_then(|()| file.sync_all())
            .map_err(|e| LedgerError::WriteFailed(e.to_string()))?;

        self.next_id += 1;

        Ok(entry)
    }

    /// Query events from the ledger with optional filters.
    pub fn query(
        &self,
        since: Option<DateTime<Utc>>,
        event_type: Option<&str>,
        limit: usize,
    ) -> Result<Vec<LedgerEntry>, LedgerError> {
        if !self.config.path.exists() {
            return Ok(Vec::new());
        }

        let file =
            File::open(&self.config.path).map_err(|e| LedgerError::ReadFailed(e.to_string()))?;
        let reader = BufReader::new(file);

        let mut results = Vec::new();
        let mut count = 0;

        for line in reader.lines() {
            let line = line.map_err(|e| LedgerError::ReadFailed(e.to_string()))?;
            if line.is_empty() {
                continue;
            }

            let entry: LedgerEntry =
                serde_json::from_str(&line).map_err(|e| LedgerError::InvalidJson(e.to_string()))?;

            // Apply filters
            if let Some(since_ts) = since {
                if entry.timestamp < since_ts {
                    continue;
                }
            }

            if let Some(type_filter) = event_type {
                let entry_type = serde_json::to_value(&entry.event).ok().and_then(|v| {
                    v.get("type")
                        .and_then(|t| t.as_str())
                        .map(|s| s.to_string())
                });
                if entry_type.as_deref() != Some(type_filter) {
                    continue;
                }
            }

            results.push(entry);
            count += 1;

            if count >= limit {
                break;
            }
        }

        // Return in chronological order (file is append-only, so it's already ordered)
        Ok(results)
    }

    /// Get the tail of the ledger (most recent N entries).
    pub fn tail(&self, n: usize) -> Result<Vec<LedgerEntry>, LedgerError> {
        if !self.config.path.exists() {
            return Ok(Vec::new());
        }

        let file =
            File::open(&self.config.path).map_err(|e| LedgerError::ReadFailed(e.to_string()))?;
        let reader = BufReader::new(file);

        let mut all_entries = Vec::new();
        for line in reader.lines() {
            let line = line.map_err(|e| LedgerError::ReadFailed(e.to_string()))?;
            if line.is_empty() {
                continue;
            }
            if let Ok(entry) = serde_json::from_str::<LedgerEntry>(&line) {
                all_entries.push(entry);
            }
        }

        // Return the last N entries
        let start = all_entries.len().saturating_sub(n);
        Ok(all_entries[start..].to_vec())
    }

    /// Get total entry count (approximate — counts lines).
    pub fn entry_count(&self) -> Result<u64, LedgerError> {
        if !self.config.path.exists() {
            return Ok(0);
        }

        let file =
            File::open(&self.config.path).map_err(|e| LedgerError::ReadFailed(e.to_string()))?;
        let reader = BufReader::new(file);

        Ok(reader.lines().count() as u64)
    }
}

/// Read the maximum entry ID from an existing ledger file (for rehydration).
fn read_max_entry_id(path: &Path) -> Result<u64, LedgerError> {
    if !path.exists() {
        return Ok(0);
    }

    let file = File::open(path).map_err(|e| LedgerError::ReadFailed(e.to_string()))?;
    let reader = BufReader::new(file);

    let mut max_id: u64 = 0;
    for line in reader.lines() {
        let line = line.map_err(|e| LedgerError::ReadFailed(e.to_string()))?;
        if line.is_empty() {
            continue;
        }
        if let Ok(entry) = serde_json::from_str::<LedgerEntry>(&line) {
            // Parse ID from "evt-XXXXXXXXXXXX" format
            if let Some(hex_part) = entry.id.strip_prefix("evt-") {
                if let Ok(id) = u64::from_str_radix(hex_part, 16) {
                    max_id = max_id.max(id);
                }
            }
        }
    }

    Ok(max_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_ledger_append_and_query() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test-ledger.jsonl");

        let config = LedgerConfig {
            path: path.clone(),
            retention_days: 7,
        };

        let mut ledger = EventLedger::open(config).unwrap();

        // Append some events
        ledger
            .append(LedgerEvent::Info {
                message: "test event 1".into(),
                data: None,
            })
            .unwrap();

        ledger
            .append(LedgerEvent::DaemonStarted {
                version: "0.1.0".into(),
                socket_path: "/run/riod.sock".into(),
                ledger_path: path.to_string_lossy().into(),
                poll_interval_s: 30,
            })
            .unwrap();

        // Query all events
        let events = ledger.query(None, None, 100).unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].id, "evt-000000000001");
        assert_eq!(events[1].id, "evt-000000000002");

        // Query by type
        let info_events = ledger.query(None, Some("info"), 100).unwrap();
        assert_eq!(info_events.len(), 1);

        // Tail
        let tail = ledger.tail(1).unwrap();
        assert_eq!(tail.len(), 1);
        assert_eq!(tail[0].id, "evt-000000000002");

        // Entry count
        assert_eq!(ledger.entry_count().unwrap(), 2);
    }

    #[test]
    fn test_ledger_rehydration() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test-ledger.jsonl");

        let config = LedgerConfig {
            path: path.clone(),
            retention_days: 7,
        };

        // Create ledger and add events
        {
            let mut ledger = EventLedger::open(config.clone()).unwrap();
            for i in 0..5 {
                ledger
                    .append(LedgerEvent::Info {
                        message: format!("event {}", i),
                        data: None,
                    })
                    .unwrap();
            }
        }

        // Reopen — should rehydrate ID counter
        let mut ledger = EventLedger::open(config).unwrap();
        let entry = ledger
            .append(LedgerEvent::Info {
                message: "after reopen".into(),
                data: None,
            })
            .unwrap();

        assert_eq!(entry.id, "evt-000000000006");
    }

    #[test]
    fn test_ledger_since_filter() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test-ledger.jsonl");

        let config = LedgerConfig {
            path,
            retention_days: 7,
        };

        let mut ledger = EventLedger::open(config).unwrap();

        // Add an event, wait a tiny bit, add another
        ledger
            .append(LedgerEvent::Info {
                message: "old".into(),
                data: None,
            })
            .unwrap();

        std::thread::sleep(std::time::Duration::from_millis(10));

        let cutoff = Utc::now();
        std::thread::sleep(std::time::Duration::from_millis(10));

        ledger
            .append(LedgerEvent::Info {
                message: "new".into(),
                data: None,
            })
            .unwrap();

        // Query since cutoff — should only get the second event
        let recent = ledger.query(Some(cutoff), None, 100).unwrap();
        assert_eq!(recent.len(), 1);
        assert_eq!(recent[0].event.as_info().map(|(m, _)| m), Some("new"));
    }

    // Helper to extract Info event data
    trait LedgerEventExt {
        fn as_info(&self) -> Option<(&str, &Option<serde_json::Value>)>;
    }

    impl LedgerEventExt for LedgerEvent {
        fn as_info(&self) -> Option<(&str, &Option<serde_json::Value>)> {
            match self {
                LedgerEvent::Info { message, data } => Some((message, data)),
                _ => None,
            }
        }
    }
}
