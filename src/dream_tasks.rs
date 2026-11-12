//! Dream task framework (Phase 3).
//!
//! Provides the contract and infrastructure for dream tasks — units of
//! background work that execute during dream cycles. rIOd does not implement
//! dream tasks itself; it provides:
//!
//! - `DreamTaskContext`: execution context passed to tasks (cycle ID, time budget)
//! - `DreamTaskResult`: structured outcome of a task execution
//! - `DreamTaskRegistry`: metadata about registered dream tasks
//! - Ledger events for task lifecycle tracking
//! - `dream.report` API method for consumers to submit results
//!
//! External consumers (crabjar, etc.) subscribe to dream cycle events,
//! allocate resources with the cycle ID, execute their work, and report
//! results back via the API. When the cycle ends, preemption releases
//! all tagged allocations automatically.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Context provided to a dream task at execution time.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DreamTaskContext {
    /// Unique identifier for the current dream cycle.
    pub cycle_id: String,
    /// When this dream cycle started.
    pub cycle_started_at: DateTime<Utc>,
    /// Maximum duration for this dream cycle in minutes.
    pub max_cycle_duration_mins: u64,
    /// The client ID of the consumer executing this task.
    pub client_id: String,
    /// Optional allocation ID if resources were allocated for this task.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub allocation_id: Option<String>,
}

/// Outcome of a dream task execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DreamTaskResult {
    /// Name of the dream task that produced this result.
    pub task_name: String,
    /// Cycle ID this result belongs to.
    pub cycle_id: String,
    /// Client that executed the task.
    pub client_id: String,
    /// When the task completed.
    pub completed_at: DateTime<Utc>,
    /// How long the task ran in seconds.
    pub duration_secs: u64,
    /// Whether the task succeeded.
    pub success: bool,
    /// Optional error message if the task failed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Structured observations produced by the task.
    /// These are unverified and go into the staged state model —
    /// they require promotion before being treated as facts.
    pub observations: Vec<DreamObservation>,
}

/// A single observation produced by a dream task.
/// Observations are unverified hypotheses, not facts.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DreamObservation {
    /// Unique identifier for this observation.
    pub id: String,
    /// Category of the observation (e.g., "pattern", "anomaly", "optimization").
    pub category: String,
    /// Human-readable description of what was observed.
    pub description: String,
    /// Confidence level 0.0–1.0 (how sure the task is about this observation).
    pub confidence: f32,
    /// Supporting evidence or data points.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub evidence: Option<serde_json::Value>,
    /// Suggested action if any (e.g., "investigate", "promote", "ignore").
    #[serde(skip_serializing_if = "Option::is_none")]
    pub suggested_action: Option<String>,
}

/// Metadata about a registered dream task.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DreamTaskInfo {
    /// Unique name of the dream task.
    pub name: String,
    /// Human-readable description of what this task does.
    pub description: String,
    /// The client/consumer that provides this task.
    pub provider: String,
    /// Estimated duration in minutes (for scheduling heuristics).
    pub estimated_duration_mins: u64,
    /// Whether this task requires GPU resources.
    pub requires_gpu: bool,
    /// Minimum idle threshold before this task should run (overrides global).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_idle_threshold_mins: Option<u64>,
}

/// Registry of available dream tasks.
///
/// Consumers register their tasks here so that the `dream.tasks` API method
/// can list what's available. This is informational — rIOd does not schedule
/// or execute tasks based on this registry.
#[derive(Debug, Default)]
pub struct DreamTaskRegistry {
    tasks: HashMap<String, DreamTaskInfo>,
}

impl DreamTaskRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a dream task. Returns an error if the name is already taken.
    pub fn register(&mut self, info: DreamTaskInfo) -> Result<(), String> {
        if self.tasks.contains_key(&info.name) {
            return Err(format!("dream task '{}' already registered", info.name));
        }
        tracing::info!(task = %info.name, provider = %info.provider, "dream task registered");
        self.tasks.insert(info.name.clone(), info);
        Ok(())
    }

    /// Remove a dream task by name.
    pub fn unregister(&mut self, name: &str) -> Option<DreamTaskInfo> {
        let removed = self.tasks.remove(name);
        if removed.is_some() {
            tracing::info!(task = %name, "dream task unregistered");
        }
        removed
    }

    /// Get info for a specific task.
    pub fn get(&self, name: &str) -> Option<&DreamTaskInfo> {
        self.tasks.get(name)
    }

    /// List all registered tasks.
    pub fn list(&self) -> Vec<&DreamTaskInfo> {
        self.tasks.values().collect()
    }

    /// Number of registered tasks.
    pub fn len(&self) -> usize {
        self.tasks.len()
    }

    pub fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }
}

/// Ledger events for dream task lifecycle.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "task_event", rename_all = "snake_case")]
pub enum DreamTaskEvent {
    /// A dream task started execution.
    Started {
        at: DateTime<Utc>,
        task_name: String,
        cycle_id: String,
        client_id: String,
        allocation_id: Option<String>,
    },
    /// A dream task completed (success or failure).
    Completed {
        at: DateTime<Utc>,
        task_name: String,
        cycle_id: String,
        client_id: String,
        success: bool,
        duration_secs: u64,
        observation_count: usize,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_task_info(name: &str) -> DreamTaskInfo {
        DreamTaskInfo {
            name: name.to_string(),
            description: format!("Test task {}", name),
            provider: "test-provider".to_string(),
            estimated_duration_mins: 5,
            requires_gpu: false,
            min_idle_threshold_mins: None,
        }
    }

    #[test]
    fn test_registry_register_and_list() {
        let mut registry = DreamTaskRegistry::new();
        assert!(registry.is_empty());

        registry.register(test_task_info("task-a")).unwrap();
        registry.register(test_task_info("task-b")).unwrap();

        assert_eq!(registry.len(), 2);
        let tasks = registry.list();
        assert_eq!(tasks.len(), 2);
    }

    #[test]
    fn test_registry_duplicate_rejected() {
        let mut registry = DreamTaskRegistry::new();
        registry.register(test_task_info("task-a")).unwrap();

        let err = registry.register(test_task_info("task-a"));
        assert!(err.is_err());
        assert!(err.unwrap_err().contains("already registered"));
    }

    #[test]
    fn test_registry_unregister() {
        let mut registry = DreamTaskRegistry::new();
        registry.register(test_task_info("task-a")).unwrap();
        assert_eq!(registry.len(), 1);

        let removed = registry.unregister("task-a");
        assert!(removed.is_some());
        assert_eq!(registry.len(), 0);
    }

    #[test]
    fn test_registry_get() {
        let mut registry = DreamTaskRegistry::new();
        registry.register(test_task_info("task-a")).unwrap();

        assert!(registry.get("task-a").is_some());
        assert!(registry.get("nonexistent").is_none());
    }

    #[test]
    fn test_dream_task_result_serialization() {
        let result = DreamTaskResult {
            task_name: "pattern-analysis".to_string(),
            cycle_id: "dream-cycle-00000001".to_string(),
            client_id: "crabjar-worker-1".to_string(),
            completed_at: Utc::now(),
            duration_secs: 120,
            success: true,
            error: None,
            observations: vec![DreamObservation {
                id: "obs-001".to_string(),
                category: "pattern".to_string(),
                description: "User tends to run training jobs on weekends".to_string(),
                confidence: 0.85,
                evidence: Some(serde_json::json!({ "samples": 12 })),
                suggested_action: Some("promote".to_string()),
            }],
        };

        let json = serde_json::to_string(&result).unwrap();
        let parsed: DreamTaskResult = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.task_name, "pattern-analysis");
        assert_eq!(parsed.observations.len(), 1);
        assert_eq!(parsed.observations[0].confidence, 0.85);
    }

    #[test]
    fn test_dream_task_context_serialization() {
        let ctx = DreamTaskContext {
            cycle_id: "dream-cycle-00000001".to_string(),
            cycle_started_at: Utc::now(),
            max_cycle_duration_mins: 30,
            client_id: "crabjar-worker-1".to_string(),
            allocation_id: Some("alloc-00000001".to_string()),
        };

        let json = serde_json::to_string(&ctx).unwrap();
        let parsed: DreamTaskContext = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.cycle_id, "dream-cycle-00000001");
        assert_eq!(parsed.allocation_id, Some("alloc-00000001".to_string()));
    }

    #[test]
    fn test_dream_task_event_serialization() {
        let event = DreamTaskEvent::Started {
            at: Utc::now(),
            task_name: "pattern-analysis".to_string(),
            cycle_id: "dream-cycle-00000001".to_string(),
            client_id: "crabjar-worker-1".to_string(),
            allocation_id: Some("alloc-00000001".to_string()),
        };

        let json = serde_json::to_value(&event).unwrap();
        assert_eq!(json["task_event"], "started");
        assert_eq!(json["task_name"], "pattern-analysis");
    }
}
