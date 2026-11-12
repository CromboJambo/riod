//! Phase 2: Purpose-based authorization via declarative TOML policies.
//!
//! Clients declare an *intent* (why they want resources) before allocating.
//! The policy engine evaluates the intent against configured rules and
//! grants/denies based on current system state + policy conditions.

use crate::observe::{IdleDetector, ResourceSnapshot};
use chrono::{DateTime, Timelike, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use thiserror::Error;

/// Error types for policy operations.
#[derive(Debug, Error)]
pub enum PolicyError {
    #[error("failed to read policy file: {0}")]
    ReadFailed(String),

    #[error("failed to parse TOML: {0}")]
    ParseFailed(String),

    #[error("invalid policy '{name}': {detail}")]
    InvalidPolicy { name: String, detail: String },

    #[error("no matching policy for purpose: {0}")]
    NoMatchingPolicy(String),

    #[error("intent not found: {0}")]
    IntentNotFound(String),

    #[error("intent denied by policy '{policy}': {reason}")]
    Denied { policy: String, reason: String },
}

/// Purpose categories for resource requests.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Purpose {
    Training,
    Inference,
    Dreaming,
    Maintenance,
    #[serde(untagged)]
    Custom(String),
}

impl std::fmt::Display for Purpose {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Purpose::Training => write!(f, "training"),
            Purpose::Inference => write!(f, "inference"),
            Purpose::Dreaming => write!(f, "dreaming"),
            Purpose::Maintenance => write!(f, "maintenance"),
            Purpose::Custom(s) => write!(f, "{s}"),
        }
    }
}

/// A condition that must be satisfied for a policy to grant an intent.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Condition {
    /// System must have been idle for at least this many seconds.
    IdleDuration { min_secs: u64 },
    /// GPU utilization must be below this percentage (all GPUs).
    GpuUtilization { max_percent: u8 },
    /// At least this percentage of total VRAM must be free.
    VramFree { min_percent: u8 },
    /// CPU usage must be below this percentage.
    CpuUsage { max_percent: f32 },
    /// Current hour (UTC) must be in [start, end) range.
    HourRange { start: u8, end: u8 },
    /// RAM usage must be below this percentage of total.
    RamUsage { max_percent: u8 },
}

impl Condition {
    /// Evaluate this condition against current system state.
    pub fn evaluate(&self, snapshot: &ResourceSnapshot, detector: &IdleDetector) -> bool {
        match self {
            Condition::IdleDuration { min_secs } => {
                let idle_secs = detector.idle_since.map_or(0, |since| {
                    (Utc::now() - since).num_seconds().max(0) as u64
                });
                idle_secs >= *min_secs
            }
            Condition::GpuUtilization { max_percent } => {
                if snapshot.gpus.is_empty() {
                    // No GPUs — condition is vacuously true (no GPU contention possible)
                    return true;
                }
                snapshot
                    .gpus
                    .iter()
                    .all(|g| g.utilization_percent <= *max_percent)
            }
            Condition::VramFree { min_percent } => {
                if snapshot.gpus.is_empty() {
                    return true;
                }
                let total_vram: u64 = snapshot.gpus.iter().map(|g| g.vram_total_mb).sum();
                let used_vram: u64 = snapshot.gpus.iter().map(|g| g.vram_used_mb).sum();
                if total_vram == 0 {
                    return true;
                }
                let free_percent = ((total_vram - used_vram) * 100) / total_vram;
                free_percent >= *min_percent as u64
            }
            Condition::CpuUsage { max_percent } => snapshot.cpu_usage_percent <= *max_percent,
            Condition::HourRange { start, end } => {
                let hour = Utc::now().hour();
                let (start, end) = (*start as u32, *end as u32);
                if start <= end {
                    hour >= start && hour < end
                } else {
                    // Wraps midnight: e.g. [22, 6) means 22:00-05:59
                    hour >= start || hour < end
                }
            }
            Condition::RamUsage { max_percent } => {
                if snapshot.ram_total_mb == 0 {
                    return true;
                }
                let used_percent = (snapshot.ram_used_mb * 100) / snapshot.ram_total_mb;
                used_percent <= *max_percent as u64
            }
        }
    }

    /// Human-readable description of this condition.
    pub fn describe(&self) -> String {
        match self {
            Condition::IdleDuration { min_secs } => format!("idle for >= {min_secs}s"),
            Condition::GpuUtilization { max_percent } => {
                format!("GPU utilization <= {max_percent}%")
            }
            Condition::VramFree { min_percent } => format!("VRAM free >= {min_percent}%"),
            Condition::CpuUsage { max_percent } => format!("CPU usage <= {max_percent}%"),
            Condition::HourRange { start, end } => format!("hour in [{start}, {end})"),
            Condition::RamUsage { max_percent } => format!("RAM usage <= {max_percent}%"),
        }
    }
}

/// A declarative policy rule loaded from TOML.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Policy {
    pub purpose: Purpose,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub conditions: Vec<Condition>,
    /// Maximum duration (seconds) an allocation under this policy can last.
    #[serde(default)]
    pub max_duration_s: Option<u64>,
    /// Higher priority policies are evaluated first. Default 0.
    #[serde(default)]
    pub priority: i32,
}

/// Status of a declared intent.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum IntentStatus {
    Granted,
    Denied(String),
    Expired,
    Consumed, // Allocation was made against this intent
}

/// A client's declared intent to use resources for a specific purpose.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Intent {
    pub id: String,
    pub client_id: String,
    pub purpose: Purpose,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub estimated_duration_s: Option<u64>,
    #[serde(default)]
    pub metadata: serde_json::Value,
    pub status: IntentStatus,
    pub declared_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub policy_applied: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub granted_at: Option<DateTime<Utc>>,
    /// Which conditions failed (if denied).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failed_conditions: Option<Vec<String>>,
}

/// Policy store: holds loaded policies and declared intents.
pub struct PolicyEngine {
    policies: Vec<(String, Policy)>, // (name, policy)
    intents: HashMap<String, Intent>,
    policy_path: Option<PathBuf>,
}

impl PolicyEngine {
    /// Create a new empty policy engine.
    pub fn new() -> Self {
        Self {
            policies: Vec::new(),
            intents: HashMap::new(),
            policy_path: None,
        }
    }

    /// Load policies from a TOML file.
    ///
    /// Expected format:
    /// ```toml
    /// [policy.training-during-idle]
    /// purpose = "training"
    /// description = "Allow training when system is idle"
    /// conditions = [
    ///     { type = "idle_duration", min_secs = 600 },
    ///     { type = "gpu_utilization", max_percent = 20 },
    /// ]
    /// max_duration_s = 14400
    /// priority = 10
    /// ```
    pub fn load_from_file(&mut self, path: &Path) -> Result<usize, PolicyError> {
        let content = std::fs::read_to_string(path).map_err(|e| {
            PolicyError::ReadFailed(format!("{}: {e}", path.display()))
        })?;

        let loaded = Self::parse_toml(&content)?;
        let count = loaded.len();

        // Replace existing policies
        self.policies = loaded;
        self.policy_path = Some(path.to_path_buf());

        tracing::info!(path = %path.display(), count, "policies loaded");
        Ok(count)
    }

    /// Parse TOML content into policies.
    pub fn parse_toml(content: &str) -> Result<Vec<(String, Policy)>, PolicyError> {
        let parsed: toml::Value =
            toml::from_str(content).map_err(|e| PolicyError::ParseFailed(e.to_string()))?;

        let policy_table = parsed
            .get("policy")
            .and_then(|v| v.as_table())
            .ok_or_else(|| {
                PolicyError::ParseFailed("missing [policy] table in TOML".to_string())
            })?;

        let mut policies = Vec::new();
        for (name, value) in policy_table {
            let policy: Policy = toml::Value::try_into(value.clone())
                .map_err(|e| PolicyError::InvalidPolicy {
                    name: name.clone(),
                    detail: e.to_string(),
                })?;

            // Validate
            if policy.conditions.is_empty() && policy.max_duration_s.is_none() {
                tracing::warn!(policy = name, "policy has no conditions or max duration — always grants");
            }

            policies.push((name.clone(), policy));
        }

        // Sort by priority (higher first)
        policies.sort_by(|a, b| b.1.priority.cmp(&a.1.priority));
        Ok(policies)
    }

    /// Reload policies from the configured file (for SIGHUP).
    pub fn reload(&mut self) -> Result<usize, PolicyError> {
        if let Some(path) = self.policy_path.clone() {
            self.load_from_file(&path)
        } else {
            Err(PolicyError::ReadFailed(
                "no policy file configured".to_string(),
            ))
        }
    }

    /// Get the number of loaded policies.
    pub fn policy_count(&self) -> usize {
        self.policies.len()
    }

    /// Evaluate an intent declaration against loaded policies.
    ///
    /// Returns the updated Intent with status set to Granted or Denied.
    pub fn evaluate_intent(
        &mut self,
        client_id: &str,
        purpose: Purpose,
        description: String,
        estimated_duration_s: Option<u64>,
        metadata: serde_json::Value,
        snapshot: &ResourceSnapshot,
        detector: &IdleDetector,
    ) -> Intent {
        let intent_id = format!("int-{}", uuid::Uuid::new_v4().as_simple());

        // Find all policies matching this purpose, sorted by priority (already sorted)
        let matching: Vec<&(String, Policy)> = self
            .policies
            .iter()
            .filter(|(_, p)| p.purpose == purpose)
            .collect();

        if matching.is_empty() {
            return Intent {
                id: intent_id,
                client_id: client_id.to_string(),
                purpose,
                description,
                estimated_duration_s,
                metadata,
                status: IntentStatus::Denied("no matching policy for this purpose".to_string()),
                declared_at: Utc::now(),
                policy_applied: None,
                granted_at: None,
                failed_conditions: None,
            };
        }

        // Try each policy in priority order
        for (policy_name, policy) in &matching {
            let mut failed = Vec::new();
            let all_pass = policy.conditions.iter().all(|c| {
                let pass = c.evaluate(snapshot, detector);
                if !pass {
                    failed.push(c.describe());
                }
                pass
            });

            // Check duration constraint
            if all_pass
                && policy.max_duration_s.is_some()
                && estimated_duration_s.is_some()
                && estimated_duration_s.unwrap() > policy.max_duration_s.unwrap()
            {
                failed.push(format!(
                    "estimated duration {}s exceeds policy max {}s",
                    estimated_duration_s.unwrap(),
                    policy.max_duration_s.unwrap()
                ));
            }

            if all_pass && failed.is_empty() {
                let intent = Intent {
                    id: intent_id.clone(),
                    client_id: client_id.to_string(),
                    purpose: purpose.clone(),
                    description,
                    estimated_duration_s,
                    metadata,
                    status: IntentStatus::Granted,
                    declared_at: Utc::now(),
                    policy_applied: Some(policy_name.clone()),
                    granted_at: Some(Utc::now()),
                    failed_conditions: None,
                };
                self.intents.insert(intent_id.clone(), intent.clone());
                tracing::info!(
                    intent_id,
                    client_id,
                    purpose = %purpose,
                    policy = policy_name,
                    "intent granted"
                );
                return intent;
            }

            // Remember failures from highest-priority policy for the denial reason
            if !failed.is_empty() {
                let _ = &failed; // Will use in final denial if no policy matches
            }
        }

        // No policy granted — deny with conditions from highest-priority match
        let top_policy = &matching[0];
        let failed: Vec<String> = top_policy
            .1
            .conditions
            .iter()
            .filter(|c| !c.evaluate(snapshot, detector))
            .map(|c| c.describe())
            .collect();

        let intent = Intent {
            id: intent_id.clone(),
            client_id: client_id.to_string(),
            purpose: purpose.clone(),
            description,
            estimated_duration_s,
            metadata,
            status: IntentStatus::Denied(format!(
                "no policy conditions satisfied (checked {} policies)",
                matching.len()
            )),
            declared_at: Utc::now(),
            policy_applied: Some(top_policy.0.clone()),
            granted_at: None,
            failed_conditions: if failed.is_empty() {
                None
            } else {
                Some(failed)
            },
        };
        self.intents.insert(intent_id.clone(), intent.clone());
        tracing::info!(
            intent_id,
            client_id,
            purpose = %purpose,
            policy = top_policy.0,
            "intent denied"
        );
        intent
    }

    /// Look up an intent by ID.
    pub fn get_intent(&self, id: &str) -> Option<&Intent> {
        self.intents.get(id)
    }

    /// Mark an intent as consumed (allocation was made against it).
    pub fn consume_intent(&mut self, id: &str) -> Result<(), PolicyError> {
        let intent = self
            .intents
            .get_mut(id)
            .ok_or(PolicyError::IntentNotFound(id.to_string()))?;

        if intent.status != IntentStatus::Granted {
            return Err(PolicyError::Denied {
                policy: intent.policy_applied.clone().unwrap_or_default(),
                reason: format!("intent is {:?}, not Granted", intent.status),
            });
        }

        intent.status = IntentStatus::Consumed;
        Ok(())
    }

    /// Verify an intent is valid for allocation: exists, granted, not expired, client matches.
    /// Returns the policy name if valid.
    pub fn verify_intent_for_allocation(&self, id: &str, client_id: &str) -> Result<String, PolicyError> {
        let intent = self
            .intents
            .get(id)
            .ok_or(PolicyError::IntentNotFound(id.to_string()))?;

        if intent.client_id != client_id {
            return Err(PolicyError::Denied {
                policy: intent.policy_applied.clone().unwrap_or_default(),
                reason: format!(
                    "intent belongs to client '{}', not '{}'",
                    intent.client_id, client_id
                ),
            });
        }

        if intent.status != IntentStatus::Granted {
            return Err(PolicyError::Denied {
                policy: intent.policy_applied.clone().unwrap_or_default(),
                reason: format!("intent status is {:?}, not Granted", intent.status),
            });
        }

        // Check expiry: granted intents are valid for 5 minutes
        if let Some(granted_at) = intent.granted_at {
            let age_secs = (Utc::now() - granted_at).num_seconds();
            if age_secs > 300 {
                return Err(PolicyError::Denied {
                    policy: intent.policy_applied.clone().unwrap_or_default(),
                    reason: format!("intent expired (granted {}s ago, max 300s)", age_secs),
                });
            }
        }

        Ok(intent.policy_applied.clone().unwrap_or_default())
    }

    /// List all intents (for debugging/monitoring).
    pub fn list_intents(&self) -> Vec<&Intent> {
        let mut intents: Vec<&Intent> = self.intents.values().collect();
        intents.sort_by(|a, b| b.declared_at.cmp(&a.declared_at));
        intents
    }

    /// Get policy names for a given purpose.
    pub fn policies_for_purpose(&self, purpose: &Purpose) -> Vec<&str> {
        self.policies
            .iter()
            .filter(|(_, p)| &p.purpose == purpose)
            .map(|(name, _)| name.as_str())
            .collect()
    }
}

impl Default for PolicyEngine {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::observe::{GpuInfo, ObserveConfig};

    fn test_snapshot() -> ResourceSnapshot {
        ResourceSnapshot {
            timestamp: Utc::now(),
            cpu_usage_percent: 2.5,
            ram_total_mb: 16384,
            ram_used_mb: 4096,
            gpus: vec![GpuInfo {
                index: 0,
                name: "Test GPU".into(),
                vram_total_mb: 16000,
                vram_used_mb: 2000,
                utilization_percent: 5,
            }],
        }
    }

    fn test_detector_idle() -> IdleDetector {
        let mut d = IdleDetector::new(ObserveConfig::default());
        d.is_system_idle = true;
        d.idle_since = Some(Utc::now() - chrono::Duration::seconds(1200));
        d
    }

    fn test_detector_busy() -> IdleDetector {
        let mut d = IdleDetector::new(ObserveConfig::default());
        d.is_system_idle = false;
        d.idle_since = None;
        d
    }

    #[test]
    fn test_parse_policies_toml() {
        let toml = r#"
[policy.training-idle]
purpose = "training"
description = "Training when idle"
conditions = [
    { type = "idle_duration", min_secs = 600 },
    { type = "gpu_utilization", max_percent = 20 },
]
max_duration_s = 14400
priority = 10

[policy.inference-anytime]
purpose = "inference"
description = "Always allow inference"
conditions = []
max_duration_s = 3600
priority = 5
"#;
        let policies = PolicyEngine::parse_toml(toml).unwrap();
        assert_eq!(policies.len(), 2);
        // Sorted by priority: training-idle (10) before inference-anytime (5)
        assert_eq!(policies[0].0, "training-idle");
        assert_eq!(policies[1].0, "inference-anytime");
    }

    #[test]
    fn test_condition_idle_duration() {
        let snap = test_snapshot();
        let idle_det = test_detector_idle();
        let busy_det = test_detector_busy();

        let c = Condition::IdleDuration { min_secs: 600 };
        assert!(c.evaluate(&snap, &idle_det)); // 1200s idle >= 600s
        assert!(!c.evaluate(&snap, &busy_det)); // not idle
    }

    #[test]
    fn test_condition_gpu_utilization() {
        let snap = test_snapshot(); // GPU at 5%
        let det = test_detector_idle();

        let c_pass = Condition::GpuUtilization { max_percent: 20 };
        let c_fail = Condition::GpuUtilization { max_percent: 3 };
        assert!(c_pass.evaluate(&snap, &det));
        assert!(!c_fail.evaluate(&snap, &det));
    }

    #[test]
    fn test_condition_vram_free() {
        let snap = test_snapshot(); // 14000/16000 free = 87.5%
        let det = test_detector_idle();

        let c_pass = Condition::VramFree { min_percent: 80 };
        let c_fail = Condition::VramFree { min_percent: 95 };
        assert!(c_pass.evaluate(&snap, &det));
        assert!(!c_fail.evaluate(&snap, &det));
    }

    #[test]
    fn test_condition_no_gpus_vacuously_true() {
        let mut snap = test_snapshot();
        snap.gpus = vec![];
        let det = test_detector_idle();

        assert!(Condition::GpuUtilization { max_percent: 0 }.evaluate(&snap, &det));
        assert!(Condition::VramFree { min_percent: 100 }.evaluate(&snap, &det));
    }

    #[test]
    fn test_intent_granted() {
        let mut engine = PolicyEngine::new();
        let toml = r#"
[policy.allow-training]
purpose = "training"
conditions = [
    { type = "idle_duration", min_secs = 600 },
]
"#;
        engine.policies = PolicyEngine::parse_toml(toml).unwrap();

        let snap = test_snapshot();
        let det = test_detector_idle();

        let intent = engine.evaluate_intent(
            "test-client",
            Purpose::Training,
            "Test training run".into(),
            Some(3600),
            serde_json::json!({}),
            &snap,
            &det,
        );

        assert!(matches!(intent.status, IntentStatus::Granted));
        assert_eq!(intent.policy_applied.as_deref(), Some("allow-training"));
    }

    #[test]
    fn test_intent_denied_not_idle() {
        let mut engine = PolicyEngine::new();
        let toml = r#"
[policy.allow-training]
purpose = "training"
conditions = [
    { type = "idle_duration", min_secs = 600 },
]
"#;
        engine.policies = PolicyEngine::parse_toml(toml).unwrap();

        let snap = test_snapshot();
        let det = test_detector_busy(); // Not idle

        let intent = engine.evaluate_intent(
            "test-client",
            Purpose::Training,
            "Test training run".into(),
            Some(3600),
            serde_json::json!({}),
            &snap,
            &det,
        );

        assert!(matches!(intent.status, IntentStatus::Denied(_)));
        assert!(intent.failed_conditions.unwrap().contains(&"idle for >= 600s".to_string()));
    }

    #[test]
    fn test_intent_no_matching_policy() {
        let mut engine = PolicyEngine::new();
        let toml = r#"
[policy.allow-inference]
purpose = "inference"
conditions = []
"#;
        engine.policies = PolicyEngine::parse_toml(toml).unwrap();

        let snap = test_snapshot();
        let det = test_detector_idle();

        let intent = engine.evaluate_intent(
            "test-client",
            Purpose::Training, // No training policy loaded
            "Test".into(),
            None,
            serde_json::json!({}),
            &snap,
            &det,
        );

        assert!(matches!(intent.status, IntentStatus::Denied(ref r) if r.contains("no matching policy")));
    }

    #[test]
    fn test_intent_consume() {
        let mut engine = PolicyEngine::new();
        let toml = r#"
[policy.allow-all]
purpose = "inference"
conditions = []
"#;
        engine.policies = PolicyEngine::parse_toml(toml).unwrap();

        let snap = test_snapshot();
        let det = test_detector_idle();

        let intent = engine.evaluate_intent(
            "test-client",
            Purpose::Inference,
            "Test".into(),
            None,
            serde_json::json!({}),
            &snap,
            &det,
        );
        assert!(matches!(intent.status, IntentStatus::Granted));

        // Consume it
        engine.consume_intent(&intent.id).unwrap();
        let consumed = engine.get_intent(&intent.id).unwrap();
        assert!(matches!(consumed.status, IntentStatus::Consumed));

        // Can't consume twice
        assert!(engine.consume_intent(&intent.id).is_err());
    }

    #[test]
    fn test_hour_range_wraps_midnight() {
        let snap = test_snapshot();
        let det = test_detector_idle();
        let hour = Utc::now().hour();

        // Test a range that wraps: [22, 6) — true if hour >= 22 or hour < 6
        let c = Condition::HourRange { start: 22, end: 6 };
        let expected = hour >= 22 || hour < 6;
        assert_eq!(c.evaluate(&snap, &det), expected);

        // Test a normal range: [9, 17) — true if 9 <= hour < 17
        let c2 = Condition::HourRange { start: 9, end: 17 };
        let expected2 = hour >= 9 && hour < 17;
        assert_eq!(c2.evaluate(&snap, &det), expected2);
    }

    #[test]
    fn test_duration_exceeds_policy_max() {
        let mut engine = PolicyEngine::new();
        let toml = r#"
[policy.short-training]
purpose = "training"
conditions = [
    { type = "idle_duration", min_secs = 600 },
]
max_duration_s = 3600
"#;
        engine.policies = PolicyEngine::parse_toml(toml).unwrap();

        let snap = test_snapshot();
        let det = test_detector_idle();

        // Request 2 hours — exceeds 1 hour max
        let intent = engine.evaluate_intent(
            "test-client",
            Purpose::Training,
            "Long training".into(),
            Some(7200),
            serde_json::json!({}),
            &snap,
            &det,
        );

        assert!(matches!(intent.status, IntentStatus::Denied(_)));
    }

    #[test]
    fn test_verify_intent_valid() {
        let mut engine = PolicyEngine::new();
        let toml = r#"
[policy.allow-inference]
purpose = "inference"
conditions = []
"#;
        engine.policies = PolicyEngine::parse_toml(toml).unwrap();

        let snap = test_snapshot();
        let det = test_detector_idle();

        let intent = engine.evaluate_intent(
            "client-a",
            Purpose::Inference,
            "Test".into(),
            None,
            serde_json::json!({}),
            &snap,
            &det,
        );
        assert!(matches!(intent.status, IntentStatus::Granted));

        // Verify with correct client
        let policy = engine.verify_intent_for_allocation(&intent.id, "client-a").unwrap();
        assert_eq!(policy, "allow-inference");
    }

    #[test]
    fn test_verify_intent_wrong_client() {
        let mut engine = PolicyEngine::new();
        let toml = r#"
[policy.allow-inference]
purpose = "inference"
conditions = []
"#;
        engine.policies = PolicyEngine::parse_toml(toml).unwrap();

        let snap = test_snapshot();
        let det = test_detector_idle();

        let intent = engine.evaluate_intent(
            "client-a",
            Purpose::Inference,
            "Test".into(),
            None,
            serde_json::json!({}),
            &snap,
            &det,
        );

        // Wrong client should fail
        let err = engine.verify_intent_for_allocation(&intent.id, "client-b").unwrap_err();
        assert!(err.to_string().contains("belongs to client"));
    }

    #[test]
    fn test_verify_intent_consumed() {
        let mut engine = PolicyEngine::new();
        let toml = r#"
[policy.allow-inference]
purpose = "inference"
conditions = []
"#;
        engine.policies = PolicyEngine::parse_toml(toml).unwrap();

        let snap = test_snapshot();
        let det = test_detector_idle();

        let intent = engine.evaluate_intent(
            "client-a",
            Purpose::Inference,
            "Test".into(),
            None,
            serde_json::json!({}),
            &snap,
            &det,
        );

        // Consume it first
        engine.consume_intent(&intent.id).unwrap();

        // Now verification should fail
        let err = engine.verify_intent_for_allocation(&intent.id, "client-a").unwrap_err();
        assert!(err.to_string().contains("Consumed"));
    }

    #[test]
    fn test_verify_intent_not_found() {
        let engine = PolicyEngine::new();
        let err = engine.verify_intent_for_allocation("int-nonexistent", "client-a").unwrap_err();
        assert!(matches!(err, PolicyError::IntentNotFound(_)));
    }

    #[test]
    fn test_verify_intent_expired() {
        let mut engine = PolicyEngine::new();
        let toml = r#"
[policy.allow-inference]
purpose = "inference"
conditions = []
"#;
        engine.policies = PolicyEngine::parse_toml(toml).unwrap();

        let snap = test_snapshot();
        let det = test_detector_idle();

        let intent = engine.evaluate_intent(
            "client-a",
            Purpose::Inference,
            "Test".into(),
            None,
            serde_json::json!({}),
            &snap,
            &det,
        );

        // Manually backdate the granted_at to simulate expiry
        let expired_time = Utc::now() - chrono::Duration::seconds(400);
        engine.intents.get_mut(&intent.id).unwrap().granted_at = Some(expired_time);

        // Should now be expired (300s limit)
        let err = engine.verify_intent_for_allocation(&intent.id, "client-a").unwrap_err();
        assert!(err.to_string().contains("expired"));
    }
}
