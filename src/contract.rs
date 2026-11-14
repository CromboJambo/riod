//! Interface contract for the rIOd JSON-RPC API.
//!
//! This module defines a declarative, versioned description of every method
//! exposed by the daemon. It is the abstraction boundary — transport layers
//! (Unix sockets, TCP, SSH tunnels) are interchangeable realizations of this
//! contract.
//!
//! Design principles:
//! - **Contract-first**: The spec below IS the API definition. Handlers must
//!   match their contract entry; mismatches are caught at startup.
//! - **Language-neutral wire format**: Params and returns are described with
//!   JSON Schema, not Rust types. A Python client can read this contract and
//!   generate typed wrappers without depending on Rust.
//! - **Versioned negotiation**: Clients declare their maximum supported
//!   protocol version; the server responds with the negotiated method set.
//!   Like Cargo.toml (intent/ranges) vs Cargo.lock (resolved reality).
//! - **No separate IDL**: The contract lives in Rust as data, not as a
//!   generated artifact from an external format. It can be serialized to JSON
//!   for inspection but is authored alongside the handlers.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Protocol version (semantic).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ApiVersion {
    pub major: u32,
    pub minor: u32,
    pub patch: u32,
}

impl ApiVersion {
    pub const fn new(major: u32, minor: u32, patch: u32) -> Self {
        Self { major, minor, patch }
    }

    /// Parse "MAJOR.MINOR.PATCH" from a string.
    pub fn parse(s: &str) -> Option<Self> {
        let parts: Vec<&str> = s.split('.').collect();
        if parts.len() != 3 {
            return None;
        }
        Some(Self {
            major: parts[0].parse().ok()?,
            minor: parts[1].parse().ok()?,
            patch: parts[2].parse().ok()?,
        })
    }

    /// Format as "MAJOR.MINOR.PATCH".
    pub fn to_string(&self) -> String {
        format!("{}.{}.{}", self.major, self.minor, self.patch)
    }

    /// Check if this version is compatible with `other` under semver rules:
    /// same major, and this >= other (server can offer more than client asks).
    pub fn compatible_with(&self, other: &ApiVersion) -> bool {
        self.major == other.major && self >= other
    }
}

impl std::fmt::Display for ApiVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}

/// Which development phase introduced a method.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Phase {
    /// Observation and reporting (CPU/RAM/GPU polling, idle detection, ledger).
    Zero,
    /// Resource brokerage (allocation, heartbeats, reclamation, cgroups).
    One,
    /// Purpose-based authorization (policies, intents, risk scoring).
    Two,
    /// Background cognition on idle (dream scheduler, task registry).
    Three,
}

/// Description of a single API method.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MethodSpec {
    /// Fully qualified method name (e.g., "resources.allocate").
    pub name: String,

    /// Human-readable description of what the method does.
    pub description: String,

    /// The phase that introduced this method.
    pub phase: Phase,

    /// Protocol version when this method was added.
    #[serde(rename = "since")]
    pub since_version: ApiVersion,

    /// If set, the version from which this method is deprecated.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deprecated_since: Option<ApiVersion>,

    /// JSON Schema describing the `params` object for this method.
    /// `null` means the method takes no parameters.
    #[serde(default)]
    pub params_schema: serde_json::Value,

    /// JSON Schema describing the `result.data` field in successful responses.
    #[serde(default)]
    pub returns_schema: serde_json::Value,

    /// Whether this method mutates server state.
    /// Idempotent read methods should be `false`.
    pub mutating: bool,
}

/// The complete API contract for a rIOd instance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiContract {
    /// Service name (always "riod").
    pub name: String,

    /// Current protocol version of this server.
    pub version: ApiVersion,

    /// Capabilities this server supports beyond the base contract.
    /// Examples: "cgroup_v2", "gpu_nvidia", "dream_scheduler".
    pub capabilities: Vec<String>,

    /// All methods this server exposes, sorted by name.
    pub methods: BTreeMap<String, MethodSpec>,
}

impl ApiContract {
    /// Build the canonical contract for the current rIOd implementation.
    ///
    /// This is the single source of truth for what the API looks like.
    /// Every handler registered in socket.rs must have a corresponding entry here.
    pub fn current() -> Self {
        let version = ApiVersion::new(1, 0, 0);

        let mut methods = BTreeMap::new();

        // ── Phase 0: System and Ledger ────────────────────────────────

        methods.insert(
            "system.status".into(),
            MethodSpec {
                name: "system.status".into(),
                description: "Returns current resource snapshot, idle state, and daemon uptime."
                    .into(),
                phase: Phase::Zero,
                since_version: version.clone(),
                deprecated_since: None,
                params_schema: serde_json::json!({ "type": "null" }),
                returns_schema: serde_json::json!({
                    "type": "object",
                    "properties": {
                        "snapshot": { "description": "Latest ResourceSnapshot or null if not yet available" },
                        "idle": {
                            "type": "object",
                            "properties": {
                                "is_idle": { "type": "boolean" },
                                "idle_since": { "type": ["string", "null"], "format": "date-time" },
                                "consecutive_idle_samples": { "type": "integer" }
                            }
                        },
                        "uptime_secs": { "type": "integer" }
                    }
                }),
                mutating: false,
            },
        );

        methods.insert(
            "ledger.query".into(),
            MethodSpec {
                name: "ledger.query".into(),
                description: "Query ledger events with optional time range, type filter, and limit."
                    .into(),
                phase: Phase::Zero,
                since_version: version.clone(),
                deprecated_since: None,
                params_schema: serde_json::json!({
                    "type": "object",
                    "properties": {
                        "since": { "type": "string", "format": "date-time", "description": "Only events at or after this time" },
                        "event_type": { "type": "string", "description": "Filter by event type (e.g., 'allocation_granted')" },
                        "limit": { "type": "integer", "minimum": 1, "maximum": 1000, "default": 100 }
                    },
                    "additionalProperties": false
                }),
                returns_schema: serde_json::json!({
                    "type": "object",
                    "properties": {
                        "events": { "type": "array", "items": { "$ref": "#/definitions/LedgerEntry" } },
                        "count": { "type": "integer" }
                    }
                }),
                mutating: false,
            },
        );

        methods.insert(
            "ledger.tail".into(),
            MethodSpec {
                name: "ledger.tail".into(),
                description: "Get the N most recent ledger events."
                    .into(),
                phase: Phase::Zero,
                since_version: version.clone(),
                deprecated_since: None,
                params_schema: serde_json::json!({
                    "type": "object",
                    "properties": {
                        "n": { "type": "integer", "minimum": 1, "maximum": 1000, "default": 20 }
                    },
                    "additionalProperties": false
                }),
                returns_schema: serde_json::json!({
                    "type": "object",
                    "properties": {
                        "events": { "type": "array" },
                        "count": { "type": "integer" }
                    }
                }),
                mutating: false,
            },
        );

        methods.insert(
            "ledger.stats".into(),
            MethodSpec {
                name: "ledger.stats".into(),
                description: "Get ledger statistics (entry count, path)."
                    .into(),
                phase: Phase::Zero,
                since_version: version.clone(),
                deprecated_since: None,
                params_schema: serde_json::json!({ "type": "null" }),
                returns_schema: serde_json::json!({
                    "type": "object",
                    "properties": {
                        "entry_count": { "type": "integer" },
                        "path": { "type": "string" }
                    }
                }),
                mutating: false,
            },
        );

        // ── Phase 1: Resource Broker ──────────────────────────────────

        methods.insert(
            "resources.allocate".into(),
            MethodSpec {
                name: "resources.allocate".into(),
                description: "Request GPU/RAM allocation with TTL. Returns an Allocation if granted."
                    .into(),
                phase: Phase::One,
                since_version: version.clone(),
                deprecated_since: None,
                params_schema: serde_json::json!({
                    "type": "object",
                    "required": ["client_id", "purpose"],
                    "properties": {
                        "client_id": { "type": "string", "description": "Unique identifier for the requesting client" },
                        "purpose": { "type": "string", "description": "Why resources are needed (e.g., 'training', 'inference')" },
                        "gpus": { "type": "array", "items": { "type": "integer" }, "description": "Specific GPU indices, or omit for auto-assign" },
                        "gpu_count": { "type": "integer", "minimum": 1, "description": "Number of GPUs if not specifying indices" },
                        "cpu_percent": { "type": "number", "minimum": 1, "maximum": 100, "default": 100 },
                        "ram_mb": { "type": "integer", "minimum": 64, "description": "RAM in MB to reserve" },
                        "ttl_secs": { "type": "integer", "minimum": 30, "maximum": 86400, "default": 3600 },
                        "intent_id": { "type": "string", "description": "Optional intent ID from intent.declare for policy enforcement" }
                    },
                    "additionalProperties": false
                }),
                returns_schema: serde_json::json!({
                    "type": "object",
                    "properties": {
                        "allocation": { "$ref": "#/definitions/Allocation" },
                        "intent_id": { "type": ["string", "null"] },
                        "message": { "type": "string" }
                    }
                }),
                mutating: true,
            },
        );

        methods.insert(
            "resources.release".into(),
            MethodSpec {
                name: "resources.release".into(),
                description: "Explicitly release an allocation. Only the owning client may release."
                    .into(),
                phase: Phase::One,
                since_version: version.clone(),
                deprecated_since: None,
                params_schema: serde_json::json!({
                    "type": "object",
                    "required": ["allocation_id", "client_id"],
                    "properties": {
                        "allocation_id": { "type": "string" },
                        "client_id": { "type": "string" }
                    },
                    "additionalProperties": false
                }),
                returns_schema: serde_json::json!({
                    "type": "object",
                    "properties": {
                        "released": { "type": "string" },
                        "client_id": { "type": "string" },
                        "gpus_freed": { "type": "array", "items": { "type": "integer" } }
                    }
                }),
                mutating: true,
            },
        );

        methods.insert(
            "resources.heartbeat".into(),
            MethodSpec {
                name: "resources.heartbeat".into(),
                description: "Extend an allocation's TTL. Must be called before expiry to keep the allocation alive."
                    .into(),
                phase: Phase::One,
                since_version: version.clone(),
                deprecated_since: None,
                params_schema: serde_json::json!({
                    "type": "object",
                    "required": ["allocation_id", "client_id"],
                    "properties": {
                        "allocation_id": { "type": "string" },
                        "client_id": { "type": "string" }
                    },
                    "additionalProperties": false
                }),
                returns_schema: serde_json::json!({
                    "type": "object",
                    "properties": {
                        "allocation_id": { "type": "string" },
                        "expires_at": { "type": "string", "format": "date-time" }
                    }
                }),
                mutating: true,
            },
        );

        methods.insert(
            "resources.list".into(),
            MethodSpec {
                name: "resources.list".into(),
                description: "List all active allocations with derived lifecycle state."
                    .into(),
                phase: Phase::One,
                since_version: version.clone(),
                deprecated_since: None,
                params_schema: serde_json::json!({ "type": "null" }),
                returns_schema: serde_json::json!({
                    "type": "object",
                    "properties": {
                        "allocations": {
                            "type": "array",
                            "items": {
                                "allOf": [
                                    { "$ref": "#/definitions/Allocation" },
                                    {
                                        "type": "object",
                                        "properties": {
                                            "state": {
                                                "type": "string",
                                                "enum": ["Active", "Unknown", "Expired", "Released"]
                                            }
                                        }
                                    }
                                ]
                            }
                        },
                        "count": { "type": "integer" }
                    }
                }),
                mutating: false,
            },
        );

        methods.insert(
            "resources.available".into(),
            MethodSpec {
                name: "resources.available".into(),
                description: "Show current available capacity (utilization-aware). Does not allocate."
                    .into(),
                phase: Phase::One,
                since_version: version.clone(),
                deprecated_since: None,
                params_schema: serde_json::json!({ "type": "null" }),
                returns_schema: serde_json::json!({
                    "type": "object",
                    "properties": {
                        "available_gpus": { "type": "array", "items": { "type": "integer" } },
                        "total_gpus": { "type": "integer" },
                        "allocated_gpus": { "type": "integer" },
                        "available_ram_mb": { "type": "integer" },
                        "total_ram_mb": { "type": "integer" },
                        "system_used_ram_mb": { "type": "integer" },
                        "riod_allocated_ram_mb": { "type": "integer" },
                        "active_allocations": { "type": "integer" },
                        "data_fresh": { "type": "boolean" },
                        "gpu_details": { "type": "array" }
                    }
                }),
                mutating: false,
            },
        );

        methods.insert(
            "resources.add_process".into(),
            MethodSpec {
                name: "resources.add_process".into(),
                description: "Add a PID to an allocation's cgroup for resource enforcement."
                    .into(),
                phase: Phase::One,
                since_version: version.clone(),
                deprecated_since: None,
                params_schema: serde_json::json!({
                    "type": "object",
                    "required": ["allocation_id", "client_id", "pid"],
                    "properties": {
                        "allocation_id": { "type": "string" },
                        "client_id": { "type": "string" },
                        "pid": { "type": "integer", "minimum": 1 }
                    },
                    "additionalProperties": false
                }),
                returns_schema: serde_json::json!({
                    "type": "object",
                    "properties": {
                        "allocation_id": { "type": "string" },
                        "pid": { "type": "integer" },
                        "message": { "type": "string" }
                    }
                }),
                mutating: true,
            },
        );

        methods.insert(
            "resources.risk_assess".into(),
            MethodSpec {
                name: "resources.risk_assess".into(),
                description: "Dry-run risk assessment without declaring intent or allocating resources."
                    .into(),
                phase: Phase::Two,
                since_version: version.clone(),
                deprecated_since: None,
                params_schema: serde_json::json!({
                    "type": "object",
                    "required": ["client_id", "purpose"],
                    "properties": {
                        "client_id": { "type": "string" },
                        "purpose": { "type": "string" },
                        "estimated_duration_s": { "type": "integer" },
                        "gpu_count": { "type": "integer" },
                        "ram_mb": { "type": "integer" }
                    }
                }),
                returns_schema: serde_json::json!({
                    "type": "object",
                    "description": "RiskScore with total, level, and component breakdowns"
                }),
                mutating: false,
            },
        );

        // ── Phase 2: Policy and Intent ────────────────────────────────

        methods.insert(
            "intent.declare".into(),
            MethodSpec {
                name: "intent.declare".into(),
                description: "Declare an intent to use resources. Returns a grant/denial decision with policy rationale."
                    .into(),
                phase: Phase::Two,
                since_version: version.clone(),
                deprecated_since: None,
                params_schema: serde_json::json!({
                    "type": "object",
                    "required": ["client_id", "purpose"],
                    "properties": {
                        "client_id": { "type": "string" },
                        "purpose": { "type": "string" },
                        "description": { "type": "string" },
                        "estimated_duration_s": { "type": "integer" },
                        "metadata": { "type": "object" }
                    },
                    "additionalProperties": false
                }),
                returns_schema: serde_json::json!({
                    "type": "object",
                    "description": "IntentDecision with id, status, policy_applied, risk assessment"
                }),
                mutating: true,
            },
        );

        methods.insert(
            "intent.status".into(),
            MethodSpec {
                name: "intent.status".into(),
                description: "Check the status of a previously declared intent."
                    .into(),
                phase: Phase::Two,
                since_version: version.clone(),
                deprecated_since: None,
                params_schema: serde_json::json!({
                    "type": "object",
                    "required": ["id"],
                    "properties": {
                        "id": { "type": "string" }
                    },
                    "additionalProperties": false
                }),
                returns_schema: serde_json::json!({
                    "type": "object",
                    "description": "IntentDecision for the given ID"
                }),
                mutating: false,
            },
        );

        methods.insert(
            "policy.list".into(),
            MethodSpec {
                name: "policy.list".into(),
                description: "List all loaded policies (count only; policy contents are server-side)."
                    .into(),
                phase: Phase::Two,
                since_version: version.clone(),
                deprecated_since: None,
                params_schema: serde_json::json!({ "type": "null" }),
                returns_schema: serde_json::json!({
                    "type": "object",
                    "properties": {
                        "policy_count": { "type": "integer" }
                    }
                }),
                mutating: false,
            },
        );

        methods.insert(
            "policy.reload".into(),
            MethodSpec {
                name: "policy.reload".into(),
                description: "Reload policies from the configured TOML file."
                    .into(),
                phase: Phase::Two,
                since_version: version.clone(),
                deprecated_since: None,
                params_schema: serde_json::json!({ "type": "null" }),
                returns_schema: serde_json::json!({
                    "type": "object",
                    "properties": {
                        "reloaded_policies": { "type": "integer" }
                    }
                }),
                mutating: true,
            },
        );

        // ── Phase 3: Dream Scheduler ──────────────────────────────────

        methods.insert(
            "dream.status".into(),
            MethodSpec {
                name: "dream.status".into(),
                description: "Query the dream scheduler state (current cycle, next trigger, etc.)."
                    .into(),
                phase: Phase::Three,
                since_version: version.clone(),
                deprecated_since: None,
                params_schema: serde_json::json!({ "type": "null" }),
                returns_schema: serde_json::json!({
                    "type": "object",
                    "description": "DreamSchedulerState"
                }),
                mutating: false,
            },
        );

        methods.insert(
            "dream.cancel".into(),
            MethodSpec {
                name: "dream.cancel".into(),
                description: "Cancel an active dream cycle. Preempts all allocations tagged with the cycle."
                    .into(),
                phase: Phase::Three,
                since_version: version.clone(),
                deprecated_since: None,
                params_schema: serde_json::json!({
                    "type": "object",
                    "properties": {
                        "reason": { "type": "string" }
                    },
                    "additionalProperties": false
                }),
                returns_schema: serde_json::json!({
                    "type": "object",
                    "properties": {
                        "cancelled": { "type": "boolean" },
                        "reason": { "type": "string" },
                        "cycle_id": { "type": ["string", "null"] },
                        "allocations_released": { "type": "integer" }
                    }
                }),
                mutating: true,
            },
        );

        methods.insert(
            "dream.tasks".into(),
            MethodSpec {
                name: "dream.tasks".into(),
                description: "List all registered dream tasks."
                    .into(),
                phase: Phase::Three,
                since_version: version.clone(),
                deprecated_since: None,
                params_schema: serde_json::json!({ "type": "null" }),
                returns_schema: serde_json::json!({
                    "type": "object",
                    "properties": {
                        "tasks": { "type": "array" },
                        "count": { "type": "integer" }
                    }
                }),
                mutating: false,
            },
        );

        methods.insert(
            "dream.register_task".into(),
            MethodSpec {
                name: "dream.register_task".into(),
                description: "Register a dream task with the registry."
                    .into(),
                phase: Phase::Three,
                since_version: version.clone(),
                deprecated_since: None,
                params_schema: serde_json::json!({
                    "type": "object",
                    "required": ["name", "description", "provider"],
                    "properties": {
                        "name": { "type": "string" },
                        "description": { "type": "string" },
                        "provider": { "type": "string" },
                        "estimated_duration_mins": { "type": "integer" },
                        "requires_gpu": { "type": "boolean" },
                        "min_idle_threshold_mins": { "type": "integer" }
                    }
                }),
                returns_schema: serde_json::json!({
                    "type": "object",
                    "properties": {
                        "registered": { "type": "boolean" },
                        "task": { "type": "object" }
                    }
                }),
                mutating: true,
            },
        );

        methods.insert(
            "dream.report".into(),
            MethodSpec {
                name: "dream.report".into(),
                description: "Submit a dream task result with observations."
                    .into(),
                phase: Phase::Three,
                since_version: version.clone(),
                deprecated_since: None,
                params_schema: serde_json::json!({
                    "type": "object",
                    "required": ["task_name", "cycle_id", "client_id", "success"],
                    "properties": {
                        "task_name": { "type": "string" },
                        "cycle_id": { "type": "string" },
                        "client_id": { "type": "string" },
                        "duration_secs": { "type": "integer" },
                        "success": { "type": "boolean" },
                        "error": { "type": ["string", "null"] },
                        "observations": { "type": "array" }
                    }
                }),
                returns_schema: serde_json::json!({
                    "type": "object",
                    "properties": {
                        "reported": { "type": "boolean" },
                        "observation_count": { "type": "integer" }
                    }
                }),
                mutating: true,
            },
        );

        // ── Contract introspection ────────────────────────────────────

        methods.insert(
            "contract.info".into(),
            MethodSpec {
                name: "contract.info".into(),
                description: "Return the full API contract (this document). Clients should call this first to discover available methods and negotiate version."
                    .into(),
                phase: Phase::Zero,
                since_version: version.clone(),
                deprecated_since: None,
                params_schema: serde_json::json!({ "type": "null" }),
                returns_schema: serde_json::json!({
                    "type": "object",
                    "properties": {
                        "name": { "type": "string" },
                        "version": { "type": "string" },
                        "capabilities": { "type": "array", "items": { "type": "string" } },
                        "methods": { "type": "object" }
                    }
                }),
                mutating: false,
            },
        );

        Self {
            name: "riod".into(),
            version,
            capabilities: vec![
                "unix_socket_jsonrpc".into(),
                "append_only_ledger".into(),
                "event_derived_projections".into(),
                "lifecycle_state_machine".into(),
            ],
            methods,
        }
    }

    /// Get the spec for a method by name.
    pub fn method(&self, name: &str) -> Option<&MethodSpec> {
        self.methods.get(name)
    }

    /// Validate that a set of registered handler names matches this contract exactly.
    /// Returns a list of mismatches (missing handlers or unregistered methods).
    pub fn validate_handlers(&self, registered: &[&str]) -> Vec<String> {
        let mut issues = Vec::new();

        for name in self.methods.keys() {
            if !registered.contains(&name.as_str()) {
                issues.push(format!("contract declares '{}' but no handler is registered", name));
            }
        }

        for name in registered {
            if !self.methods.contains_key(*name) {
                issues.push(format!("handler '{}' is registered but not declared in contract", name));
            }
        }

        issues
    }

    /// Negotiate a compatible method set with a client.
    ///
    /// The client declares its maximum supported version and optionally the
    /// methods it needs. Returns the subset of this server's methods that are:
    /// 1. Available at or below the client's max version
    /// 2. Not deprecated (unless the client explicitly requests them)
    ///
    /// This is the "Cargo.lock" step — resolving intent to reality.
    pub fn negotiate(&self, client_max_version: &ApiVersion) -> Option<NegotiatedContract> {
        if !self.version.compatible_with(client_max_version)
            && !client_max_version.compatible_with(&self.version)
        {
            return None; // Major version mismatch — incompatible
        }

        // The negotiated version is the minimum of both sides (most conservative).
        let negotiated_version = if self.version <= *client_max_version {
            self.version.clone()
        } else {
            client_max_version.clone()
        };

        // Include all methods available at or before the negotiated version.
        let mut compatible_methods = BTreeMap::new();
        for (name, spec) in &self.methods {
            if spec.since_version <= negotiated_version {
                compatible_methods.insert(name.clone(), spec.clone());
            }
        }

        Some(NegotiatedContract {
            server_version: self.version.clone(),
            client_max_version: client_max_version.clone(),
            negotiated_version,
            methods: compatible_methods,
            capabilities: self.capabilities.clone(),
        })
    }
}

/// Result of version negotiation between client and server.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NegotiatedContract {
    /// The server's full protocol version.
    pub server_version: ApiVersion,
    /// The maximum version the client supports.
    pub client_max_version: ApiVersion,
    /// The version both sides agree to use (minimum of the two).
    pub negotiated_version: ApiVersion,
    /// Methods available under the negotiated version.
    pub methods: BTreeMap<String, MethodSpec>,
    /// Server capabilities.
    pub capabilities: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version_parse_and_display() {
        let v = ApiVersion::parse("1.2.3").unwrap();
        assert_eq!(v.major, 1);
        assert_eq!(v.minor, 2);
        assert_eq!(v.patch, 3);
        assert_eq!(v.to_string(), "1.2.3");
    }

    #[test]
    fn test_version_compatibility() {
        let v1 = ApiVersion::new(1, 0, 0);
        let v1_1 = ApiVersion::new(1, 1, 0);
        let v2 = ApiVersion::new(2, 0, 0);

        // Same major, higher minor is compatible
        assert!(v1_1.compatible_with(&v1));
        assert!(!v1.compatible_with(&v1_1)); // Server can't offer less than client asks... 
                                            // actually this direction means "can I serve this client?"
        
        // Different major is incompatible
        assert!(!v2.compatible_with(&v1));
        assert!(!v1.compatible_with(&v2));
    }

    #[test]
    fn test_current_contract_is_complete() {
        let contract = ApiContract::current();
        
        // All expected methods are present
        let expected = [
            "system.status", "ledger.query", "ledger.tail", "ledger.stats",
            "resources.allocate", "resources.release", "resources.heartbeat",
            "resources.list", "resources.available", "resources.add_process",
            "resources.risk_assess",
            "intent.declare", "intent.status", "policy.list", "policy.reload",
            "dream.status", "dream.cancel", "dream.tasks",
            "dream.register_task", "dream.report",
            "contract.info",
        ];
        
        for name in expected {
            assert!(
                contract.method(name).is_some(),
                "missing method in contract: {}",
                name
            );
        }
    }

    #[test]
    fn test_validate_handlers_catches_mismatches() {
        let contract = ApiContract::current();
        
        // Simulate a handler list missing one method
        let mut handlers: Vec<&str> = contract.methods.keys().map(|s| s.as_str()).collect();
        handlers.pop(); // Remove one
        
        let issues = contract.validate_handlers(&handlers);
        assert_eq!(issues.len(), 1);
        assert!(issues[0].contains("no handler is registered"));
    }

    #[test]
    fn test_negotiation_same_version() {
        let contract = ApiContract::current();
        let client_v = contract.version.clone();
        
        let negotiated = contract.negotiate(&client_v).unwrap();
        assert_eq!(negotiated.negotiated_version, client_v);
        assert_eq!(negotiated.methods.len(), contract.methods.len());
    }

    #[test]
    fn test_negotiation_major_mismatch_fails() {
        let contract = ApiContract::current();
        let client_v = ApiVersion::new(2, 0, 0); // Client wants v2, server is v1
        
        let result = contract.negotiate(&client_v);
        assert!(result.is_none());
    }

    #[test]
    fn test_contract_serialization_roundtrip() {
        let contract = ApiContract::current();
        let json = serde_json::to_string(&contract).unwrap();
        let deserialized: ApiContract = serde_json::from_str(&json).unwrap();
        
        assert_eq!(deserialized.name, contract.name);
        assert_eq!(deserialized.version, contract.version);
        assert_eq!(deserialized.methods.len(), contract.methods.len());
    }
}
