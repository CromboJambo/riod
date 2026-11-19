//! Crabjar-guard integration for riod Phase 2: trust-aware authorization.
//!
//! This module bridges riod's resource allocation policy with crabjar-guard's
//! trust resolution and approval queueing. It provides:
//!
//! 1. **Trust resolution** — PID/client-based trust scoring via `TrustResolver`
//! 2. **Approval routing** — High/Critical risk allocations from untrusted clients
//!    are queued for human approval instead of immediate allocation
//! 3. **PID auto-trust** — Processes spawned by trusted parents inherit trust,
//!    enabling seamless agent workflows without repeated approvals
//!
//! ## Design
//!
//! The `GuardBridge` wraps crabjar-guard's `TrustResolver` and maintains a local
//! pending approval queue. When riod evaluates an intent with High/Critical risk:
//!
//! 1. Resolve the client's effective trust level
//! 2. If trusted enough → auto-approve (no human in the loop)
//! 3. If not trusted → queue for human approval via `guard.approvals_*` API methods
//!
//! This keeps riod's existing policy engine intact while adding a guard-managed
//! approval layer for high-risk operations.

use std::collections::HashMap;

use crabjar_guard::{PolicyChain, RequestedTrust, TrustResolver};
use serde::{Deserialize, Serialize};
use tracing::{debug, info};
use uuid::Uuid;

/// A pending approval request for a high-risk allocation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingApproval {
    /// Unique approval ID (for API references).
    pub id: String,
    /// The intent ID this approval is for.
    pub intent_id: String,
    /// Client requesting the allocation.
    pub client_id: String,
    /// Purpose of the allocation.
    pub purpose: String,
    /// Risk level that triggered the approval requirement.
    pub risk_level: String,
    /// Risk score (0.0-1.0).
    pub risk_score: f32,
    /// Reason for requiring approval.
    pub reason: String,
    /// When this approval was requested.
    pub requested_at: chrono::DateTime<chrono::Utc>,
}

/// Bridge between riod's policy engine and crabjar-guard's trust system.
///
/// Manages trust resolution and pending approvals for high-risk allocations.
pub struct GuardBridge {
    /// Trust resolver for PID/client-based trust scoring.
    trust_resolver: TrustResolver,
    /// Pending approval requests awaiting human decision.
    pending_approvals: HashMap<String, PendingApproval>,
}

impl Default for GuardBridge {
    fn default() -> Self {
        Self::new()
    }
}

impl GuardBridge {
    /// Create a new guard bridge with default trust resolution.
    pub fn new() -> Self {
        // Start with an empty policy chain — no caps or requirements by default.
        // Policies can be added later as needed (e.g., cap untrusted clients at layer 1).
        let policy_chain = PolicyChain::new();
        let trust_resolver = TrustResolver::new(policy_chain);

        Self {
            trust_resolver,
            pending_approvals: HashMap::new(),
        }
    }

    /// Resolve the effective trust level for a client.
    ///
    /// Uses PID-based trust resolution if available, otherwise falls back to
    /// the requested trust level with default policy chain resolution.
    pub fn resolve_trust(&self, client_id: &str) -> crabjar_guard::TrustResolution {
        // For now, we resolve based on the client ID as a "source".
        // In a full implementation, this would use PID/PPID chains for auto-trust.
        let requested = RequestedTrust::working(client_id);
        self.trust_resolver.resolve(&requested)
    }

    /// Check if a client is trusted enough to auto-approve high-risk allocations.
    ///
    /// Returns `true` if the client's effective trust allows auto-execution.
    pub fn is_trusted_for_auto_approve(&self, client_id: &str) -> bool {
        let resolution = self.resolve_trust(client_id);
        debug!(
            client_id = %client_id,
            layer = resolution.effective.layer,
            confidence = resolution.effective.confidence,
            "trust resolution for auto-approve check"
        );
        resolution.effective.allows_auto_execute()
    }

    /// Authorize a resource allocation through the guard bridge.
    ///
    /// Returns:
    /// - `Ok(true)` — approved (auto or pre-approved)
    /// - `Ok(false)` — queued for human approval (check `list_pending`)
    /// - `Err(reason)` — denied outright
    pub fn authorize_allocation(
        &mut self,
        intent: &crate::policy::Intent,
        risk_level: &crate::risk::RiskLevel,
        client_pid: Option<u32>,
    ) -> Result<bool, String> {
        let risk_score = intent.risk.as_ref().map(|r| r.total as f32).unwrap_or(0.0);

        // Trusted clients auto-approve even high-risk allocations.
        if self.is_trusted_for_auto_approve(&intent.client_id) {
            info!(
                intent_id = %intent.id,
                client_id = %intent.client_id,
                risk_level = %risk_level,
                "high-risk allocation auto-approved for trusted client"
            );
            return Ok(true);
        }

        // Untrusted clients with High/Critical risk need human approval.
        let approval_id = Uuid::new_v4().to_string();
        let reason = format!(
            "{} risk allocation (score: {:.0}) from untrusted client '{}' requires approval",
            risk_level, risk_score, intent.client_id
        );

        let pending = PendingApproval {
            id: approval_id.clone(),
            intent_id: intent.id.clone(),
            client_id: intent.client_id.clone(),
            purpose: format!("{}", intent.purpose),
            risk_level: risk_level.to_string(),
            risk_score,
            reason: reason.clone(),
            requested_at: chrono::Utc::now(),
        };

        self.pending_approvals.insert(approval_id.clone(), pending);

        info!(
            approval_id = %approval_id,
            intent_id = %intent.id,
            client_id = %intent.client_id,
            risk_level = %risk_level,
            "high-risk allocation queued for human approval"
        );

        let _ = client_pid; // Reserved for future PID-based auto-trust
        Ok(false)
    }

    /// List all pending approvals.
    pub fn list_pending(&self) -> Vec<PendingApproval> {
        self.pending_approvals.values().cloned().collect()
    }

    /// Approve a pending allocation by ID.
    ///
    /// Returns the intent ID that was approved, or an error if not found.
    pub fn approve(&mut self, approval_id: &str) -> Result<String, String> {
        let approval = self
            .pending_approvals
            .get(approval_id)
            .ok_or_else(|| format!("approval '{}' not found", approval_id))?;

        let intent_id = approval.intent_id.clone();
        self.pending_approvals.remove(approval_id);

        info!(
            approval_id = %approval_id,
            intent_id = %intent_id,
            "allocation approved by human"
        );

        Ok(intent_id)
    }

    /// Reject a pending allocation by ID.
    ///
    /// Returns the intent ID that was rejected, or an error if not found.
    pub fn reject(&mut self, approval_id: &str) -> Result<String, String> {
        let approval = match self.pending_approvals.get(approval_id) {
            Some(a) => a.clone(),
            None => return Err(format!("approval '{}' not found", approval_id)),
        };

        let intent_id = approval.intent_id.clone();
        self.pending_approvals.remove(approval_id);

        info!(
            approval_id = %approval_id,
            intent_id = %intent_id,
            "allocation rejected by human"
        );

        Ok(intent_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::policy::{Intent, IntentStatus, Purpose};
    use crate::risk::{RiskLevel, RiskScore};

    fn make_intent(client_id: &str) -> Intent {
        Intent {
            id: Uuid::new_v4().to_string(),
            client_id: client_id.to_string(),
            purpose: Purpose::Training,
            description: "test intent".to_string(),
            estimated_duration_s: Some(300),
            metadata: serde_json::json!({}),
            status: IntentStatus::Granted,
            declared_at: chrono::Utc::now(),
            policy_applied: None,
            granted_at: Some(chrono::Utc::now()),
            failed_conditions: None,
            risk: Some(RiskScore {
                total: 80,
                level: RiskLevel::High,
                factors: vec![],
            }),
        }
    }

    #[test]
    fn test_new_bridge() {
        let bridge = GuardBridge::new();
        assert!(bridge.list_pending().is_empty());
    }

    #[test]
    fn test_untrusted_client_needs_approval() {
        let mut bridge = GuardBridge::new();
        let intent = make_intent("untrusted-client");

        // Untrusted client with high risk should be queued for approval
        let result = bridge.authorize_allocation(&intent, &RiskLevel::High, None);
        assert!(result.is_ok());
        assert!(!result.unwrap()); // Queued, not auto-approved

        let pending = bridge.list_pending();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].client_id, "untrusted-client");
    }

    #[test]
    fn test_approve_pending() {
        let mut bridge = GuardBridge::new();
        let intent = make_intent("client-1");

        let result = bridge.authorize_allocation(&intent, &RiskLevel::High, None);
        assert!(!result.unwrap());

        let pending = bridge.list_pending();
        let approval_id = pending[0].id.clone();
        let intent_id = pending[0].intent_id.clone();

        let approved_intent = bridge.approve(&approval_id).unwrap();
        assert_eq!(approved_intent, intent_id);
        assert!(bridge.list_pending().is_empty());
    }

    #[test]
    fn test_reject_pending() {
        let mut bridge = GuardBridge::new();
        let intent = make_intent("client-1");

        let result = bridge.authorize_allocation(&intent, &RiskLevel::High, None);
        assert!(!result.unwrap());

        let pending = bridge.list_pending();
        let approval_id = pending[0].id.clone();
        let intent_id = pending[0].intent_id.clone();

        let rejected_intent = bridge.reject(&approval_id).unwrap();
        assert_eq!(rejected_intent, intent_id);
        assert!(bridge.list_pending().is_empty());
    }

    #[test]
    fn test_approve_unknown() {
        let mut bridge = GuardBridge::new();
        let result = bridge.approve("nonexistent");
        assert!(result.is_err());
    }
}
