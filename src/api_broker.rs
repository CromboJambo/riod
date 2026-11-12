//! JSON-RPC handlers for Phase 1 resource broker methods.
//!
//! Extracted from socket.rs to keep both files under the 500 LoC limit.

use crate::broker::AllocationRequest;
use crate::socket::{error_response, success_response};
use crate::state::ServerState;
use serde_json::{json, Value};
use std::sync::Arc;

/// Handle `resources.allocate` — request resources from the broker.
pub async fn handle_resources_allocate(
    id: &Option<Value>,
    params: Option<Value>,
    state: &Arc<ServerState>,
) -> Value {
    let request: AllocationRequest = match params.and_then(|p| serde_json::from_value(p).ok()) {
        Some(req) => req,
        None => {
            return error_response(
                id,
                -32602,
                "Invalid params",
                Some(json!({
                    "expected": "AllocationRequest object with client_id, purpose, and optionally gpus/gpu_count/cpu_percent/ram_mb/ttl_secs"
                })),
            );
        }
    };

    let mut broker = state.broker.lock().unwrap();
    let mut ledger = state.ledger.lock().unwrap();

    match broker.allocate(&request, &mut ledger) {
        Ok(alloc) => success_response(
            id,
            json!({
                "success": true,
                "data": {
                    "allocation": alloc,
                    "message": format!(
                        "Allocated {} GPU(s) to {} for '{}'",
                        alloc.gpus.len(),
                        alloc.client_id,
                        alloc.purpose
                    ),
                },
            }),
        ),
        Err(e) => error_response(
            id,
            -32000,
            "Allocation failed",
            Some(json!({ "error": e.to_string() })),
        ),
    }
}

/// Handle `resources.release` — release an allocation.
pub async fn handle_resources_release(
    id: &Option<Value>,
    params: Option<Value>,
    state: &Arc<ServerState>,
) -> Value {
    let params = match params {
        Some(p) => p,
        None => {
            return error_response(id, -32602, "Missing params", Some(json!({})));
        }
    };

    let allocation_id: String = match params.get("allocation_id").and_then(|v| v.as_str()) {
        Some(s) => s.to_string(),
        None => {
            return error_response(id, -32602, "Missing allocation_id", Some(json!({})));
        }
    };
    let client_id: String = match params.get("client_id").and_then(|v| v.as_str()) {
        Some(s) => s.to_string(),
        None => {
            return error_response(id, -32602, "Missing client_id", Some(json!({})));
        }
    };

    let mut broker = state.broker.lock().unwrap();
    let mut ledger = state.ledger.lock().unwrap();

    match broker.release(&allocation_id, &client_id, &mut ledger) {
        Ok(alloc) => success_response(
            id,
            json!({
                "success": true,
                "data": {
                    "released": alloc.id,
                    "client_id": alloc.client_id,
                    "gpus_freed": alloc.gpus,
                },
            }),
        ),
        Err(e) => error_response(
            id,
            -32000,
            "Release failed",
            Some(json!({ "error": e.to_string() })),
        ),
    }
}

/// Handle `resources.heartbeat` — extend an allocation's TTL.
pub async fn handle_resources_heartbeat(
    id: &Option<Value>,
    params: Option<Value>,
    state: &Arc<ServerState>,
) -> Value {
    let params = match params {
        Some(p) => p,
        None => {
            return error_response(id, -32602, "Missing params", Some(json!({})));
        }
    };

    let allocation_id: String = match params.get("allocation_id").and_then(|v| v.as_str()) {
        Some(s) => s.to_string(),
        None => {
            return error_response(id, -32602, "Missing allocation_id", Some(json!({})));
        }
    };
    let client_id: String = match params.get("client_id").and_then(|v| v.as_str()) {
        Some(s) => s.to_string(),
        None => {
            return error_response(id, -32602, "Missing client_id", Some(json!({})));
        }
    };

    let mut broker = state.broker.lock().unwrap();
    let mut ledger = state.ledger.lock().unwrap();

    match broker.heartbeat(&allocation_id, &client_id, &mut ledger) {
        Ok(alloc) => success_response(
            id,
            json!({
                "success": true,
                "data": {
                    "allocation_id": alloc.id,
                    "expires_at": alloc.expires_at.to_rfc3339(),
                },
            }),
        ),
        Err(e) => error_response(
            id,
            -32000,
            "Heartbeat failed",
            Some(json!({ "error": e.to_string() })),
        ),
    }
}

/// Handle `resources.list` — list all active allocations.
pub async fn handle_resources_list(id: &Option<Value>, state: &Arc<ServerState>) -> Value {
    let broker = state.broker.lock().unwrap();
    let allocations: Vec<&crate::broker::Allocation> = broker.list_allocations();

    success_response(
        id,
        json!({
            "success": true,
            "data": {
                "allocations": allocations,
                "count": allocations.len(),
            },
        }),
    )
}

/// Handle `resources.available` — show current available capacity.
pub async fn handle_resources_available(id: &Option<Value>, state: &Arc<ServerState>) -> Value {
    let broker = state.broker.lock().unwrap();
    let status = broker.status();

    success_response(
        id,
        json!({
            "success": true,
            "data": {
                "available_gpus": broker.available_gpus(),
                "total_gpus": status.total_gpus.len(),
                "allocated_gpus": status.allocated_gpus.len(),
                "total_ram_mb": status.total_ram_mb,
                "allocated_ram_mb": status.allocated_ram_mb,
                "available_ram_mb": status.total_ram_mb.saturating_sub(status.allocated_ram_mb),
                "active_allocations": status.active_allocations,
            },
        }),
    )
}

/// Handle `resources.add_process` — add a PID to an allocation's cgroup.
pub async fn handle_resources_add_process(
    id: &Option<Value>,
    params: Option<Value>,
    state: &Arc<ServerState>,
) -> Value {
    let params = match params {
        Some(p) => p,
        None => {
            return error_response(id, -32602, "Missing params", Some(json!({})));
        }
    };

    let allocation_id: String = match params.get("allocation_id").and_then(|v| v.as_str()) {
        Some(s) => s.to_string(),
        None => {
            return error_response(id, -32602, "Missing allocation_id", Some(json!({})));
        }
    };
    let client_id: String = match params.get("client_id").and_then(|v| v.as_str()) {
        Some(s) => s.to_string(),
        None => {
            return error_response(id, -32602, "Missing client_id", Some(json!({})));
        }
    };
    let pid: u32 = match params.get("pid").and_then(|v| v.as_u64()) {
        Some(p) => p as u32,
        None => {
            return error_response(id, -32602, "Missing or invalid pid", Some(json!({})));
        }
    };

    let broker = state.broker.lock().unwrap();

    match broker.add_process(&allocation_id, &client_id, pid) {
        Ok(()) => success_response(
            id,
            json!({
                "success": true,
                "data": {
                    "allocation_id": allocation_id,
                    "pid": pid,
                    "message": format!("Process {} added to cgroup for {}", pid, allocation_id),
                },
            }),
        ),
        Err(e) => error_response(
            id,
            -32000,
            "Failed to add process",
            Some(json!({ "error": e.to_string() })),
        ),
    }
}
