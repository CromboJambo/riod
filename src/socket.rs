//! Unix socket JSON-RPC 2.0 server for rIOd client API.

use crate::state::ServerState;
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::{json, Value};
use std::path::PathBuf;
use std::sync::Arc;
use thiserror::Error;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};

/// Error types for the socket server.
#[derive(Debug, Error)]
pub enum SocketError {
    #[error("failed to bind socket at {0}: {1}")]
    BindFailed(String, String),

    #[error("failed to accept connection: {0}")]
    AcceptFailed(String),

    #[error("IO error: {0}")]
    Io(String),

    #[error("JSON parse error: {0}")]
    JsonParse(String),
}

/// JSON-RPC 2.0 request.
#[derive(Debug, Deserialize)]
struct JsonRpcRequest {
    #[allow(dead_code)] // Protocol field, validated by presence not value in Phase 0
    jsonrpc: Option<String>,
    method: String,
    params: Option<Value>,
    id: Option<Value>,
}

/// JSON-RPC 2.0 response builder.
pub fn success_response(id: &Option<Value>, result: Value) -> Value {
    json!({
        "jsonrpc": "2.0",
        "result": result,
        "id": id,
    })
}

pub fn error_response(id: &Option<Value>, code: i32, message: &str, data: Option<Value>) -> Value {
    let mut err = json!({
        "code": code,
        "message": message,
    });
    if let Some(d) = data {
        err["data"] = d;
    }
    json!({
        "jsonrpc": "2.0",
        "error": err,
        "id": id,
    })
}

/// Return the list of all method names this server handles.
/// Used at startup to validate against the API contract.
pub fn supported_methods() -> Vec<&'static str> {
    vec![
        "system.status",
        "contract.info",
        "ledger.query",
        "ledger.tail",
        "ledger.stats",
        "resources.allocate",
        "resources.release",
        "resources.heartbeat",
        "resources.list",
        "resources.available",
        "resources.add_process",
        "resources.risk_assess",
        "intent.declare",
        "intent.status",
        "policy.list",
        "policy.reload",
        "dream.status",
        "dream.cancel",
        "dream.tasks",
        "dream.register_task",
        "dream.report",
    ]
}

/// Start the Unix socket server. Blocks until the task is cancelled.
pub async fn serve(
    socket_path: PathBuf,
    socket_perm: u32,
    state: Arc<ServerState>,
) -> Result<(), SocketError> {
    // Remove stale socket file if it exists
    let _ = std::fs::remove_file(&socket_path);

    // Ensure parent directory exists
    if let Some(parent) = socket_path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| {
            SocketError::BindFailed(socket_path.display().to_string(), e.to_string())
        })?;
    }

    let listener = UnixListener::bind(&socket_path)
        .map_err(|e| SocketError::BindFailed(socket_path.display().to_string(), e.to_string()))?;

    // Apply requested permissions to the socket file
    use std::os::unix::fs::PermissionsExt;
    let perms = std::fs::Permissions::from_mode(socket_perm);
    if let Err(e) = std::fs::set_permissions(&socket_path, perms) {
        tracing::warn!(error = %e, "failed to set socket permissions, using default");
    }

    tracing::info!(path = %socket_path.display(), perm = format_args!("{socket_perm:o}"), "socket server listening");

    loop {
        let (stream, _) = listener
            .accept()
            .await
            .map_err(|e| SocketError::AcceptFailed(e.to_string()))?;

        let state = Arc::clone(&state);
        tokio::spawn(handle_connection(stream, state));
    }
}

/// Handle a single client connection. Reads JSON-RPC requests line by line.
async fn handle_connection(stream: UnixStream, state: Arc<ServerState>) {
    let (reader, mut writer) = stream.into_split();
    let mut buf_reader = BufReader::new(reader);

    loop {
        let mut line = String::new();
        match buf_reader.read_line(&mut line).await {
            Ok(0) => break, // EOF
            Ok(_) => {}
            Err(e) => {
                tracing::debug!(error = %e, "connection read error, closing");
                break;
            }
        }

        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        let response = handle_request(line, &state).await;
        let response_line = serde_json::to_string(&response).unwrap_or_else(|_| {
            serde_json::to_string(&error_response(&None, -32603, "Internal error", None)).unwrap()
        });

        if writer.write_all(response_line.as_bytes()).await.is_err()
            || writer.write_all(b"\n").await.is_err()
        {
            break; // Client disconnected
        }
    }
}

/// Process a single JSON-RPC request and return the response.
async fn handle_request(line: &str, state: &Arc<ServerState>) -> Value {
    let request: JsonRpcRequest = match serde_json::from_str(line) {
        Ok(req) => req,
        Err(e) => {
            return error_response(
                &None,
                -32700,
                "Parse error",
                Some(json!({ "detail": e.to_string() })),
            );
        }
    };

    let id = request.id.clone();

    match request.method.as_str() {
        "system.status" => handle_system_status(&id, state).await,
        "contract.info" => handle_contract_info(&id, state).await,
        "ledger.query" => handle_ledger_query(&id, request.params, state).await,
        "ledger.tail" => handle_ledger_tail(&id, request.params, state).await,
        "ledger.stats" => handle_ledger_stats(&id, state).await,
        // Phase 1: Resource broker methods
        "resources.allocate" => {
            crate::api_broker::handle_resources_allocate(&id, request.params, state).await
        }
        "resources.release" => {
            crate::api_broker::handle_resources_release(&id, request.params, state).await
        }
        "resources.heartbeat" => {
            crate::api_broker::handle_resources_heartbeat(&id, request.params, state).await
        }
        "resources.list" => crate::api_broker::handle_resources_list(&id, state).await,
        "resources.available" => crate::api_broker::handle_resources_available(&id, state).await,
        "resources.add_process" => {
            crate::api_broker::handle_resources_add_process(&id, request.params, state).await
        }
        // Phase 2: Policy and intent methods
        "intent.declare" => handle_intent_declare(
            &id,
            request.params.unwrap_or(Value::Null),
            state,
        )
        .await,
        "intent.status" => {
            handle_intent_status(&id, request.params.unwrap_or(Value::Null), state).await
        },
        "policy.list" => handle_policy_list(&id, state).await,
        "policy.reload" => handle_policy_reload(&id, state).await,
        // Risk assessment (dry-run)
        "resources.risk_assess" => {
            handle_risk_assess(&id, request.params.unwrap_or(Value::Null), state).await
        },
        // Phase 3: Dream scheduler methods
        "dream.status" => handle_dream_status(&id, state).await,
        "dream.cancel" => handle_dream_cancel(&id, request.params.unwrap_or(Value::Null), state).await,
        "dream.tasks" => handle_dream_tasks(&id, state).await,
        "dream.register_task" => {
            handle_dream_register_task(&id, request.params.unwrap_or(Value::Null), state).await
        },
        "dream.report" => {
            handle_dream_report(&id, request.params.unwrap_or(Value::Null), state).await
        },
        _ => error_response(
            &id,
            -32601,
            "Method not found",
            Some(json!({
                "available": [
                    "system.status", "ledger.query", "ledger.tail", "ledger.stats",
                    "resources.allocate", "resources.release", "resources.heartbeat",
                    "resources.list", "resources.available", "resources.add_process",
                    "intent.declare", "intent.status", "policy.list", "policy.reload",
                    "resources.risk_assess",
                    "dream.status", "dream.cancel", "dream.tasks",
                    "dream.register_task", "dream.report"
                ]
            })),
        ),
    }
}

/// Handle `system.status` — returns current resource state and idle status.
async fn handle_system_status(id: &Option<Value>, state: &Arc<ServerState>) -> Value {
    let detector = state.detector.lock().unwrap();
    let latest = detector.latest_snapshot();

    let result = if let Some(snap) = latest {
        json!({
            "success": true,
            "data": {
                "snapshot": snap,
                "idle": {
                    "is_idle": detector.is_system_idle,
                    "idle_since": detector.idle_since,
                    "consecutive_idle_samples": detector.consecutive_idle_samples(),
                },
                "uptime_secs": (Utc::now() - state.started_at).num_seconds(),
            },
            "doubt": {
                "assumptions": ["Resource readings are point-in-time samples, not averages"],
                "blind_spots": ["Cannot see resource usage of processes in other cgroups/namespaces"],
                "last_validation": Utc::now().to_rfc3339(),
                "stale_after": (Utc::now() + chrono::Duration::seconds(state.poll_interval_s as i64)).to_rfc3339(),
            },
        })
    } else {
        json!({
            "success": true,
            "data": {
                "snapshot": null,
                "message": "No resource snapshot yet — observer is still initializing",
                "uptime_secs": (Utc::now() - state.started_at).num_seconds(),
            },
        })
    };

    success_response(id, result)
}

/// Handle `contract.info` — return the full API contract for introspection.
async fn handle_contract_info(id: &Option<Value>, _state: &Arc<ServerState>) -> Value {
    let contract = crate::contract::ApiContract::current();
    success_response(
        id,
        serde_json::to_value(&contract).unwrap_or(json!({ "error": "failed to serialize contract" })),
    )
}

/// Handle `ledger.query` — query events with filters.
async fn handle_ledger_query(
    id: &Option<Value>,
    params: Option<Value>,
    state: &Arc<ServerState>,
) -> Value {
    let params = params.unwrap_or(json!({}));

    let since: Option<DateTime<Utc>> = params
        .get("since")
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse().ok());

    let event_type: Option<String> = params
        .get("event_type")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let limit: usize = params.get("limit").and_then(|v| v.as_u64()).unwrap_or(100) as usize;

    let ledger = state.ledger.lock().unwrap();
    match ledger.query(since, event_type.as_deref(), limit.min(1000)) {
        Ok(events) => success_response(
            id,
            json!({
                "success": true,
                "data": {
                    "events": events,
                    "count": events.len(),
                },
            }),
        ),
        Err(e) => error_response(
            id,
            -32603,
            "Ledger query failed",
            Some(json!({ "error": e.to_string() })),
        ),
    }
}

/// Handle `ledger.tail` — get most recent N events.
async fn handle_ledger_tail(
    id: &Option<Value>,
    params: Option<Value>,
    state: &Arc<ServerState>,
) -> Value {
    let n: usize = params
        .as_ref()
        .and_then(|p| p.get("n").and_then(|v| v.as_u64()))
        .unwrap_or(20) as usize;

    let ledger = state.ledger.lock().unwrap();
    match ledger.tail(n.min(1000)) {
        Ok(events) => success_response(
            id,
            json!({
                "success": true,
                "data": {
                    "events": events,
                    "count": events.len(),
                },
            }),
        ),
        Err(e) => error_response(
            id,
            -32603,
            "Ledger tail failed",
            Some(json!({ "error": e.to_string() })),
        ),
    }
}

/// Handle `ledger.stats` — get ledger statistics.
async fn handle_ledger_stats(id: &Option<Value>, state: &Arc<ServerState>) -> Value {
    let ledger = state.ledger.lock().unwrap();
    match ledger.entry_count() {
        Ok(count) => success_response(
            id,
            json!({
                "success": true,
                "data": {
                    "entry_count": count,
                    "path": ledger_path_string(&state),
                },
            }),
        ),
        Err(e) => error_response(
            id,
            -32603,
            "Ledger stats failed",
            Some(json!({ "error": e.to_string() })),
        ),
    }
}

fn ledger_path_string(_state: &Arc<ServerState>) -> String {
    // We don't store the path in ServerState directly; this is a placeholder
    // for Phase 0. In Phase 1+, we'll add proper config reporting.
    "/var/log/riod/events.jsonl".to_string()
}

// Phase 1 resource broker handlers live in crate::api_broker

// ─── Phase 2: Policy and Intent Handlers ──────────────────────────────

/// Handle `intent.declare` — declare an intent to use resources for a purpose.
///
/// Params: { client_id, purpose, description?, estimated_duration_s?, metadata? }
async fn handle_intent_declare(id: &Option<Value>, params: Value, state: &Arc<ServerState>) -> Value {
    let client_id = match params.get("client_id").and_then(|v| v.as_str()) {
        Some(c) => c.to_string(),
        None => return error_response(id, -32602, "Missing required param: client_id", None),
    };

    let purpose_str = match params.get("purpose").and_then(|v| v.as_str()) {
        Some(p) => p.to_string(),
        None => return error_response(id, -32602, "Missing required param: purpose", None),
    };

    // Parse purpose (tolerant — unknown purposes become Custom)
    let purpose: crate::policy::Purpose = serde_json::from_value(json!(purpose_str))
        .unwrap_or_else(|_| crate::policy::Purpose::Custom(purpose_str.clone()));

    let description = params
        .get("description")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    let estimated_duration_s = params
        .get("estimated_duration_s")
        .and_then(|v| v.as_u64());

    let metadata = params.get("metadata").cloned().unwrap_or(json!({}));

    // Get current snapshot for condition evaluation
    let detector_guard = state.detector.lock().unwrap();
    let latest_snapshot = match detector_guard.latest_snapshot() {
        Some(s) => s,
        None => {
            drop(detector_guard);
            return error_response(
                id,
                -32603,
                "No resource snapshot available yet (daemon just started?)",
                None,
            );
        }
    };

    // Evaluate intent against policies using the policy engine
    let intent = {
        let mut engine = state.policy_engine.lock().unwrap();
        engine.evaluate_intent(
            &client_id,
            purpose.clone(),
            description,
            estimated_duration_s,
            metadata.clone(),
            &latest_snapshot,
            &detector_guard,
        )
    };
    drop(detector_guard);

    // Log the intent decision to the ledger
    {
        let mut ledger = state.ledger.lock().unwrap();
        let event_type = match &intent.status {
            crate::policy::IntentStatus::Granted => "intent_granted",
            crate::policy::IntentStatus::Denied(_) => "intent_denied",
            _ => "intent_declared",
        };
        let _ = ledger.append(crate::ledger::LedgerEvent::Info {
            message: format!("{}: {} ({})", event_type, intent.id, purpose),
            data: Some(json!({
                "intent_id": intent.id,
                "client_id": client_id,
                "purpose": purpose_str,
                "status": &intent.status,
                "policy_applied": intent.policy_applied,
                "failed_conditions": intent.failed_conditions,
                "risk_score": intent.risk.as_ref().map(|r| r.total),
                "risk_level": intent.risk.as_ref().map(|r| r.level.to_string()),
            })),
        });
    }

    success_response(id, json!({
        "success": true,
        "data": intent,
    }))
}

/// Handle `intent.status` — check the status of a previously declared intent.
///
/// Params: { id }
async fn handle_intent_status(id: &Option<Value>, params: Value, state: &Arc<ServerState>) -> Value {
    let intent_id = match params.get("id").and_then(|v| v.as_str()) {
        Some(i) => i.to_string(),
        None => return error_response(id, -32602, "Missing required param: id", None),
    };

    let engine = state.policy_engine.lock().unwrap();
    match engine.get_intent(&intent_id) {
        Some(intent) => success_response(id, json!({ "success": true, "data": intent })),
        None => error_response(
            id,
            -32602,
            "Intent not found",
            Some(json!({ "id": intent_id })),
        ),
    }
}

/// Handle `policy.list` — list all loaded policies.
async fn handle_policy_list(id: &Option<Value>, state: &Arc<ServerState>) -> Value {
    let engine = state.policy_engine.lock().unwrap();
    let count = engine.policy_count();
    success_response(
        id,
        json!({
            "success": true,
            "data": {
                "policy_count": count,
            },
        }),
    )
}

/// Handle `policy.reload` — reload policies from the configured file.
async fn handle_policy_reload(id: &Option<Value>, state: &Arc<ServerState>) -> Value {
    let mut engine = state.policy_engine.lock().unwrap();
    match engine.reload() {
        Ok(count) => success_response(
            id,
            json!({
                "success": true,
                "data": { "reloaded_policies": count },
            }),
        ),
        Err(e) => error_response(id, -32603, "Policy reload failed", Some(json!({ "error": e.to_string() }))),
    }
}

/// Handle `resources.risk_assess` — dry-run risk assessment without declaring intent.
async fn handle_risk_assess(id: &Option<Value>, params: Value, state: &Arc<ServerState>) -> Value {
    let req: crate::risk::RiskInput = match serde_json::from_value(params) {
        Ok(r) => r,
        Err(e) => {
            return error_response(
                id,
                -32602,
                "Invalid params",
                Some(json!({ "error": format!("expected RiskInput: {}", e) })),
            );
        },
    };

    let detector = state.detector.lock().unwrap();
    let snapshot = match detector.latest_snapshot() {
        Some(s) => s,
        None => {
            return error_response(
                id,
                -32603,
                "No resource snapshot available yet",
                Some(json!({ "message": "observer is still initializing" })),
            );
        },
    };

    let engine = state.policy_engine.lock().unwrap();
    let score = engine.assess_risk(&req, &snapshot);

    success_response(
        id,
        json!({
            "success": true,
            "data": score,
        }),
    )
}

// ─── Phase 3: Dream Scheduler Handlers ──────────────────────────────

/// Handle `dream.status` — query the dream scheduler state.
async fn handle_dream_status(id: &Option<Value>, state: &Arc<ServerState>) -> Value {
    let scheduler = state.dream_scheduler.lock().unwrap();
    let status = scheduler.state();

    success_response(
        id,
        json!({
            "success": true,
            "data": status,
        }),
    )
}

/// Handle `dream.cancel` — manually cancel an active dream cycle.
/// This triggers preemption: all allocations tagged with the cycle ID are released.
///
/// Params: { reason? } (optional reason string)
async fn handle_dream_cancel(id: &Option<Value>, params: Value, state: &Arc<ServerState>) -> Value {
    let reason = params
        .get("reason")
        .and_then(|v| v.as_str())
        .unwrap_or("manual cancel via API")
        .to_string();

    // Cancel the dream cycle (may produce a Preempted event)
    let event = {
        let mut scheduler = state.dream_scheduler.lock().unwrap();
        scheduler.cancel(&reason)
    };

    match event {
        Some(event) => {
            // Log the cancellation to the ledger
            let data = serde_json::to_value(&event).unwrap_or_default();
            let mut ledger = state.ledger.lock().unwrap();
            if let Err(e) = ledger.append(crate::ledger::LedgerEvent::DreamEvent { data }) {
                tracing::warn!(error = %e, "failed to log dream cancel event");
            }

            // Release all allocations tagged with this cycle ID
            if let Some(cycle_id) = extract_cancel_cycle_id(&event) {
                let reclaimed = {
                    let mut broker = state.broker.lock().unwrap();
                    let mut ledger = state.ledger.lock().unwrap();
                    broker.reclaim_dream_cycle(&cycle_id, &mut ledger)
                };
                tracing::info!(cycle_id = %cycle_id, reclaimed, "released dream cycle allocations on cancel");

                success_response(
                    id,
                    json!({
                        "success": true,
                        "data": {
                            "cancelled": true,
                            "reason": reason,
                            "cycle_id": cycle_id,
                            "allocations_released": reclaimed,
                        },
                    }),
                )
            } else {
                success_response(
                    id,
                    json!({
                        "success": true,
                        "data": {
                            "cancelled": true,
                            "reason": reason,
                            "allocations_released": 0,
                        },
                    }),
                )
            }
        }
        None => error_response(
            id,
            -32602,
            "No active dream cycle to cancel",
            Some(json!({ "message": "dream scheduler is not in Triggered state" })),
        ),
    }
}

/// Extract the cycle ID from a Preempted event (used by dream.cancel handler).
fn extract_cancel_cycle_id(event: &crate::dream::DreamEvent) -> Option<String> {
    use crate::dream::DreamEvent;
    match event {
        DreamEvent::Preempted { cycle_id, .. } => cycle_id.clone(),
        _ => None,
    }
}

/// Handle `dream.tasks` — list all registered dream tasks.
async fn handle_dream_tasks(id: &Option<Value>, state: &Arc<ServerState>) -> Value {
    let registry = state.dream_tasks.lock().unwrap();
    let tasks: Vec<&crate::dream_tasks::DreamTaskInfo> = registry.list();

    success_response(
        id,
        json!({
            "success": true,
            "data": {
                "tasks": tasks,
                "count": tasks.len(),
            },
        }),
    )
}

/// Handle `dream.register_task` — register a dream task with the registry.
///
/// Params: DreamTaskInfo fields (name, description, provider, estimated_duration_mins, requires_gpu, min_idle_threshold_mins?)
async fn handle_dream_register_task(id: &Option<Value>, params: Value, state: &Arc<ServerState>) -> Value {
    let info: crate::dream_tasks::DreamTaskInfo = match serde_json::from_value(params) {
        Ok(i) => i,
        Err(e) => {
            return error_response(
                id,
                -32602,
                "Invalid params",
                Some(json!({ "error": format!("expected DreamTaskInfo: {}", e) })),
            );
        },
    };

    let mut registry = state.dream_tasks.lock().unwrap();
    match registry.register(info.clone()) {
        Ok(()) => success_response(
            id,
            json!({
                "success": true,
                "data": { "registered": true, "task": info },
            }),
        ),
        Err(e) => error_response(id, -32602, &e, None),
    }
}

/// Handle `dream.report` — submit a dream task result.
///
/// Params: DreamTaskResult fields (task_name, cycle_id, client_id, duration_secs, success, error?, observations)
async fn handle_dream_report(id: &Option<Value>, params: Value, state: &Arc<ServerState>) -> Value {
    let mut result: crate::dream_tasks::DreamTaskResult = match serde_json::from_value(params) {
        Ok(r) => r,
        Err(e) => {
            return error_response(
                id,
                -32602,
                "Invalid params",
                Some(json!({ "error": format!("expected DreamTaskResult: {}", e) })),
            );
        },
    };

    // Fill in completed_at if not provided
    let now = Utc::now();
    result.completed_at = now;

    // Log the task completion to the ledger
    let event = crate::dream_tasks::DreamTaskEvent::Completed {
        at: now,
        task_name: result.task_name.clone(),
        cycle_id: result.cycle_id.clone(),
        client_id: result.client_id.clone(),
        success: result.success,
        duration_secs: result.duration_secs,
        observation_count: result.observations.len(),
    };

    let mut ledger = state.ledger.lock().unwrap();
    if let Err(e) = ledger.append(crate::ledger::LedgerEvent::DreamEvent {
        data: serde_json::to_value(&event).unwrap_or_default(),
    }) {
        tracing::warn!(error = %e, "failed to log dream task completion");
    }

    // Also store the full result as a separate ledger entry for provenance
    let _ = ledger.append(crate::ledger::LedgerEvent::DreamEvent {
        data: serde_json::json!({
            "task_event": "result",
            "result": result,
        }),
    });

    success_response(
        id,
        json!({
            "success": true,
            "data": {
                "reported": true,
                "observation_count": result.observations.len(),
            },
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::broker::ResourceBroker;
    use crate::ledger::{EventLedger, LedgerConfig, LedgerEvent};
    use crate::observe::{IdleDetector, ObserveConfig};
    use std::sync::{Arc, Mutex};
    use tempfile::tempdir;

    fn test_state() -> (Arc<ServerState>, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let ledger_path = dir.path().join("test-ledger.jsonl");

        let detector = IdleDetector::new(ObserveConfig::default());
        let ledger = EventLedger::open(LedgerConfig {
            path: ledger_path,
            retention_days: 7,
        })
        .unwrap();

        // Create a test broker with fake GPUs
        let gpus = vec![crate::observe::GpuInfo {
            index: 0,
            name: "Test GPU".into(),
            vram_total_mb: 16000,
            vram_used_mb: 0,
            utilization_percent: 0,
        }];
        let broker = ResourceBroker::new(gpus, 65536);
        let policy_engine = crate::policy::PolicyEngine::new();
        let dream_scheduler = crate::dream::DreamScheduler::new(crate::dream::DreamConfig {
            enabled: false,
            ..Default::default()
        });
        let dream_tasks = crate::dream_tasks::DreamTaskRegistry::new();

        let state = Arc::new(ServerState {
            detector: Mutex::new(detector),
            ledger: Mutex::new(ledger),
            broker: Mutex::new(broker),
            policy_engine: Mutex::new(policy_engine),
            dream_scheduler: Mutex::new(dream_scheduler),
            dream_tasks: Mutex::new(dream_tasks),
            started_at: Utc::now(),
            poll_interval_s: 30,
        });

        (state, dir)
    }

    #[tokio::test]
    async fn test_system_status_no_snapshot() {
        let (state, _dir) = test_state();

        let request = json!({
            "jsonrpc": "2.0",
            "method": "system.status",
            "id": 1
        });

        let response = handle_request(&request.to_string(), &state).await;
        assert_eq!(response["jsonrpc"], "2.0");
        assert_eq!(response["id"], 1);
        assert!(response["result"]["success"].as_bool().unwrap());
        assert!(response["result"]["data"]["snapshot"].is_null());
    }

    #[tokio::test]
    async fn test_contract_info() {
        let (state, _dir) = test_state();

        let request = json!({
            "jsonrpc": "2.0",
            "method": "contract.info",
            "id": 10
        });

        let response = handle_request(&request.to_string(), &state).await;
        assert_eq!(response["jsonrpc"], "2.0");
        assert_eq!(response["id"], 10);
        assert!(response["error"].is_null());

        let data = &response["result"];
        assert_eq!(data["name"], "riod");
        // Version is a structured object: {major, minor, patch}
        assert!(data["version"]["major"].is_u64());
        assert!(data["version"]["minor"].is_u64());
        assert!(data["version"]["patch"].is_u64());
        assert!(data["methods"].is_object());
        assert!(data["capabilities"].is_array());

        // Verify key methods are present
        let methods = &data["methods"];
        assert!(methods["system.status"].is_object());
        assert!(methods["resources.allocate"].is_object());
        assert!(methods["contract.info"].is_object());
    }

    #[tokio::test]
    async fn test_ledger_query_empty() {
        let (state, _dir) = test_state();

        let request = json!({
            "jsonrpc": "2.0",
            "method": "ledger.query",
            "params": {"limit": 10},
            "id": 2
        });

        let response = handle_request(&request.to_string(), &state).await;
        assert_eq!(response["result"]["success"], true);
        assert_eq!(response["result"]["data"]["count"], 0);
    }

    #[tokio::test]
    async fn test_unknown_method() {
        let (state, _dir) = test_state();

        let request = json!({
            "jsonrpc": "2.0",
            "method": "nonexistent.method",
            "id": 3
        });

        let response = handle_request(&request.to_string(), &state).await;
        assert!(response["error"].is_object());
        assert_eq!(response["error"]["code"], -32601);
    }

    #[tokio::test]
    async fn test_invalid_json() {
        let (state, _dir) = test_state();

        let response = handle_request("not valid json", &state).await;
        assert!(response["error"].is_object());
        assert_eq!(response["error"]["code"], -32700);
    }

    #[tokio::test]
    async fn test_ledger_tail_with_events() {
        let (state, _dir) = test_state();

        // Add some events directly
        {
            let mut ledger = state.ledger.lock().unwrap();
            for i in 0..5 {
                ledger
                    .append(LedgerEvent::Info {
                        message: format!("event {}", i),
                        data: None,
                    })
                    .unwrap();
            }
        }

        let request = json!({
            "jsonrpc": "2.0",
            "method": "ledger.tail",
            "params": {"n": 3},
            "id": 4
        });

        let response = handle_request(&request.to_string(), &state).await;
        assert_eq!(response["result"]["data"]["count"], 3);
    }
}
