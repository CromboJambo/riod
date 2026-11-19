# rIOd Fleet Coordination via crabjar-conductor (Phase 3.6)

Status: Spec — multi-machine dream scheduling and resource coordination
Scope: New `fleet` module in riod, integration with crabjar-conductor API
Prerequisites: Phase 0-2 complete, Phase 3 GPU dreaming (dream.allocate_gpu), crabjar-conductor deployed on jambo:8091

## §0 Problem Statement

A single riod instance manages resources on one machine. In a home-lab fleet, dream work should be able to:

1. **Migrate**: If machine A is idle but machine B has more GPUs, route heavy dream tasks to B
2. **Aggregate**: Combine residual capacity across machines for large tasks (e.g., multi-GPU training)
3. **Balance**: Avoid starting dream cycles on machines that are about to receive foreground work
4. **Report**: Provide a fleet-wide view of dream capacity and activity

Currently, each riod instance is an island. crabjar-conductor already has hardware reporting (`POST /api/report`) and task dispatch. This spec defines how riod instances register with and coordinate through the conductor.

## §1 Design Principles

1. **Conductor as coordinator, not executor.** The conductor knows *what* to dream about and *where*, but riod on each machine still handles local allocation, preemption, and enforcement. The conductor never directly allocates resources — it sends requests to riod instances.

2. **Local autonomy preserved.** A riod instance can operate fully standalone (no conductor). Fleet coordination is an opt-in enhancement that layers on top of local dream scheduling. If the conductor is unreachable, local behavior is unchanged.

3. **Report-then-request pattern.** Riod instances periodically report their state to the conductor. The conductor makes routing decisions based on aggregated state. When a dream task should run on a specific machine, the conductor sends an allocation request to that machine's riod.

4. **No cross-machine resource ownership.** Each riod owns its local resources. The conductor maintains a *view* of fleet state for routing purposes but never holds allocations. If a machine goes offline, its allocations are simply gone — the conductor detects this and reroutes.

5. **Ledger integration.** All fleet coordination events (registration, capacity reports, routing decisions) are logged to both the local riod ledger and reported to the conductor for fleet-wide auditability.

## §2 Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    crabjar-conductor                     │
│                   (jambo:8091)                           │
│  ┌─────────────┐  ┌──────────────┐  ┌────────────────┐  │
│  │ Fleet State │  │ Dream Router │  │ Task Dispatcher│  │
│  │ Registry    │  │              │  │                │  │
│  └──────┬──────┘  └──────┬───────┘  └───────┬────────┘  │
└─────────┼────────────────┼──────────────────┼───────────┘
          │                │                  │
     HTTPS/JSON       HTTPS/JSON         HTTPS/JSON
          │                │                  │
    ┌─────▼─────┐     ┌────▼─────┐       ┌────▼─────┐
    │  riod A   │     │  riod B  │       │  riod C  │
    │ (ftw3)    │     │ (lappy)  │       │(sovnode) │
    │ GPU: 1x3070│    │ GPU: 0   │       │ GPU: 0   │
    └───────────┘     └──────────┘       └──────────┘
```

### 2.1 Roles

| Component | Responsibility |
|-----------|---------------|
| **riod (each machine)** | Local resource management, dream scheduling, capacity reporting, executing dream allocations |
| **crabjar-conductor** | Fleet state aggregation, dream task routing, coordination decisions |
| **Dream clients (crabjar, etc.)** | Submit dream tasks to the conductor (or directly to a riod for local-only work) |

## §3 Conductor API Extensions

### 3.1 New Endpoints

| Method | Path | Purpose |
|--------|------|---------|
| `POST` | `/api/fleet/register` | Riod registers itself with hardware inventory |
| `PUT` | `/api/fleet/{node_id}/status` | Periodic status/capacity update from riod |
| `DELETE` | `/api/fleet/{node_id}` | Riod deregisters (clean shutdown) or conductor removes stale node |
| `GET` | `/api/fleet/status` | Fleet-wide view of all registered nodes and their capacity |
| `POST` | `/api/dream/route` | Request dream task routing to an appropriate node |
| `GET` | `/api/dream/fleet-capacity` | Aggregate dream capacity across the fleet |

### 3.2 Registration Payload

```json
{
  "node_id": "ftw3",
  "hostname": "ftw3.home.lan",
  "riod_version": "0.1.1",
  "socket_path": "/run/riod.sock",
  "hardware": {
    "gpus": [{"index": 0, "name": "RTX 3070 Ti", "vram_mb": 8192}],
    "total_ram_mb": 32768,
    "cpu_cores": 16
  },
  "dream_config": {
    "enabled": true,
    "idle_threshold_mins": 120,
    "max_cycle_duration_mins": 30,
    "cooldown_mins": 240
  },
  "last_seen": "2026-11-19T12:00:00Z"
}
```

### 3.3 Status Update Payload (periodic, every 60s)

```json
{
  "node_id": "ftw3",
  "timestamp": "2026-11-19T12:01:00Z",
  "state": "idle" | "active" | "dreaming",
  "current_dream_cycle_id": "cycle-abc123",
  "capacity": {
    "gpu_slots_free": 1,
    "gpu_mem_mib_free": 8192,
    "ram_mib_free": 16384,
    "cpu_cores_free": 8.0
  },
  "active_allocations": 0,
  "dream_allocations": 1
}
```

### 3.4 Dream Routing Request

```json
// POST /api/dream/route
{
  "task_type": "inference" | "training" | "analysis",
  "requirements": {
    "gpu_slots": 1,
    "gpu_mem_mib": 8192,
    "ram_mib": 4096,
    "duration_secs": 1800
  },
  "constraints": {
    "prefer_nodes": ["ftw3"],
    "exclude_nodes": [],
    "max_latency_ms": null
  }
}

// Response
{
  "routed_to": "ftw3",
  "allocation_request": {
    // The exact payload to send to riod's dream.allocate_gpu
    "client_id": "conductor-routed-task-xyz",
    "purpose": "dreaming",
    "vram_request_mb": 8192,
    "duration_secs": 1800
  },
  "reason": "Only node with sufficient GPU capacity"
}
```

## §4 Riod Fleet Module

### 4.1 New Module: `src/fleet.rs`

```rust
/// Fleet coordination client for riod.
/// Handles registration, status reporting, and receiving routed dream tasks.
pub struct FleetClient {
    conductor_url: String,
    node_id: String,
    http_client: reqwest::Client,
    report_interval: Duration,
    registered: bool,
}

impl FleetClient {
    pub fn new(conductor_url: String, node_id: String) -> Self;
    
    /// Register this riod instance with the conductor.
    pub async fn register(&mut self, inventory: &HardwareInventory) -> Result<(), FleetError>;
    
    /// Send periodic status update.
    pub async fn report_status(&self, status: &FleetStatus) -> Result<(), FleetError>;
    
    /// Deregister (clean shutdown).
    pub async fn deregister(&self) -> Result<(), FleetError>;
    
    /// Check if conductor is reachable.
    pub async fn ping(&self) -> bool;
}
```

### 4.2 Configuration

New CLI flags for `riod daemon`:

```
--conductor-url <URL>       # Conductor API endpoint (e.g., http://jambo:8091)
--fleet-node-id <ID>        # This node's identifier in the fleet (e.g., "ftw3")
--fleet-report-interval <S> # Status report interval in seconds (default: 60)
```

Fleet coordination is **disabled** unless `--conductor-url` is provided.

### 4.3 Background Reporting Task

Similar to the existing observer loop and reclaim loop, a third background task handles fleet reporting:

```rust
async fn run_fleet_report_loop(state: Arc<ServerState>, fleet_client: Arc<Mutex<FleetClient>>) {
    let interval = Duration::from_secs(60);
    
    loop {
        tokio::time::sleep(interval).await;
        
        // Gather current state
        let status = FleetStatus {
            node_id: fleet_client.lock().unwrap().node_id(),
            timestamp: Utc::now(),
            state: get_current_state(&state),
            current_dream_cycle_id: state.dream_scheduler.lock().unwrap().current_cycle_id(),
            capacity: compute_fleet_capacity(&state),
            active_allocations: state.broker.lock().unwrap().active_allocation_count(),
            dream_allocations: state.broker.lock().unwrap().dream_allocation_count(),
        };
        
        // Report (best-effort — log warning on failure, don't crash)
        if let Err(e) = fleet_client.lock().unwrap().report_status(&status).await {
            tracing::warn!(error = %e, "failed to report fleet status");
        }
    }
}
```

### 4.4 Receiving Routed Tasks

When the conductor routes a dream task to this node, it calls riod's existing `dream.allocate_gpu` method (via Unix socket or a new HTTP endpoint). No new local API is needed — the conductor acts as a client that calls the same API as any other consumer.

Optionally, riod could expose a lightweight HTTP endpoint for conductor-to-riod communication:

```
POST /api/fleet/allocate  # Same as dream.allocate_gpu but over HTTP
```

This would allow the conductor to trigger allocations without needing Unix socket access. **Preferred approach**: keep using Unix socket locally (conductor runs `riod-client` or uses SSH). Only add HTTP if needed.

## §5 Conductor-Side Logic

### 5.1 Fleet State Registry

The conductor maintains an in-memory registry of all registered nodes with their last-reported status. Nodes that miss 3 consecutive status reports are marked `stale`; after 10 minutes, they're marked `offline` and excluded from routing.

### 5.2 Dream Routing Algorithm

```
route_dream_task(requirements, constraints):
    candidates = [node for node in fleet if node.state == "idle" or node.state == "dreaming"]
    candidates = filter_by_requirements(candidates, requirements)
    candidates = apply_constraints(candidates, constraints)
    
    if candidates is empty:
        return None  # No suitable node
    
    # Score candidates by:
    # 1. Available capacity margin (prefer nodes with more headroom)
    # 2. Current load (prefer less loaded)
    # 3. Proximity to data (if task has data locality hints)
    
    best = max(candidates, key=score)
    return RoutingDecision(node=best, allocation_request=build_request(requirements))
```

### 5.3 Fleet-Wide Dream Capacity View

`GET /api/dream/fleet-capacity` returns:

```json
{
  "total_nodes": 4,
  "online_nodes": 3,
  "idle_nodes": 2,
  "dreaming_nodes": 1,
  "aggregate_capacity": {
    "gpu_slots_free": 2,
    "gpu_mem_mib_free": 16384,
    "ram_mib_free": 49152
  },
  "nodes": [
    {"id": "ftw3", "state": "dreaming", "gpu_free": 0, "in_dream_cycle": true},
    {"id": "lappy", "state": "idle", "gpu_free": 0, "ram_free_mb": 8192},
    {"id": "sovnode", "state": "idle", "gpu_free": 0, "ram_free_mb": 4096}
  ]
}
```

## §6 Integration with Existing Dream Infrastructure

### 6.1 Local-First Routing

If a dream client submits a task directly to a local riod (via Unix socket), it runs locally regardless of fleet state. Fleet routing only applies when tasks are submitted through the conductor.

### 6.2 Preemption Coordination

If the conductor routes a high-priority task to a node that's currently dreaming, the conductor can trigger `dream.cancel` on that node before sending the new allocation. This is a two-step operation:

1. `POST /api/fleet/{node_id}/dream/cancel` (conductor → riod)
2. Wait for confirmation
3. `POST /api/fleet/{node_id}/dream/allocate_gpu` (conductor → riod)

### 6.3 Failure Handling

| Scenario | Behavior |
|----------|----------|
| Conductor unreachable | Riod continues operating locally; reports queued and discarded on failure |
| Node goes offline during dream cycle | Conductor marks node offline after timeout; dream allocations are lost (local riod is gone) |
| Node comes back online | Re-registers with conductor; any in-progress dream work is local-only (conductor doesn't track it) |
| Duplicate routing (two tasks to same node) | Riod's local broker handles capacity — second task gets rejected if no room |

## §7 Implementation Phases

### Phase 3.6a: Registration + Status Reporting

1. Add `src/fleet.rs` with `FleetClient`
2. Add CLI flags for conductor URL and node ID
3. Implement registration and periodic status reporting
4. Add conductor endpoints: `/api/fleet/register`, `/api/fleet/{node_id}/status`, `/api/fleet/status`
5. **No routing yet** — just visibility into fleet state

### Phase 3.6b: Dream Routing

1. Implement conductor-side routing algorithm
2. Add `/api/dream/route` endpoint
3. Conductor sends routed allocations to target riod (via SSH + riod-client or HTTP)
4. End-to-end test: submit dream task to conductor → routed to appropriate node → allocated locally

### Phase 3.6c: Coordination + Preemption

1. Implement preemption coordination (cancel dream on target before routing new work)
2. Add fleet-wide capacity view endpoint
3. Stale/offline detection and handling
4. Integration tests with multiple riod instances

## §8 Testing Strategy

### 8.1 Unit Tests

- `FleetClient` registration/deregistration with mock conductor
- Routing algorithm with various fleet states
- Capacity aggregation calculations
- Stale node detection logic

### 8.2 Integration Tests

- Two riod instances + mock conductor: verify status reporting from both
- Route a dream task to the node with available capacity
- Verify preemption: cancel dream on busy node, then allocate
- Simulate node going offline: verify conductor excludes it from routing

### 8.3 Manual Fleet Test

1. Deploy riod with `--conductor-url` on ftw3 and lappy
2. Verify both appear in `/api/fleet/status`
3. Submit a dream task requiring GPU → should route to ftw3
4. Submit a CPU-only dream task → can route to either idle node
5. Kill riod on one node → verify it's marked offline after timeout

## §9 Open Questions

1. **Authentication**: How do riod instances authenticate with the conductor? Options: shared secret token, TLS mutual auth, or trust the LAN (current home-lab setup). For now: no auth (LAN-only), add later if needed.

2. **Task persistence**: If the conductor restarts, does it lose pending dream tasks? Current design: no persistence (dream work is ephemeral by nature). Future: persist to disk if needed.

3. **Multi-GPU aggregation**: Can a single dream task span multiple machines (e.g., distributed training)? Out of scope for this spec — that requires a different coordination model (parameter server, etc.). This spec handles *routing* whole tasks to nodes, not splitting tasks across nodes.

4. **Priority preemption**: Should the conductor be able to preempt a low-priority dream task on one node to route a higher-priority task there? Current design: yes, via the cancel-then-allocate pattern. Priority levels need to be defined in the routing request.

## Appendix A: Relationship to crabjar-conductor Existing APIs

The conductor already has:
- `POST /api/report` — hardware/environment reporting (similar to fleet registration)
- `GET /api/reports` — list reports from nodes

This spec **extends** the conductor with fleet-specific endpoints rather than overloading the existing report API. The existing `/api/report` is for one-time hardware discovery; `/api/fleet/register` + `/api/fleet/{node_id}/status` is for continuous fleet membership and status tracking.

## Appendix B: Security Considerations

- Conductor-to-riod communication should be over TLS in production (home-lab: plain HTTP on trusted LAN is acceptable for now)
- Node IDs should be unique and stable (use hostname or a configured ID, not random)
- The conductor should validate that status updates come from the node they claim to be from (IP allowlist or token-based auth)
- Dream allocations routed by the conductor are subject to the same local policy enforcement as any other allocation — the conductor can't bypass riod's risk scoring
