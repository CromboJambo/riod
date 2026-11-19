# rIOd Waypoint Routing — Fleet-Wide Resource Navigation (Phase 4.1)

Status: Spec — extends FLEET_COORDINATION_SPEC for resource-aware navigation
Scope: New `waypoints` module in riod, integration with crabjar waypoint system and conductor API
Prerequisites: Phase 3 complete (GPU dreaming), FLEET_COORDINATION_SPEC implemented, crabjar WAYPOINT-SYSTEM.md core phases (1-2)

## §0 Problem Statement

Crabjar's waypoint system provides durable navigational objects that help workers orient themselves within a project. Currently, waypoints are local to each node's crabjar-state SQLite database. This creates three problems in a fleet environment:

1. **No resource awareness**: A worker can't ask "which nodes have GPUs available for this task?" through the waypoint system — that information lives in rIOd's resource registry
2. **No cross-node navigation**: If a worker needs to traverse to data or resources on another node, there's no routing mechanism
3. **No reachability hints**: Workers don't know which waypoints are actually reachable given current network/resource constraints

This spec defines how rIOd becomes the **routing and resource-awareness layer** for the waypoint system. Crabjar keeps waypoint storage and local traversal; rIOd adds fleet-wide capabilities.

## §1 Design Principles

1. **rIOd routes, crabjar stores.** Waypoint data (hierarchy, lineage, content) stays in crabjar-state on each node. rIOd maintains a *view* of what waypoints exist where for routing purposes, but never becomes the authoritative store.

2. **Resource-aware navigation.** When a worker asks "where can I find GPU X?" or "which nodes have this dataset?", rIOd answers based on its resource registry and fleet state. This turns waypoints from static references into dynamic, resource-aware navigational hints.

3. **Local-first with fleet extension.** All waypoint operations work locally without rIOd involvement. Fleet-wide routing is an opt-in enhancement that layers on top of local traversal (same pattern as FLEET_COORDINATION_SPEC).

4. **No cross-node ownership.** Each node's rIOd manages its local waypoints. The conductor maintains a fleet-wide *index* for routing but never holds waypoint content. If a node goes offline, its waypoints become unreachable — the system detects this and marks them stale.

5. **Reachability is advisory.** Riod tells crabjar which waypoints are *likely* reachable. Crabjar enforces actual access through its guard system. Reachability hints reduce wasted network calls but don't replace security checks.

## §2 Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    crabjar-conductor                     │
│                   (jambo:8091)                           │
│  ┌─────────────┐  ┌──────────────────┐                  │
│  │ Fleet State │  │ Waypoint Index   │  ← NEW           │
│  │ Registry    │  │ (routing only)   │                  │
│  └──────┬──────┘  └────────┬─────────┘                  │
└─────────┼──────────────────┼────────────────────────────┘
          │                  │
     HTTPS/JSON         HTTPS/JSON
          │                  │
    ┌─────▼─────┐      ┌─────▼─────┐       ┌────────────┐
    │  riod A   │      │  riod B   │       │  riod C    │
    │ (ftw3)    │      │ (lappy)   │       │ (sovnode)  │
    │ GPU: 1x3070│     │ GPU: 0    │       │ GPU: 0     │
    │           │      │           │       │            │
    │ ┌───────┐ │      │ ┌───────┐ │       │ ┌────────┐ │
    │ │Waypoint│ │      │ │Waypoint│ │       │ │ Waypoint│ │
    │ │Router │ │      │ │Router │ │       │ │ Router │ │
    │ └───┬───┘ │      │ └───┬───┘ │       │ └───┬────┘ │
    └─────┼─────┘      └─────┼─────┘       └─────┼──────┘
          │                  │                   │
     Unix socket         Unix socket          Unix socket
          │                  │                   │
    ┌─────▼─────┐      ┌─────▼─────┐       ┌─────▼─────┐
    │crabjar-state│    │crabjar-state│     │crabjar-state│
    │  (SQLite)  │     │  (SQLite)  │      │  (SQLite)  │
    └───────────┘      └───────────┘       └───────────┘
```

### 2.1 Roles

| Component | Responsibility |
|-----------|---------------|
| **crabjar-state (per node)** | Authoritative waypoint storage: hierarchy, lineage, content, traversal |
| **riod (per node)** | Local waypoint routing, resource-aware reachability hints, fleet indexing |
| **crabjar-conductor** | Fleet-wide waypoint index for cross-node routing decisions |
| **Crabjar workers** | Consume waypoints through crabjar's API; riod integration is transparent |

## §3 New Riod Module: `src/waypoints.rs`

### 3.1 Waypoint Router

```rust
/// Routes waypoint queries between local crabjar-state and the fleet.
pub struct WaypointRouter {
    /// Local crabjar-state socket for direct waypoint operations
    local_state_socket: String,
    
    /// Fleet client for cross-node routing (optional)
    fleet_client: Option<Arc<Mutex<FleetClient>>>,
    
    /// Cache of known waypoints on this node (for reachability hints)
    local_waypoint_index: WaypointIndex,
    
    /// Resource registry reference (for resource-aware queries)
    resource_registry: Arc<ResourceRegistry>,
}

impl WaypointRouter {
    pub fn new(
        local_state_socket: String,
        fleet_client: Option<Arc<Mutex<FleetClient>>>,
        resource_registry: Arc<ResourceRegistry>,
    ) -> Self;
    
    /// Resolve a waypoint reference. If local, query crabjar-state directly.
    /// If remote (node_id != local), route through conductor.
    pub async fn resolve(&self, ref: &WaypointRef) -> Result<WaypointResolution>;
    
    /// Find waypoints that match resource criteria.
    /// E.g., "find all GPU workstations with >8GB VRAM free"
    pub async fn find_by_resource(
        &self, 
        criteria: &ResourceCriteria
    ) -> Result<Vec<WaypointHit>>;
    
    /// Get reachability hints for a set of waypoints.
    /// Returns which are local, which are remote-reachable, which are stale.
    pub async fn check_reachability(
        &self, 
        refs: &[WaypointRef]
    ) -> Result<Vec<ReachabilityHint>>;
    
    /// Register a new waypoint with the fleet index (for cross-node discovery).
    pub async fn register_with_fleet(&self, wp: &WaypointSummary) -> Result<()>;
    
    /// Update local waypoint index from crabjar-state.
    pub async fn refresh_local_index(&mut self) -> Result<()>;
}
```

### 3.2 Data Models

```rust
/// Reference to a waypoint (local or remote)
pub struct WaypointRef {
    pub node_id: String,          // Which node hosts this waypoint
    pub waypoint_id: String,      // Local ID within that node's crabjar-state
    pub path: Vec<String>,        // Hierarchical path for human readability
}

/// Result of resolving a waypoint reference
pub struct WaypointResolution {
    pub ref: WaypointRef,
    pub reachable: bool,
    pub source: ResolutionSource,
    pub summary: Option<WaypointSummary>,  // If we can get it without full traversal
    pub resolution_cost: ResolutionCost,   // How expensive was this lookup?
}

pub enum ResolutionSource {
    LocalCache,                   // Found in local index (no network call)
    LocalState,                   // Queried local crabjar-state
    FleetIndex,                   // Routed through conductor's fleet index
    Unreachable { reason: String },// Node offline, waypoint deleted, etc.
}

pub struct ResourceCriteria {
    pub resource_type: ResourceType,  // GPU, RAM, dataset, model, etc.
    pub min_spec: Option<SpecFilter>, // Minimum specifications required
    pub state_filter: Option<ResourceState>,  // Only idle/available resources?
}

pub struct WaypointHit {
    pub ref: WaypointRef,
    pub match_score: f32,           // How well does this match the criteria?
    pub resource_summary: ResourceSummary,  // What resources are available here?
    pub reachability: ReachabilityHint,
}

pub struct ReachabilityHint {
    pub ref: WaypointRef,
    pub status: ReachStatus,
    pub last_verified: Option<Timestamp>,
    pub estimated_latency_ms: Option<u32>,  // If known from previous checks
}

pub enum ReachStatus {
    Local,                          // On this node
    RemoteReachable { node_id: String },  // On another reachable node
    Stale { last_seen: Timestamp },  // Known but not verified recently
    Unknown,                        // Never seen or can't verify
}
```

### 3.3 New Socket Methods

Add to riod's Unix socket JSON-RPC API:

```json
// Resolve a waypoint reference (local or fleet-wide)
{
  "method": "waypoint.resolve",
  "params": {
    "node_id": "ftw3",
    "waypoint_id": "gpu-workstation-alpha",
    "path": ["root", "compute", "gpu-nodes", "ftw3"]
  }
}

// Response
{
  "reachable": true,
  "source": "local-cache",
  "summary": {
    "name": "GPU Workstation Alpha",
    "type": "structural",
    "resources": [{"type": "gpu", "model": "RTX 3070 Ti", "vram_mb": 8192}],
    "last_verified": "2026-11-19T12:00:00Z"
  }
}

// Find waypoints matching resource criteria (fleet-wide)
{
  "method": "waypoint.find_by_resource",
  "params": {
    "resource_type": "gpu",
    "min_spec": {"vram_mb": 8192},
    "state_filter": "idle"
  }
}

// Response
{
  "hits": [
    {
      "node_id": "ftw3",
      "waypoint_id": "gpu-workstation-alpha",
      "match_score": 0.95,
      "resource_summary": {"type": "gpu", "model": "RTX 3070 Ti", "vram_mb": 8192},
      "reachability": "local"
    }
  ]
}

// Check reachability of a set of waypoints
{
  "method": "waypoint.check_reachability",
  "params": {
    "refs": [
      {"node_id": "ftw3", "waypoint_id": "gpu-alpha"},
      {"node_id": "lappy", "waypoint_id": "cpu-node-1"}
    ]
  }
}

// Response
{
  "results": [
    {"ref": {...}, "status": "local", "last_verified": "..."},
    {"ref": {...}, "status": "remote-reachable", "node_id": "lappy"}
  ]
}
```

## §4 Conductor API Extensions

### 4.1 Waypoint Index Endpoints

| Method | Path | Purpose |
|--------|------|---------|
| `POST` | `/api/waypoints/register` | Node registers its waypoints with the fleet index |
| `PUT` | `/api/waypoints/{node_id}/{wp_id}` | Update waypoint status/metadata |
| `DELETE` | `/api/waypoints/{node_id}/{wp_id}` | Remove waypoint from index (deleted or node offline) |
| `GET` | `/api/waypoints/search` | Search fleet-wide waypoints by criteria |
| `GET` | `/api/waypoints/{node_id}` | List all waypoints known on a specific node |

### 4.2 Waypoint Index Entry

```json
{
  "node_id": "ftw3",
  "waypoint_id": "gpu-workstation-alpha",
  "path": ["root", "compute", "gpu-nodes", "ftw3"],
  "name": "GPU Workstation Alpha",
  "type": "structural",
  "resources": [
    {"type": "gpu", "model": "RTX 3070 Ti", "vram_mb": 8192}
  ],
  "status": "active",  // active | stale | offline
  "last_verified": "2026-11-19T12:00:00Z",
  "registered_at": "2026-11-15T08:00:00Z"
}
```

### 4.3 Search Endpoint

```json
// GET /api/waypoints/search?resource_type=gpu&min_vram=8192&state=idle
{
  "results": [
    {
      "node_id": "ftw3",
      "waypoint_id": "gpu-workstation-alpha",
      "match_score": 0.95,
      "resource_summary": {...},
      "reachability": "online"
    }
  ],
  "total": 1
}
```

## §5 Integration with Crabjar Waypoint System

### 5.1 Transparent Routing

Crabjar workers interact with waypoints through crabjar's API. The riod integration is transparent:

```rust
// In crabjar, when resolving a waypoint:
pub async fn resolve_waypoint(&self, ref: &WaypointRef) -> Result<Waypoint> {
    // 1. Try local cache first
    if let Some(wp) = self.local_cache.get(&ref) {
        return Ok(wp);
    }
    
    // 2. If remote node, ask riod to route
    if ref.node_id != self.local_node_id {
        let resolution = self.riod_client.resolve(ref).await?;
        if !resolution.reachable {
            return Err(WaypointError::Unreachable(resolution.reason));
        }
        // Route through conductor or directly to target node's riod
        return self.fetch_remote_waypoint(&ref, &resolution).await;
    }
    
    // 3. Local resolution from crabjar-state
    self.local_state.resolve(ref).await
}
```

### 5.2 Resource-Aware Waypoint Creation

When creating a waypoint that references resources (GPUs, datasets, etc.), crabjar can query riod to get accurate resource metadata:

```rust
// In crabjar, when creating a structural waypoint for a GPU node:
pub async fn create_resource_waypoint(&self, node_id: &str) -> Result<Waypoint> {
    // Get current resource state from riod
    let resources = self.riod_client.get_resources(node_id).await?;
    
    // Create waypoint with accurate resource metadata
    Waypoint {
        name: format!("GPU Workstation {}", node_id),
        type: WaypointType::Structural,
        resources: resources.into_iter().map(ResourceRef::from).collect(),
        parent: Some(self.get_compute_root()),
        ..default()
    }
}
```

### 5.3 Reachability Hints in Traversal

When a worker traverses the waypoint hierarchy, crabjar can ask riod for reachability hints to avoid suggesting unreachable paths:

```rust
// During traversal, before presenting child waypoints:
let children = self.get_children(parent_id).await?;
let refs: Vec<WaypointRef> = children.iter().map(|c| c.to_ref()).collect();
let hints = self.riod_client.check_reachability(&refs).await?;

// Annotate children with reachability status
for (child, hint) in children.into_iter().zip(hints) {
    child.reachability = Some(hint.status);
}
```

## §6 Phased Implementation

### Phase 4.1a: Local Waypoint Indexing
- Add `src/waypoints.rs` module to riod
- Implement local waypoint index refresh from crabjar-state
- Add `waypoint.resolve` and `waypoint.check_reachability` socket methods (local only)
- No conductor integration yet

### Phase 4.1b: Fleet-Waypoint Indexing
- Extend FLEET_COORDINATION_SPEC registration to include waypoint summaries
- Add conductor endpoints for waypoint index
- Implement cross-node resolution through conductor
- Add `waypoint.find_by_resource` (fleet-wide search)

### Phase 4.1c: Resource-Aware Navigation
- Integrate with crabjar's waypoint creation (resource metadata from riod)
- Reachability hints during traversal
- Stale detection and automatic re-verification

## §7 Constraints & Policies

### No Content in the Index
The fleet index stores only *summaries* (name, type, resource refs, status). Full waypoint content stays on the owning node. This keeps the conductor lightweight and avoids duplicating large amounts of data.

### Stale Detection
Waypoints that haven't been verified within a configurable window (default: 24 hours) are marked `stale`. Workers can still traverse to stale waypoints but receive a warning. The system attempts re-verification on access.

### Local Operations Never Block on Fleet
If the conductor is unreachable, local waypoint operations continue to work normally. Fleet-wide features degrade gracefully — workers just can't see remote waypoints until connectivity is restored.

### Security
Waypoint resolution goes through crabjar's guard system for authorization. Riod provides routing and reachability hints but doesn't enforce access control — that's crabjar-guard's job (consistent with the POSITIVE-BOUNDARY-MODEL spec).

## §8 Success Criteria

1. Workers can discover resources across the fleet through waypoint navigation
2. Resource-aware queries ("find me a GPU node") return accurate results from riod's registry
3. Cross-node waypoint traversal works when nodes are online, degrades gracefully when offline
4. Local waypoint operations have zero dependency on conductor availability
5. Reachability hints reduce failed network calls by >80% compared to blind traversal

## §9 Open Questions

- How frequently should the fleet index be refreshed? (On change vs. periodic)
- Should waypoints carry version numbers for conflict detection when multiple nodes reference the same resource?
- Can we use riod's existing trust scoring to weight reachability hints? (Trusted nodes get higher priority)
- What's the right granularity for resource metadata in waypoint summaries? (Full spec vs. key attributes only)
