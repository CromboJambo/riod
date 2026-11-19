# rIOd Roadmap

Spec-driven development plan. Each phase has acceptance criteria that must be met before moving to the next. Phases are ordered by dependency — later phases build on earlier ones.

## Completed

### Phase 0: Observer ✅ (v0.1.0)

**Goal**: Observe system resources and report state without making decisions.

| Spec | Description | Status |
|------|-------------|--------|
| 0.1 | CPU/RAM/GPU polling via `/proc` and `nvidia-smi` at configurable intervals | ✅ |
| 0.2 | Idle detection state machine with configurable thresholds and sample windows | ✅ |
| 0.3 | Append-only JSONL event ledger with query/rehydration | ✅ |
| 0.4 | Unix socket JSON-RPC 2.0 API (`system.status`, `resources.snapshot`, `idle.state`, `ledger.*`) | ✅ |

**Acceptance**: Daemon reports accurate resource state within poll interval. All events are ledgered and queryable. Socket API is versioned and documented.

### Phase 1: Broker ✅ (v0.1.0)

**Goal**: Allocate resources to clients without owning their lifecycle.

| Spec | Description | Status |
|------|-------------|--------|
| 1.1 | GPU slot allocation with TTL and heartbeat extension | ✅ |
| 1.2 | RAM/CPU percentage allocation (advisory) | ✅ |
| 1.3 | cgroup v2 memory/CPU limits for allocated processes | ✅ |
| 1.4 | Stale allocation reclamation via TTL expiry | ✅ |
| 1.5 | `riod-client` crate: typed sync + async client library | ✅ |

**Acceptance**: Client can request, use, heartbeat, and release GPU/RAM allocations. Expired allocations are automatically reclaimed. cgroup limits are enforced (advisory mode).

### Phase 2: Policy & Authorization ✅ (v0.1.1)

**Goal**: Authorize resource allocation by purpose, not just availability.

| Spec | Description | Status |
|------|-------------|--------|
| 2.1 | Declarative TOML policy engine with conditions (idle duration, GPU utilization, VRAM free, time of day) | ✅ |
| 2.2 | Risk scoring: 4-factor assessment (resource intensity 40pts, duration 20pts, purpose weight 20pts, system load 20pts) → 0–100 score with Low/Medium/High/Critical levels | ✅ |
| 2.3 | Intent lifecycle: `intent.declare` → Granted/Denied → Consumed on allocation (one-shot) | ✅ |
| 2.4 | Dry-run risk assessment via `resources.risk_assess` | ✅ |
| 2.5 | Policy hot-reload without daemon restart | ✅ |
| 2.6 | Guard integration: trust-aware approval for High/Critical risk from untrusted clients (`crabjar-guard`) | ✅ |

**Acceptance**: Clients must declare intent before allocating (when policies loaded). Risk scores are computed and logged. Untrusted clients with High/Critical risk are queued for human approval via `guard.approvals_*` API. Trusted clients auto-approve.

### Phase 3: Dream Scheduler 🔄 (v0.1.1, in progress)

**Goal**: Trigger background cognition on idle compute without interfering with foreground work.

| Spec | Description | Status |
|------|-------------|--------|
| 3.1 | Idle-triggered state machine: Idle → Armed → Triggered → Idle | ✅ |
| 3.2 | Configurable thresholds: idle duration, max cycle duration, cooldown | ✅ |
| 3.3 | Preemption on activity: dream cycles yield to foreground work | ✅ |
| 3.4 | Dream task framework: registry of executable tasks with reporting API | ✅ |
| 3.5 | GPU dreaming: allocate idle GPUs for background inference/training during dream cycles (`dream.allocate_gpu` + enhanced `dream.status`) | ✅ |
| 3.6 | Fleet coordination: multi-machine dream scheduling via crabjar-conductor | ⬜ |

**Acceptance**: Dream scheduler triggers after sustained idle, preempts on activity, and emits ledger events that downstream consumers (crabjar) can act on. GPU dreaming allocates idle GPUs for background work without starving foreground allocations.

## Planned

### Phase 4: Fleet Coordination ⬜

**Goal**: Coordinate resource allocation across multiple machines in a fleet.

| Spec | Description | Status |
|------|-------------|--------|
| 4.1 | Fleet registry: discover and track peer rIOd instances via Tailscale | ⬜ |
| 4.2 | Cross-machine allocation: request resources from any machine in the fleet | ⬜ |
| 4.3 | Load balancing: route allocations to least-loaded machines | ⬜ |
| 4.4 | Fleet-wide policy enforcement: consistent authorization across machines | ⬜ |

**Acceptance**: A client can request resources from the fleet and receive them from the most appropriate machine. Policies are enforced consistently regardless of which machine handles the allocation.

### Phase 5: Self-Optimization ⬜

**Goal**: rIOd learns from its own ledger to improve allocation decisions over time.

| Spec | Description | Status |
|------|-------------|--------|
| 5.1 | Ledger analysis: mine historical allocations for patterns (peak usage times, common purposes, typical durations) | ⬜ |
| 5.2 | Adaptive thresholds: adjust idle detection and dream trigger thresholds based on observed usage patterns | ⬜ |
| 5.3 | Predictive preemption: anticipate allocation needs based on historical patterns | ⬜ |
| 5.4 | Policy suggestions: recommend policy changes based on denial/approval patterns | ⬜ |

**Acceptance**: rIOd improves its allocation decisions over time without manual tuning. Suggestions are logged and can be applied via `policy.reload`.

## Design Constraints (All Phases)

1. **Coordinates, doesn't own lifecycle.** Dinit starts/stops/restarts processes. rIOd allocates resources and authorizes purpose only.
2. **API-first.** The JSON-RPC socket API is the contract. Breaking changes require versioning.
3. **Append-only ledger.** All events are immutable once written. Reconciliation over mutation.
4. **Per-machine authority.** Each machine runs its own rIOd. Fleet coordination (Phase 4) is additive, not replacing local authority.
5. **No LLM inference.** That's pesti/llm-workspace. rIOd allocates the GPUs they run on.
6. **Rust-first.** No Python in the daemon path. Client library is Rust (`riod-client`).

## Versioning Policy

- **0.x.y**: Pre-1.0 — breaking changes allowed in minor versions, documented in CHANGELOG.
- **API contract**: Versioned via `contract.rs`. Daemon validates API version at startup and rejects incompatible clients.
- **Ledger format**: Append-only. New event types are additive. Existing events are never modified or deleted.
