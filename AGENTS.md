# rIOd — System Resource Supervisor
# Per-machine daemon: observes resources, authorizes by purpose, allocates GPU/CPU/RAM.
# Coordinates but never owns lifecycle (dinit starts/stops/restarts processes).

## Project Scope

**In scope:**
- Resource observation (CPU, RAM, GPU via /proc and nvidia-smi)
- Idle detection with configurable thresholds
- Append-only event ledger (JSONL)
- Unix socket JSON-RPC 2.0 API
- Resource allocation/brokerage (Phase 1+)
- Purpose-based authorization (Phase 2+)
- Background "dream" cognition on idle (Phase 3+)

**Not in scope:**
- LLM inference (that's pesti/llm-workspace)
- Process lifecycle management (that's dinit)
- Task routing between workers (that's crabjar-conductor)
- Action-level authorization for commands (that's crabjar-guard)

## Build & Test

```bash
cargo check          # fast type check
cargo test           # unit + integration tests (daemon + client library)
cargo build          # debug binary
cargo build --release # optimized binary
```

No `just` file yet — add one when the project grows beyond a single crate.

## Architecture

```
main.rs        CLI entry point (clap), daemon loop, graceful shutdown
lib.rs         Module declarations
observe.rs     CPU/RAM/GPU polling, idle detection state machine
broker.rs      Resource allocation engine (GPU slots, RAM, TTL)
cgroup.rs      cgroup v2 integration (memory/CPU limits, advisory mode)
ledger.rs      Append-only JSONL event ledger with query/rehydration
socket.rs      Unix socket JSON-RPC 2.0 server
state.rs       Shared daemon state (Arc<Mutex>)
api_broker.rs  Broker API handlers (allocate/release/heartbeat/list)

riod-client/   Client library crate
client.rs      Typed sync + async client API
transport.rs   Unix socket JSON-RPC transport
types.rs       Wire types matching daemon response envelopes
```

### Design Principles

1. **Coordinates, doesn't own lifecycle.** Dinit starts/stops/restarts processes. rIOd allocates resources and authorizes purpose only.
2. **API-first.** The JSON-RPC socket API is the contract. Treat breaking changes carefully.
3. **Append-only ledger.** All events are immutable once written. Reconciliation over mutation.
4. **Per-machine authority.** Each machine runs its own rIOd. Fleet coordination is a separate concern (Phase 3+).

### The Glass (Abstraction Boundary)

- **Inside:** Resource observation, allocation state, policy decisions. Stable substrate.
- **Outside:** Concrete integrations (nvidia-smi, cgroups, specific clients). Disposable adapters.
- Clients (crabjar, PESTI, herdr) talk to rIOd via the Unix socket API. They never touch the ledger or observation internals directly.

## JSON-RPC API

| Method | Params | Returns | Description |
|--------|--------|---------|-------------|
| `system.status` | — | snapshot + idle state + uptime | Full system status |
| `resources.snapshot` | — | ResourceSnapshot | Current resource readings |
| `idle.state` | — | IdleState | Idle detection state |
| `ledger.tail` | `{n: usize}` | Recent events | Last N ledger entries |
| `ledger.query` | `{since, until, limit}` | Events | Filtered ledger query |
| `resources.allocate` | AllocationRequest | Allocation | Request GPU/RAM with TTL |
| `resources.release` | `{id, client_id}` | message | Release an allocation |
| `resources.heartbeat` | `{id, client_id}` | new expiry | Extend allocation TTL |
| `resources.list` | — | allocations[] | List active allocations |
| `resources.available` | — | available resources | Check without allocating |
| `resources.add_process` | `{id, client_id, pid}` | message | Add PID to cgroup |

All responses include a `doubt` block: `assumptions`, `blind_spots`, `last_validation`, `stale_after`.

## Deployment

### dinit service

```bash
# Copy to /etc/dinit.d/riod on target machine
sudo cp riod.service /etc/dinit.d/riod
sudo dinitctl start riod
```

Default paths:
- Socket: `/run/riod.sock`
- Ledger: `/var/log/riod/events.jsonl`
- Poll interval: 30s

### Configuration

Currently via CLI flags only. Phase 1 will add a TOML config file at `/etc/riod/riod.toml`.

## Testing

Unit tests live inline with their modules under `#[cfg(test)]`. Use `tempfile::tempdir()` for filesystem fixtures — never write into the repository.

Test names: descriptive snake_case stating behaviour (e.g., `test_idle_detector_state_transitions`).

## Phases

| Phase | Scope | Status |
|-------|-------|--------|
| 0 | Observer: CPU/RAM/GPU polling, idle detection, ledger, socket API | ✅ Complete |
| 1 | Broker: allocation requests, GPU slot management, cgroup enforcement, heartbeats | ✅ Complete |
| — | Client library: typed sync/async Rust client crate (`riod-client`) | ✅ Complete |
| 2 | Policy: purpose-based authorization, risk scoring integration with crabjar-guard | Planned |
| 3 | Dreaming: background cognition on idle, dream ledger, promotion workflow | Planned |

## Naming

- Daemon binary: `riod`
- Socket API methods: `snake_case` (e.g., `system.status`, `resources.allocate`)
- Event types: `snake_case` in JSON (e.g., `resource_snapshot`, `allocation_granted`)
- Rust types: `PascalCase` (e.g., `ResourceSnapshot`, `AllocationRequest`)
