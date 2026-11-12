# rIOd — System Resource Supervisor

Per-machine daemon that observes resources, authorizes by purpose, and allocates GPU/CPU/RAM to clients. Coordinates but never owns process lifecycle (dinit starts/stops/restarts processes; rIOd manages what they're allowed to use).

## What it does

- **Observes** CPU, RAM, and GPU utilization via `/proc` and `nvidia-smi`
- **Detects idle** state with configurable thresholds and sample windows
- **Authorizes** clients by purpose via declarative TOML policies (Phase 2)
- **Allocates** resources to clients via Unix socket JSON-RPC API
- **Enforces** cgroup v2 memory/CPU limits (advisory mode)
- **Ledgers** all events in an append-only JSONL log
- **Reclaims** stale allocations via TTL-based heartbeats

## Architecture

```
┌─────────────────────────────────────────────────┐
│                  rIOd Daemon                     │
│                                                  │
│  ┌──────────┐  ┌──────────┐  ┌───────────────┐  │
│  │ Observer │→ │ Idle     │→ │ Resource      │  │
│  │ (poll)   │  │ Detector │  │ Broker        │  │
│  └──────────┘  └──────────┘  └───────┬───────┘  │
│                                      │          │
│  ┌──────────┐  ┌──────────┐   ┌─────▼─────┐    │
│  │ Ledger   │← │ Socket   │←──│ Policy    │    │
│  │ (JSONL)  │  │ Server   │   │ Engine    │    │
│  └──────────┘  └──────────┘   └───────────┘    │
│                                                 │
│  Clients: crabjar, PESTI, herdr, any Rust app   │
│  via Unix socket JSON-RPC or riod-client crate  │
└─────────────────────────────────────────────────┘
```

## Quick Start

### Build

```bash
cargo build --release
```

### Run the daemon

```bash
./target/release/riod daemon \
    --socket-path /run/riod.sock \
    --ledger-path /var/log/riod/events.jsonl \
    --poll-interval-s 30 \
    --policy-file /etc/riod/policies.toml \
    --socket-perm 777
```

Key flags:
- `--policy-file` — TOML policy file for Phase 2 authorization (optional; without it, all intents are denied)
- `--socket-perm` — Unix socket permissions in octal (default `777`; restrict on multi-user systems)

### Query system status

```bash
echo '{"jsonrpc":"2.0","method":"system.status","params":{},"id":1}' \
    | socat - UNIX-CONNECT:/run/riod.sock
```

### Use the client library

```rust
use riod_client::{RiodClient, AllocateRequest};

let client = RiodClient::default_connect()?;

// Check what's available
let avail = client.available_resources()?;
println!("Free GPUs: {:?}", avail.available_gpus);

// Allocate resources
let req = AllocateRequest::new("my-app", "inference")
    .with_gpu_count(1)
    .with_ram_mb(4096)
    .with_ttl(3600);
let alloc = client.allocate(&req)?;
println!("Got allocation: {}", alloc.id);

// Release when done
client.release(&alloc.id, "my-app")?;
```

See [`riod-client/README.md`](riod-client/README.md) for full API docs.

## JSON-RPC API

### System & Observation

| Method | Description |
|--------|-------------|
| `system.status` | Full system status: CPU, RAM, GPUs, idle state, uptime |
| `resources.snapshot` | Current resource readings only |
| `idle.state` | Idle detection state machine output |
| `ledger.tail` | Last N ledger events |
| `ledger.query` | Filtered ledger query (since/until/limit) |

### Resource Allocation

| Method | Description |
|--------|-------------|
| `resources.allocate` | Request GPU/RAM allocation with TTL. Accepts optional `intent_id` for Phase 2 enforcement. |
| `resources.release` | Release an allocation |
| `resources.heartbeat` | Extend allocation TTL |
| `resources.list` | List active allocations |
| `resources.available` | Check available resources without allocating |
| `resources.add_process` | Add a client PID to the allocation's cgroup |

### Phase 2: Policy & Authorization

| Method | Description |
|--------|-------------|
| `intent.declare` | Declare purpose before allocating. Returns granted/denied intent with ID. |
| `intent.status` | Query status of a declared intent (granted/consumed/denied). |
| `policy.list` | List loaded policies and their conditions. |
| `policy.reload` | Hot-reload policies from file without restart. |

All responses include a `doubt` block: `assumptions`, `blind_spots`, `last_validation`, `stale_after`.

## Phase 2: Purpose-Based Authorization

Clients declare an **intent** before allocating resources. The policy engine evaluates the intent against declarative TOML policies and current system state (CPU/RAM/GPU utilization, idle duration, time of day), then grants or denies it.

### Flow

```
1. intent.declare {client_id, purpose, ...}  →  {id: "int-...", status: "granted"}
2. resources.allocate {..., intent_id: "int-..."}  →  allocation (intent consumed)
```

### Enforcement rules

- **With `intent_id`**: verified for existence, Granted status, client ownership, and expiry (300s TTL). Marked Consumed on successful allocation (one-shot).
- **Without `intent_id`**: allowed (backward compatible), but logged as a warning when policies are loaded.
- **No policy file**: all intents denied (safe default).

### Example policy file

```toml
[policy.inference-anytime]
purpose = "inference"
conditions = []
max_duration_s = 3600

[policy.training-during-idle]
purpose = "training"
conditions = [
    { type = "idle_duration", min_secs = 600 },
    { type = "gpu_utilization", max_percent = 20 },
    { type = "vram_free", min_percent = 50 },
]
max_duration_s = 86400
```

See [`examples/policies.toml`](examples/policies.toml) for a complete example with comments.

## Deployment

### dinit service (Artix Linux)

```bash
sudo cp riod.service /etc/dinit.d/riod
sudo mkdir -p /run/riod /var/log/riod
sudo dinitctl start riod
```

See [`riod.service`](riod.service) for the full unit file.

### systemd (Debian/Ubuntu/Fedora/etc.)

```ini
# /etc/systemd/system/riod.service
[Unit]
Description=rIOd System Resource Supervisor
After=network.target

[Service]
Type=simple
ExecStart=/usr/local/bin/riod daemon --socket-path /run/riod.sock --ledger-path /var/log/riod/events.jsonl --poll-interval-s 30
Restart=on-failure
RestartSec=5
User=root

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl enable riod
sudo systemctl start riod
```

### Policy file (optional)

Place a TOML policy file at `/etc/riod/policies.toml` and add `--policy-file /etc/riod/policies.toml` to the daemon command line. Without it, all intent declarations are denied (safe default).

Default paths:
- Socket: `/run/riod.sock`
- Ledger: `/var/log/riod/events.jsonl`
- Policy: `/etc/riod/policies.toml` (optional)
- Poll interval: 30s

## Development

```bash
cargo check          # fast type check
cargo test           # all tests (daemon + client library)
cargo clippy         # linting
cargo build --release # optimized binary
```

### Project structure

```
src/           Daemon implementation
  main.rs      CLI entry point, daemon loop
  observe.rs   CPU/RAM/GPU polling, idle detection
  broker.rs    Resource allocation engine
  cgroup.rs    cgroup v2 integration
  ledger.rs    Append-only JSONL event ledger
  socket.rs    Unix socket JSON-RPC server
  state.rs     Shared daemon state
  api_broker.rs Broker API handlers
  policy.rs    Phase 2 policy engine (intent-based authorization)

riod-client/   Client library crate
  src/client.rs    Typed sync + async client API
  src/transport.rs Unix socket JSON-RPC transport
  src/types.rs     Wire types matching daemon envelopes
  examples/        Usage examples

examples/      Sample configuration files
  policies.toml    Phase 2 policy file example
```

## Phases

| Phase | Scope | Status |
|-------|-------|--------|
| 0 | Observer: CPU/RAM/GPU polling, idle detection, ledger, socket API | ✅ Complete |
| 1 | Broker: allocation requests, GPU slot management, cgroup enforcement, heartbeats | ✅ Complete |
| — | Client library: typed sync/async Rust client crate | ✅ Complete |
| 2 | Policy: purpose-based authorization, intent → allocation enforcement | ✅ Complete |
| — | Risk scoring integration with crabjar-guard | Planned |
| 3 | Dreaming: background cognition on idle compute | Planned |

## Design Principles

1. **Coordinates, doesn't own lifecycle.** Dinit starts/stops/restarts processes. rIOd allocates resources and authorizes purpose only.
2. **API-first.** The JSON-RPC socket API is the contract. Breaking changes require versioning.
3. **Append-only ledger.** All events are immutable once written. Reconciliation over mutation.
4. **Per-machine authority.** Each machine runs its own rIOd. Fleet coordination is a separate concern.

## License

AGPL-3.0-or-later — see [LICENSE](LICENSE).

This is a network daemon with a client library. The AGPL copyleft applies to modifications of rIOd itself and any software that links against or communicates with the `riod-client` crate in a way that creates a combined work. Standalone clients that communicate over the JSON-RPC socket API are separate works (the "corresponding source" obligation is triggered by modification, not by network interaction alone — see AGPL Section 13).

## Related Projects

- [crabjar](https://github.com/CromboJambo/crabjar) — Agent orchestration framework (rIOd's primary client)
- PESTI — Portable execution substrate for transformer inference

