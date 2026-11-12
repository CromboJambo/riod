# rIOd — System Resource Supervisor

Per-machine daemon that observes resources, authorizes by purpose, and allocates GPU/CPU/RAM to clients. Coordinates but never owns process lifecycle (dinit starts/stops/restarts processes; rIOd manages what they're allowed to use).

## What it does

- **Observes** CPU, RAM, and GPU utilization via `/proc` and `nvidia-smi`
- **Detects idle** state with configurable thresholds and sample windows
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
│  ┌──────────┐  ┌──────────┐         │          │
│  │ Ledger   │← │ Socket   │←────────┘          │
│  │ (JSONL)  │  │ Server   │                     │
│  └──────────┘  └──────────┘                     │
│                                                  │
│  Clients: crabjar, PESTI, herdr, any Rust app    │
│  via Unix socket JSON-RPC or riod-client crate   │
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
    --poll-interval-s 30
```

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

| Method | Description |
|--------|-------------|
| `system.status` | Full system status: CPU, RAM, GPUs, idle state, uptime |
| `resources.snapshot` | Current resource readings only |
| `idle.state` | Idle detection state machine output |
| `ledger.tail` | Last N ledger events |
| `ledger.query` | Filtered ledger query (since/until/limit) |
| `resources.allocate` | Request GPU/RAM allocation with TTL |
| `resources.release` | Release an allocation |
| `resources.heartbeat` | Extend allocation TTL |
| `resources.list` | List active allocations |
| `resources.available` | Check available resources without allocating |
| `resources.add_process` | Add a client PID to the allocation's cgroup |

All responses include a `doubt` block: `assumptions`, `blind_spots`, `last_validation`, `stale_after`.

## Deployment

### dinit service

```bash
sudo cp riod.service /etc/dinit.d/riod
sudo mkdir -p /run/riod /var/log/riod
sudo dinitctl start riod
```

See [`riod.service`](riod.service) for the full unit file.

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

riod-client/   Client library crate
  src/client.rs    Typed sync + async client API
  src/transport.rs Unix socket JSON-RPC transport
  src/types.rs     Wire types matching daemon envelopes
  examples/        Usage examples
```

## Phases

| Phase | Scope | Status |
|-------|-------|--------|
| 0 | Observer: CPU/RAM/GPU polling, idle detection, ledger, socket API | ✅ Complete |
| 1 | Broker: allocation requests, GPU slot management, cgroup enforcement, heartbeats | ✅ Complete |
| — | Client library: typed sync/async Rust client crate | ✅ Complete |
| 2 | Policy: purpose-based authorization, risk scoring integration | Planned |
| 3 | Dreaming: background cognition on idle compute | Planned |

## Design Principles

1. **Coordinates, doesn't own lifecycle.** Dinit starts/stops/restarts processes. rIOd allocates resources and authorizes purpose only.
2. **API-first.** The JSON-RPC socket API is the contract. Breaking changes require versioning.
3. **Append-only ledger.** All events are immutable once written. Reconciliation over mutation.
4. **Per-machine authority.** Each machine runs its own rIOd. Fleet coordination is a separate concern.

## Related Projects

- [crabjar](https://github.com/CromboJambo/crabjar) — Agent orchestration framework (rIOd's primary client)
- PESTI — Portable execution substrate for transformer inference
- herdr — Terminal multiplexer for agent-driven development
