# Changelog

All notable changes to rIOd are documented here. The format is based on [Keep a Changelog](https://keepachangelog.com/).

## [0.1.1] — 2026-11-19

### Added

- **Guard integration** (`crabjar-guard`): Trust-aware allocation approval for High/Critical risk intents from untrusted clients. New API methods: `guard.approvals_list`, `guard.approvals_approve`, `guard.approvals_reject`. Trusted clients (via PID-based trust resolution) auto-approve even high-risk allocations.
- **Dormant allocation preemption**: Utilization-aware activity tracking reclaims idle GPU slots from stale allocations before TTL expiry.
- **Versioned API contract**: Startup validation ensures daemon and client agree on API version. Breaking changes now require explicit version bumps.
- **Ledger projection**: `ResourceState` can be derived from ledger events for audit/reconciliation without trusting in-memory state.
- **Rust service wrapper**: Replaced `riod-run.py` with a typed-API Rust binary (`riod-run`) using `riod-client`.
- **`riod init` command**: Generates default `/etc/riod/policies.toml` and service profiles. Idempotent — safe to re-run.
- **Utilization-aware availability**: `resources.available` now accounts for actual GPU/RAM utilization, not just allocation count.

### Changed

- **License**: Apache-2.0 → AGPL-3.0-or-later (network daemon with client library; see README for scope).
- **GPU polling resilience**: GPU query failures no longer kill the resource observation loop.

### Fixed

- Intent enforcement: one-shot consumption on successful allocation (previously could be reused).
- Rustfmt applied across remaining workspace files.

## [0.1.0] — 2026-11-14

Initial release covering Phase 0 and Phase 1:

- **Phase 0 (Observer)**: CPU/RAM/GPU polling via `/proc` and `nvidia-smi`, idle detection state machine, append-only JSONL event ledger, Unix socket JSON-RPC 2.0 API (`system.status`, `resources.snapshot`, `idle.state`, `ledger.tail`, `ledger.query`).
- **Phase 1 (Broker)**: GPU slot allocation with TTL, RAM/CPU percentage allocation, cgroup v2 memory/CPU limits (advisory mode), heartbeat-based TTL extension, stale allocation reclamation.
- **Client library** (`riod-client`): Typed sync + async Rust client for the JSON-RPC API.
- **Deployment**: dinit service file (Artix Linux) and systemd unit example.

### Added in 0.1.0 post-release commits

- Configurable Unix socket permissions (`--socket-perm`).
- Purpose-based authorization policy engine (Phase 2) with declarative TOML policies.
- Risk scoring engine: 4-factor assessment (resource intensity, duration, purpose weight, system load) producing 0–100 scores with Low/Medium/High/Critical levels.
- Intent → allocation enforcement: `intent.declare` before `resources.allocate`, one-shot consumption on success.
- Dream scheduler (Phase 3): idle-triggered state machine (Idle → Armed → Triggered), preemption on activity, dream task framework with registry and reporting API.
