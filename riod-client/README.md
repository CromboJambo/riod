# riod-client

Rust client library for the rIOd resource supervisor daemon.

## Features

- Typed synchronous API over Unix socket JSON-RPC
- Async wrapper via `tokio::task::spawn_blocking`
- Full allocation lifecycle: allocate → add_process → heartbeat → release
- System status, available resources, ledger tail queries
- Zero-copy wire types matching the daemon's response envelopes

## Quick Start

```rust
use riod_client::{RiodClient, AllocateRequest};

fn main() -> Result<(), Box<dyn std::error::Error>> {
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

    // Track a process in the allocation
    client.add_process(&alloc.id, "my-app", std::process::id())?;

    // Release when done
    client.release(&alloc.id, "my-app")?;

    Ok(())
}
```

## Async Usage

```rust
use riod_client::{RiodClientAsync, AllocateRequest};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = RiodClientAsync::default_connect()?;

    let req = AllocateRequest::new("worker", "training").with_gpu_count(1);
    let alloc = client.allocate(req).await?;

    // ... do work ...

    client.release(alloc.id, "worker".to_string()).await?;
    Ok(())
}
```

## Configuration

- Default socket path: `/run/riod.sock`
- Custom socket: `RiodClient::connect("/path/to/socket")`

## Development

```bash
cargo test -p riod-client          # Run tests (uses mock daemon)
cargo run --example basic_usage    # Basic usage example
cargo run --example live_smoke_test  # Live smoke test against running daemon
```
