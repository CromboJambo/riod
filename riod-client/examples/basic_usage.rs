//! Example: Basic resource allocation workflow with riod-client.
//!
//! Demonstrates connecting to a riod daemon, checking availability,
//! requesting and releasing GPU allocations.

use riod_client::{AllocateRequest, RiodClient};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to the riod daemon (default socket: /run/riod.sock)
    let client = RiodClient::default_connect()?;

    println!("Connected to riod at {}", client.socket_path().display());

    // Check system status
    let status = client.system_status()?;
    println!("\n=== System Status ===");
    println!("CPU: {:.1}%", status.cpu_usage_percent);
    println!("RAM: {} / {} MB used", status.ram_used_mb, status.ram_total_mb);
    for gpu in &status.gpus {
        println!(
            "GPU {}: {} ({}% util, {}MB VRAM used)",
            gpu.index, gpu.name, gpu.utilization_percent, gpu.vram_used_mb
        );
    }

    // Check what's available for allocation
    let avail = client.available_resources()?;
    println!("\n=== Available Resources ===");
    println!("GPUs available: {:?}", avail.available_gpus);
    println!(
        "RAM available: {} MB ({} active allocations)",
        avail.available_ram_mb, avail.active_allocations
    );

    if avail.available_gpus.is_empty() {
        println!("\nNo GPUs available. Exiting.");
        return Ok(());
    }

    // Request a GPU allocation for inference using the builder API
    println!("\n=== Requesting Allocation ===");
    let req = AllocateRequest::new("example-app", "inference")
        .with_ram_mb(4096)      // up to 4GB RAM
        .with_cpu_percent(50.0) // up to 50% CPU
        .with_ttl(3600);        // 1 hour TTL

    let alloc = client.allocate(&req)?;

    println!(
        "Granted: {} on GPU(s) {:?}, {}MB RAM, expires {}",
        alloc.id, alloc.gpus, alloc.ram_mb, alloc.expires_at
    );

    if let Some(cg_path) = &alloc.cgroup_path {
        println!("Cgroup path: {}", cg_path);
        // Register this process with the cgroup for enforcement
        client.add_process(&alloc.id, "example-app", std::process::id())?;
        println!("Process {} registered with cgroup", std::process::id());
    }

    // Simulate some work...
    println!("\nDoing work on GPU(s) {:?}...", alloc.gpus);
    std::thread::sleep(std::time::Duration::from_secs(2));

    // Send a heartbeat to extend the TTL
    let new_expiry = client.heartbeat(&alloc.id, "example-app")?;
    println!("Heartbeat sent. New expiry: {}", new_expiry);

    // Release when done
    let release = client.release(&alloc.id, "example-app")?;
    println!("\nReleased {}. Freed GPUs: {:?}", release.released, release.gpus_freed);

    Ok(())
}
