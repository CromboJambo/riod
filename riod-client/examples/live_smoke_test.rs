//! Live smoke test against a real riod daemon.
use riod_client::{AllocateRequest, RiodClient};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let socket = "/tmp/riod-client-smoke.sock";
    let client = RiodClient::connect(socket)?;
    println!("Connected to riod at {}", socket);

    // 1. System status
    let status = client.system_status()?;
    println!("\n=== System Status ===");
    println!("CPU: {:.1}%", status.cpu_usage_percent);
    println!("RAM: {}/{} MB", status.ram_used_mb, status.ram_total_mb);
    for gpu in &status.gpus {
        println!(
            "GPU {}: {} ({}MB VRAM, {:.0}% util)",
            gpu.index, gpu.name, gpu.vram_used_mb, gpu.utilization_percent
        );
    }

    // 2. Available resources
    let avail = client.available_resources()?;
    println!("\n=== Available Resources ===");
    println!("Free GPUs: {:?}", avail.available_gpus);
    println!("Free RAM: {} MB", avail.available_ram_mb);

    if avail.available_gpus.is_empty() {
        println!("\nNo GPUs available — skipping allocation test");
        return Ok(());
    }

    // 3. Allocate using the builder API
    let req = AllocateRequest::new("client-smoke-test", "smoke-test")
        .with_gpus(vec![avail.available_gpus[0]])
        .with_ram_mb(4096)
        .with_ttl(120);

    let alloc = client.allocate(&req)?;
    println!("\n=== Allocation Granted ===");
    println!("ID: {}", alloc.id);
    println!("GPUs: {:?}", alloc.gpus);
    println!("RAM: {} MB", alloc.ram_mb);
    if let Some(cg) = &alloc.cgroup_path {
        println!("Cgroup: {}", cg);
    }

    // 4. Heartbeat
    let new_expiry = client.heartbeat(&alloc.id, "client-smoke-test")?;
    println!("\nHeartbeat sent — new expiry: {}", new_expiry);

    // 5. List allocations
    let allocs = client.list_allocations()?;
    println!("\n=== Active Allocations ({}) ===", allocs.len());
    for a in &allocs {
        println!("  {} — {} on GPUs {:?}", a.id, a.client_id, a.gpus);
    }

    // 6. Release
    let result = client.release(&alloc.id, "client-smoke-test")?;
    println!("\n=== Released ===");
    println!("GPUs freed: {:?}", result.gpus_freed);

    // 7. Verify release
    let avail2 = client.available_resources()?;
    println!("\nAfter release — Free GPUs: {:?}", avail2.available_gpus);

    println!("\n✅ All smoke tests passed!");
    Ok(())
}
