//! Integration tests for riod-client using a mock riod daemon.

use std::io::{Read, Write};
use std::os::unix::net::UnixListener;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;

use riod_client::{AllocateRequest, RiodClient};

/// Start a mock riod daemon that responds to JSON-RPC requests.
fn start_mock_daemon(socket_path: &PathBuf) -> Arc<AtomicBool> {
    let running = Arc::new(AtomicBool::new(true));
    let socket_path = socket_path.clone();
    let running_clone = running.clone();

    thread::spawn(move || {
        // Remove stale socket
        let _ = std::fs::remove_file(&socket_path);

        let listener = match UnixListener::bind(&socket_path) {
            Ok(l) => l,
            Err(_) => return,
        };

        while running_clone.load(Ordering::Relaxed) {
            // Set a timeout so we can check the running flag
            for stream in listener.incoming() {
                let mut stream = match stream {
                    Ok(s) => s,
                    Err(_) => continue,
                };
                let _ = stream.set_read_timeout(Some(std::time::Duration::from_millis(100)));

                let mut buffer = Vec::new();
                let mut chunk = [0u8; 4096];
                loop {
                    match stream.read(&mut chunk) {
                        Ok(0) => break,
                        Ok(n) => buffer.extend_from_slice(&chunk[..n]),
                        Err(_) => break,
                    }
                }

                let response = handle_mock_request(&buffer);
                let _ = stream.write_all(response.as_bytes());
            }
        }

        let _ = std::fs::remove_file(&socket_path);
    });

    // Give the daemon time to start
    thread::sleep(std::time::Duration::from_millis(100));
    running
}

/// Handle a JSON-RPC request and return a mock response.
fn handle_mock_request(request_bytes: &[u8]) -> String {
    let request: serde_json::Value = match serde_json::from_slice(request_bytes) {
        Ok(v) => v,
        Err(_) => {
            return serde_json::json!({
                "jsonrpc": "2.0",
                "error": {"code": -32700, "message": "Parse error"},
                "id": null
            }).to_string();
        }
    };

    let method = request["method"].as_str().unwrap_or("");
    let id = request.get("id").cloned().unwrap_or(serde_json::Value::Null);

    let data = match method {
        "resources.allocate" => serde_json::json!({
            "allocation": {
                "id": "alloc-test-001",
                "client_id": request["params"]["client_id"].as_str().unwrap_or("unknown"),
                "purpose": request["params"]["purpose"].as_str().unwrap_or("unknown"),
                "gpus": [0],
                "ram_mb": 4096,
                "cpu_percent": Some(50.0),
                "granted_at": "2026-11-11T00:00:00Z",
                "last_heartbeat": "2026-11-11T00:00:00Z",
                "expires_at": "2026-11-11T01:00:00Z"
            },
            "message": "Allocated 1 GPU(s) to test-client for 'test-purpose'"
        }),
        "resources.release" => serde_json::json!({
            "released": request["params"]["allocation_id"].as_str().unwrap_or("unknown"),
            "gpus_freed": [0]
        }),
        "resources.heartbeat" => serde_json::json!({
            "allocation_id": request["params"]["allocation_id"].as_str().unwrap_or("unknown"),
            "expires_at": "2026-11-11T03:00:00Z"
        }),
        "resources.list" => serde_json::json!({
            "allocations": [
                {
                    "id": "alloc-test-001",
                    "client_id": "test-client",
                    "purpose": "test",
                    "gpus": [0],
                    "ram_mb": 4096,
                    "ttl_secs": 3600,
                    "granted_at": "2026-11-11T00:00:00Z",
                    "last_heartbeat": "2026-11-11T00:00:00Z",
                    "expires_at": "2026-11-11T01:00:00Z"
                }
            ],
            "count": 1
        }),
        "resources.available" => serde_json::json!({
            "available_gpus": [1, 2],
            "total_gpus": 3,
            "allocated_gpus": 1,
            "total_ram_mb": 65536,
            "allocated_ram_mb": 4096,
            "available_ram_mb": 61440,
            "active_allocations": 1
        }),
        "resources.add_process" => serde_json::json!({}),
        "system.status" => serde_json::json!({
            "snapshot": {
                "timestamp": "2026-11-11T00:00:00Z",
                "cpu_usage_percent": 25.5,
                "ram_total_mb": 65536,
                "ram_used_mb": 16384,
                "gpus": [{
                    "index": 0,
                    "name": "NVIDIA RTX 3070 Ti",
                    "vram_total_mb": 8192,
                    "vram_used_mb": 2048,
                    "utilization_percent": 75
                }]
            },
            "uptime_secs": 3600
        }),
        "ledger.query" => serde_json::json!([]),
        "ledger.tail" => serde_json::json!({
            "events": [
                {
                    "id": "evt-001",
                    "timestamp": "2026-11-11T00:00:00Z",
                    "type": "allocation_granted",
                    "data": {"id": "alloc-test-001"}
                }
            ],
            "count": 1
        }),
        "ledger.stats" => serde_json::json!({
            "total_events": 42,
            "by_type": {
                "allocation_granted": 10,
                "allocation_released": 9,
                "idle_detected": 5
            }
        }),
        _ => {
            return serde_json::json!({
                "jsonrpc": "2.0",
                "error": {"code": -32601, "message": format!("Method not found: {}", method)},
                "id": id
            }).to_string();
        }
    };

    // Wrap in the standard success envelope
    serde_json::json!({
        "jsonrpc": "2.0",
        "result": {
            "success": true,
            "data": data
        },
        "id": id
    })
    .to_string()
}

#[test]
fn test_allocate_and_release() {
    let socket_path = PathBuf::from("/tmp/riod-client-test.sock");
    let running = start_mock_daemon(&socket_path);

    let client = RiodClient::connect(&socket_path).unwrap();

    // Allocate using the builder API
    let req = AllocateRequest::new("test-client", "test-purpose")
        .with_gpu_count(1)
        .with_ram_mb(4096)
        .with_ttl(3600);
    let alloc = client.allocate(&req).unwrap();
    assert_eq!(alloc.id, "alloc-test-001");
    assert_eq!(alloc.gpus, vec![0]);
    assert_eq!(alloc.ram_mb, 4096);

    // Release
    let release = client.release(&alloc.id, "test-client").unwrap();
    assert_eq!(release.released, "alloc-test-001");
    assert_eq!(release.gpus_freed, vec![0]);

    running.store(false, Ordering::Relaxed);
}

#[test]
fn test_available_resources() {
    let socket_path = PathBuf::from("/tmp/riod-client-test-avail.sock");
    let running = start_mock_daemon(&socket_path);

    let client = RiodClient::connect(&socket_path).unwrap();
    let avail = client.available_resources().unwrap();

    assert_eq!(avail.total_gpus, 3);
    assert_eq!(avail.allocated_gpus, 1);
    assert_eq!(avail.available_gpus, vec![1, 2]);
    assert_eq!(avail.active_allocations, 1);

    running.store(false, Ordering::Relaxed);
}

#[test]
fn test_system_status() {
    let socket_path = PathBuf::from("/tmp/riod-client-test-status.sock");
    let running = start_mock_daemon(&socket_path);

    let client = RiodClient::connect(&socket_path).unwrap();
    let status = client.system_status().unwrap();

    assert_eq!(status.cpu_usage_percent, 25.5);
    assert_eq!(status.ram_total_mb, 65536);
    assert_eq!(status.gpus.len(), 1);
    assert_eq!(status.gpus[0].name, "NVIDIA RTX 3070 Ti");

    running.store(false, Ordering::Relaxed);
}

#[test]
fn test_heartbeat() {
    let socket_path = PathBuf::from("/tmp/riod-client-test-hb.sock");
    let running = start_mock_daemon(&socket_path);

    let client = RiodClient::connect(&socket_path).unwrap();
    let expires_at = client.heartbeat("alloc-test-001", "test-client").unwrap();

    // Verify the response deserializes correctly
    assert_eq!(expires_at, "2026-11-11T03:00:00Z");

    running.store(false, Ordering::Relaxed);
}

#[test]
fn test_add_process() {
    let socket_path = PathBuf::from("/tmp/riod-client-test-pid.sock");
    let running = start_mock_daemon(&socket_path);

    let client = RiodClient::connect(&socket_path).unwrap();
    let result = client.add_process("alloc-test-001", "test-client", 12345);
    assert!(result.is_ok());

    running.store(false, Ordering::Relaxed);
}

#[test]
fn test_ledger_tail() {
    let socket_path = PathBuf::from("/tmp/riod-client-test-ledger.sock");
    let running = start_mock_daemon(&socket_path);

    let client = RiodClient::connect(&socket_path).unwrap();
    let events = client.ledger_tail(10).unwrap();

    assert_eq!(events.len(), 1);
    // LedgerEntry flattens the event data, so event_type is inside the event value
    let event_type = events[0].event.get("type").and_then(|v| v.as_str());
    assert_eq!(event_type, Some("allocation_granted"));

    running.store(false, Ordering::Relaxed);
}

#[test]
fn test_connect_failure() {
    let result = RiodClient::connect("/tmp/nonexistent-riod.sock");
    assert!(result.is_ok()); // connect just creates the transport, doesn't actually connect

    // The actual connection failure happens on first call
    let client = result.unwrap();
    let err = client.available_resources().unwrap_err();
    assert!(matches!(err, riod_client::ClientError::ConnectFailed { .. }));
}
