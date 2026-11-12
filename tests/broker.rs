//! Integration tests for the resource broker.

use riod::broker::{AllocationRequest, BrokerError, ResourceBroker};
use riod::ledger::{EventLedger, LedgerConfig};
use riod::observe::GpuInfo;
use tempfile::tempdir;

fn test_broker() -> (ResourceBroker, EventLedger, tempfile::TempDir) {
    let dir = tempdir().unwrap();
    let ledger_path = dir.path().join("test.jsonl");
    let ledger = EventLedger::open(LedgerConfig {
        path: ledger_path,
        retention_days: 1,
    })
    .unwrap();

    let gpus = vec![
        GpuInfo {
            index: 0,
            name: "Test GPU 0".into(),
            vram_total_mb: 16000,
            vram_used_mb: 0,
            utilization_percent: 0,
        },
        GpuInfo {
            index: 1,
            name: "Test GPU 1".into(),
            vram_total_mb: 16000,
            vram_used_mb: 0,
            utilization_percent: 0,
        },
    ];

    let broker = ResourceBroker::new(gpus, 65536);
    (broker, ledger, dir)
}

#[test]
fn test_allocate_specific_gpu() {
    let (mut broker, mut ledger, _dir) = test_broker();

    let request = AllocationRequest {
        client_id: "test-client".into(),
        purpose: "testing".into(),
        gpus: Some(vec![0]),
        gpu_count: None,
        cpu_percent: Some(50.0),
        ram_mb: Some(8192),
        ttl_secs: 600,
        intent_id: None,
        dream_cycle_id: None,
    };

    let alloc = broker.allocate(&request, &mut ledger).unwrap();
    assert_eq!(alloc.gpus, vec![0]);
    assert_eq!(alloc.client_id, "test-client");
    assert_eq!(broker.allocations.len(), 1);
    assert!(broker.gpu_to_allocation.contains_key(&0));
}

#[test]
fn test_allocate_auto_assign() {
    let (mut broker, mut ledger, _dir) = test_broker();

    let request = AllocationRequest {
        client_id: "test-client".into(),
        purpose: "testing".into(),
        gpus: None,
        gpu_count: Some(2),
        cpu_percent: None,
        ram_mb: None,
        ttl_secs: 300,
        intent_id: None,
        dream_cycle_id: None,
    };

    let alloc = broker.allocate(&request, &mut ledger).unwrap();
    assert_eq!(alloc.gpus.len(), 2);
    assert_eq!(broker.available_gpus().len(), 0);
}

#[test]
fn test_allocate_insufficient_gpus() {
    let (mut broker, mut ledger, _dir) = test_broker();

    // Allocate both GPUs first
    let req1 = AllocationRequest {
        client_id: "client-a".into(),
        purpose: "test".into(),
        gpus: Some(vec![0]),
        gpu_count: None,
        cpu_percent: None,
        ram_mb: None,
        ttl_secs: 300,
        intent_id: None,
        dream_cycle_id: None,
    };
    broker.allocate(&req1, &mut ledger).unwrap();

    // Try to allocate 2 more — should fail
    let req2 = AllocationRequest {
        client_id: "client-b".into(),
        purpose: "test".into(),
        gpus: None,
        gpu_count: Some(2),
        cpu_percent: None,
        ram_mb: None,
        ttl_secs: 300,
        intent_id: None,
        dream_cycle_id: None,
    };
    let err = broker.allocate(&req2, &mut ledger).unwrap_err();
    assert!(matches!(err, BrokerError::InsufficientResources(_)));
}

#[test]
fn test_release_by_owner() {
    let (mut broker, mut ledger, _dir) = test_broker();

    let request = AllocationRequest {
        client_id: "owner".into(),
        purpose: "test".into(),
        gpus: Some(vec![0]),
        gpu_count: None,
        cpu_percent: None,
        ram_mb: None,
        ttl_secs: 300,
        intent_id: None,
        dream_cycle_id: None,
    };
    let alloc = broker.allocate(&request, &mut ledger).unwrap();

    // Owner can release
    broker.release(&alloc.id, "owner", &mut ledger).unwrap();
    assert_eq!(broker.allocations.len(), 0);
    assert!(broker.available_gpus().contains(&0));
}

#[test]
fn test_release_by_non_owner_fails() {
    let (mut broker, mut ledger, _dir) = test_broker();

    let request = AllocationRequest {
        client_id: "owner".into(),
        purpose: "test".into(),
        gpus: Some(vec![0]),
        gpu_count: None,
        cpu_percent: None,
        ram_mb: None,
        ttl_secs: 300,
        intent_id: None,
        dream_cycle_id: None,
    };
    let alloc = broker.allocate(&request, &mut ledger).unwrap();

    // Non-owner cannot release
    let err = broker
        .release(&alloc.id, "intruder", &mut ledger)
        .unwrap_err();
    assert!(matches!(err, BrokerError::NotAllocationOwner { .. }));
}

#[test]
fn test_heartbeat_extends_ttl() {
    let (mut broker, mut ledger, _dir) = test_broker();

    let request = AllocationRequest {
        client_id: "client".into(),
        purpose: "test".into(),
        gpus: Some(vec![0]),
        gpu_count: None,
        cpu_percent: None,
        ram_mb: None,
        ttl_secs: 10,
        intent_id: None,
        dream_cycle_id: None,
    };
    let alloc = broker.allocate(&request, &mut ledger).unwrap();
    let original_expiry = alloc.expires_at;

    // Heartbeat should extend expiry
    std::thread::sleep(std::time::Duration::from_millis(50));
    let updated = broker.heartbeat(&alloc.id, "client", &mut ledger).unwrap();
    assert!(updated.expires_at > original_expiry);
}

#[test]
fn test_reclaim_expired() {
    let (mut broker, mut ledger, _dir) = test_broker();

    // Create an allocation with a very short TTL
    let request = AllocationRequest {
        client_id: "client".into(),
        purpose: "test".into(),
        gpus: Some(vec![0]),
        gpu_count: None,
        cpu_percent: None,
        ram_mb: None,
        ttl_secs: 1,
        intent_id: None,
        dream_cycle_id: None,
    };
    broker.allocate(&request, &mut ledger).unwrap();
    assert_eq!(broker.allocations.len(), 1);

    // Wait for expiry
    std::thread::sleep(std::time::Duration::from_millis(1100));

    // Reclaim should free it
    let reclaimed = broker.reclaim_expired(&mut ledger);
    assert_eq!(reclaimed, 1);
    assert_eq!(broker.allocations.len(), 0);
}

#[test]
fn test_reclaim_dream_cycle() {
    let (mut broker, mut ledger, _dir) = test_broker();

    // Allocate one GPU with a dream cycle ID
    let req1 = AllocationRequest {
        client_id: "dream-task-1".into(),
        purpose: "dreaming".into(),
        gpus: Some(vec![0]),
        gpu_count: None,
        cpu_percent: None,
        ram_mb: None,
        ttl_secs: 300,
        intent_id: None,
        dream_cycle_id: Some("dream-cycle-00000001".into()),
    };
    broker.allocate(&req1, &mut ledger).unwrap();

    // Allocate one GPU without a dream cycle ID (should not be reclaimed)
    let req2 = AllocationRequest {
        client_id: "regular-task".into(),
        purpose: "inference".into(),
        gpus: Some(vec![1]),
        gpu_count: None,
        cpu_percent: None,
        ram_mb: None,
        ttl_secs: 300,
        intent_id: None,
        dream_cycle_id: None,
    };
    broker.allocate(&req2, &mut ledger).unwrap();

    assert_eq!(broker.allocations.len(), 2);

    // Reclaim the dream cycle — should only free the dream allocation
    let reclaimed = broker.reclaim_dream_cycle("dream-cycle-00000001", &mut ledger);
    assert_eq!(reclaimed, 1);
    assert_eq!(broker.allocations.len(), 1);

    // The regular task's GPU should still be allocated
    assert!(!broker.available_gpus().contains(&1));
    // The dream task's GPU should be freed
    assert!(broker.available_gpus().contains(&0));
}
