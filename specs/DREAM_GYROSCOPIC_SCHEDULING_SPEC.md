# rIOd Dream Mode: Gyroscopic Scheduling Semantics (Phase 3)

Status: Spec — defines allocation-coupled scheduling for dream tasks
Scope: `src/dream.rs`, `src/broker.rs`, `src/socket.rs` (new API methods), `src/state.rs`
Prerequisites: Phase 0 (observer), Phase 1 (broker), existing dream scheduler + task registry

## §0 The Gyroscopic Model

### 0.1 Current State (Time-Based)

The existing dream scheduler (`dream.rs`) uses a pure time-based trigger:

```
System idle > threshold → arm → trigger cycle → run max_duration or until activity
```

This works, but it's **uncoupled from the broker**. Dream cycles know nothing about
which allocations are active, how much residual capacity exists, or when an allocation
is about to expire. The only coupling is: "activity resumed" = preempt everything.

### 0.2 The Gyroscopic Insight

A gyroscope has a spinning rotor (angular momentum) and a pivot point. Apply torque
to one axis, and the response appears on the **perpendicular** axis — gyroscopic
precession. The key property: **stall one pivot point and observe the effect on the
other arm.**

Mapping to rIOd:

| Gyroscope | rIOd |
|-----------|------|
| Spinning rotor (angular momentum) | Active allocations (system's "momentum" — work in progress) |
| Pivot points | Allocation lifecycle events (grant, heartbeat, release, TTL expiry) |
| Applied torque | A new allocation request, or a release freeing capacity |
| Precession (perpendicular response) | Dream scheduling reorients: expands into freed space, contracts when space is reclaimed |
| Stall one pivot, observe other arm | Pause/hang an allocation → dream work should expand into that space; resume it → dream work contracts |

### 0.3 The Coupling Test

The fundamental test of gyroscopic semantics:

1. Start with active allocation A (GPU, 8GB) and dream task D running on residual capacity
2. **Stall** allocation A (stop heartbeats, let TTL begin counting down, or explicitly pause)
3. **Observe**: does dream task D expand into A's space as A's TTL expires?
4. **Resume** allocation A (new heartbeat or reallocation)
5. **Observe**: does dream task D contract/preempt to make room?

If the answer to both is "yes, automatically and without external intervention,"
the gyroscopic coupling is working. The current time-based model cannot do this —
it only knows "idle" or "not idle."

## §1 Design Principles

1. **Residual-first.** Dream work runs on resources *after* active allocations are satisfied. It never steals from a live allocation. If an allocation needs 8GB and 8GB is free, dream gets the remaining 24GB (on a 32GB card), not zero.

2. **Continuous re-evaluation.** Dream scheduling is not a one-shot trigger. Every allocation event (grant, release, heartbeat, TTL tick) is a potential re-evaluation point for dream capacity.

3. **Graceful preemption.** When an active allocation needs space that dream work occupies, dream allocations are released first (they're tagged with `dream_cycle_id`). Active allocations are never preempted by dream work.

4. **Explicit coupling.** The relationship between allocation state and dream scheduling is visible in the ledger. Every capacity change that affects dream scheduling is logged as a `dream_capacity_changed` event.

5. **No lifecycle ownership.** rIOd still doesn't start/stop processes. It allocates resources and tags them. When dream allocations are released, it's up to the consuming process (crabjar, etc.) to notice its allocation is gone and stop.

## §2 Core Types

### 2.1 DreamCapacity

The central new type: a snapshot of how much resource is available for dream work
*right now*, derived from the broker's active allocations.

```rust
/// Current capacity available for dream work, computed from active allocations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DreamCapacity {
    /// GPU slots available for dream work (total - allocated to non-dream).
    pub gpu_slots: u32,
    /// GPU memory (MiB) available for dream work.
    pub gpu_mem_mib: u64,
    /// RAM (MiB) available for dream work.
    pub ram_mib: u64,
    /// CPU cores available for dream work (fractional).
    pub cpu_cores: f32,
    /// When this capacity snapshot was computed.
    pub computed_at: DateTime<Utc>,
    /// IDs of allocations that are consuming capacity (non-dream only).
    /// Dream allocations consume from the residual, not from this list.
    pub constrained_by: Vec<String>,
}

impl DreamCapacity {
    /// Can we fit a dream task with these requirements in the current residual?
    pub fn can_fit(&self, req: &DreamTaskRequirements) -> bool {
        self.gpu_slots >= req.gpu_slots
            && self.gpu_mem_mib >= req.gpu_mem_mib
            && self.ram_mib >= req.ram_mib
            && self.cpu_cores >= req.cpu_cores
    }

    /// Is there any capacity at all for dream work?
    pub fn is_empty(&self) -> bool {
        self.gpu_slots == 0 && self.gpu_mem_mib == 0 && self.ram_mib == 0 && self.cpu_cores == 0.0
    }
}
```

### 2.2 DreamTaskRequirements

What a dream task needs to run. Registered alongside the task metadata.

```rust
/// Resource requirements for a dream task.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DreamTaskRequirements {
    pub gpu_slots: u32,
    pub gpu_mem_mib: u64,
    pub ram_mib: u64,
    pub cpu_cores: f32,
}

impl Default for DreamTaskRequirements {
    fn default() -> Self {
        // Minimal dream task: no GPU, 1GB RAM, 0.5 cores
        Self {
            gpu_slots: 0,
            gpu_mem_mib: 0,
            ram_mib: 1024,
            cpu_cores: 0.5,
        }
    }
}
```

### 2.3 DreamScheduleState

Replaces the simple `DreamState` enum with a richer state that tracks capacity coupling.

```rust
/// Gyroscopic dream scheduling state.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DreamScheduleState {
    /// No dream activity. May be due to: system busy, no capacity, or disabled.
    Inactive { reason: InactiveReason },

    /// Capacity exists and is being tracked, but no tasks are running yet.
    Armed {
        capacity: DreamCapacity,
        armed_at: DateTime<Utc>,
    },

    /// Dream tasks are actively running on residual capacity.
    Active {
        started_at: DateTime<Utc>,
        cycle_id: String,
        current_capacity: DreamCapacity,
        active_task_ids: Vec<String>,
    },

    /// Capacity dropped below what running tasks need — graceful contraction in progress.
    Contracting {
        cycle_id: String,
        target_capacity: DreamCapacity,
        started_at: DateTime<Utc>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InactiveReason {
    Disabled,
    SystemBusy,
    NoResidualCapacity,
    Cooldown,
}
```

### 2.4 DreamCapacityEvent

Ledger events for capacity changes (the "torque" in the gyroscopic model).

```rust
/// Events that trigger dream scheduling re-evaluation.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum DreamCapacityEvent {
    /// An allocation was granted, reducing residual capacity.
    AllocationGranted {
        at: DateTime<Utc>,
        allocation_id: String,
        new_capacity: DreamCapacity,
    },
    /// An allocation was released, increasing residual capacity.
    AllocationReleased {
        at: DateTime<Utc>,
        allocation_id: String,
        new_capacity: DreamCapacity,
    },
    /// An allocation's TTL is expiring soon — dream work should prepare to expand.
    AllocationExpiring {
        at: DateTime<Utc>,
        allocation_id: String,
        expires_at: DateTime<Utc>,
        current_capacity: DreamCapacity,
    },
    /// Capacity changed and dream scheduling re-evaluated.
    ScheduleReevaluated {
        at: DateTime<Utc>,
        trigger: String, // "allocation_granted", "allocation_released", "ttl_tick", etc.
        previous_state: String,
        new_state: String,
        capacity: DreamCapacity,
    },
}
```

## §3 Scheduling Algorithm

### 3.1 Re-evaluation Triggers

Dream scheduling is re-evaluated on **every** broker event, not just idle detection:

| Trigger | Effect on dream capacity | Action |
|---------|--------------------------|--------|
| Allocation granted (non-dream) | Capacity decreases | Check if running dream tasks still fit; if not, enter `Contracting` |
| Allocation released | Capacity increases | If `Inactive(NoResidualCapacity)` or `Armed`, consider starting/expanding dream work |
| Allocation TTL expiry (graceful release) | Capacity increases | Same as release — dream work expands into freed space |
| Heartbeat received | No capacity change | No re-evaluation needed (allocation is healthy) |
| New allocation request arrives | Potential future decrease | If request would consume residual, preempt dream allocations first |
| Idle detector state change | Context signal | Time-based trigger still works as a *fallback* for "nobody's here at all" |

### 3.2 The Re-evaluation Function

```rust
impl GyroscopicDreamScheduler {
    /// Core re-evaluation: called on every broker event.
    /// This is the "gyroscopic precession" — torque applied to allocations
    /// produces a perpendicular response in dream scheduling.
    pub fn reevaluate(
        &mut self,
        broker: &ResourceBroker,
        idle_state: &IdleDetector,
        now: DateTime<Utc>,
    ) -> Vec<DreamCapacityEvent> {
        let mut events = Vec::new();

        // Step 1: Compute current residual capacity from broker state
        let capacity = self.compute_residual_capacity(broker);

        // Step 2: Check if capacity changed significantly since last evaluation
        if let Some(prev) = &self.last_capacity {
            if !capacity.significantly_changed_from(prev) {
                return events; // No meaningful change, skip
            }
        }
        self.last_capacity = Some(capacity.clone());

        // Step 3: State machine transition based on capacity + idle state
        let system_idle = idle_state.is_idle();
        let new_state = match &self.state {
            DreamScheduleState::Inactive { reason } => {
                if !self.config.enabled {
                    return events;
                }
                if capacity.is_empty() {
                    // Still no room
                    events.push(DreamCapacityEvent::ScheduleReevaluated {
                        at: now, trigger: "capacity_check".into(),
                        previous_state: format!("{:?}", self.state),
                        new_state: "inactive(no_capacity)".into(),
                        capacity: capacity.clone(),
                    });
                    return events;
                }
                if !system_idle && !self.config.allow_during_activity {
                    // System is busy and we don't allow dream during activity
                    return events;
                }
                // Capacity exists, system conditions met — arm
                let armed = DreamScheduleState::Armed {
                    capacity: capacity.clone(),
                    armed_at: now,
                };
                events.push(DreamCapacityEvent::ScheduleReevaluated {
                    at: now, trigger: "capacity_available".into(),
                    previous_state: format!("{:?}", self.state),
                    new_state: "armed".into(),
                    capacity: capacity.clone(),
                });
                self.state = armed;
            }
            DreamScheduleState::Armed { .. } => {
                // Capacity still exists — transition to Active (start dream cycle)
                if capacity.is_empty() {
                    // Lost capacity before we started — go back to inactive
                    self.state = DreamScheduleState::Inactive { reason: InactiveReason::NoResidualCapacity };
                    events.push(DreamCapacityEvent::ScheduleReevaluated {
                        at: now, trigger: "capacity_lost".into(),
                        previous_state: "armed".into(),
                        new_state: "inactive(no_capacity)".into(),
                        capacity: capacity.clone(),
                    });
                } else {
                    // Start dream cycle with current residual
                    let cycle_id = self.next_cycle_id();
                    let active = DreamScheduleState::Active {
                        started_at: now,
                        cycle_id: cycle_id.clone(),
                        current_capacity: capacity.clone(),
                        active_task_ids: vec![],
                    };
                    events.push(DreamCapacityEvent::ScheduleReevaluated {
                        at: now, trigger: "dream_cycle_start".into(),
                        previous_state: "armed".into(),
                        new_state: format!("active({})", cycle_id),
                        capacity: capacity.clone(),
                    });
                    self.state = active;
                }
            }
            DreamScheduleState::Active { .. } => {
                if capacity.is_empty() {
                    // All residual consumed — contract dream work
                    let contracting = DreamScheduleState::Contracting {
                        cycle_id: self.current_cycle_id().unwrap_or_default(),
                        target_capacity: capacity.clone(),
                        started_at: now,
                    };
                    events.push(DreamCapacityEvent::ScheduleReevaluated {
                        at: now, trigger: "capacity_depleted".into(),
                        previous_state: format!("active({})", self.current_cycle_id().unwrap_or_default()),
                        new_state: "contracting".into(),
                        capacity: capacity.clone(),
                    });
                    self.state = contracting;
                } else {
                    // Update capacity snapshot — dream work can continue, maybe expand
                    if let DreamScheduleState::Active { current_capacity, .. } = &mut self.state {
                        *current_capacity = capacity.clone();
                    }
                }
            }
            DreamScheduleState::Contracting { .. } => {
                // Check if contraction completed (all dream allocations released)
                if broker.active_dream_allocation_count(&self.current_cycle_id().unwrap_or_default()) == 0 {
                    self.state = DreamScheduleState::Inactive { reason: InactiveReason::SystemBusy };
                    events.push(DreamCapacityEvent::ScheduleReevaluated {
                        at: now, trigger: "contraction_complete".into(),
                        previous_state: "contracting".into(),
                        new_state: "inactive(system_busy)".into(),
                        capacity: capacity.clone(),
                    });
                }
            }
        };

        events
    }

    /// Compute residual capacity: total resources minus non-dream allocations.
    fn compute_residual_capacity(&self, broker: &ResourceBroker) -> DreamCapacity {
        let snapshot = broker.snapshot(); // ResourceSnapshot with totals
        let allocations = broker.allocations();

        // Sum up what non-dream allocations are using
        let (mut used_gpu_slots, mut used_gpu_mem, mut used_ram) = (0u32, 0u64, 0u64);
        let mut constrained_by = Vec::new();

        for alloc in allocations {
            if alloc.dream_cycle_id.is_none() {
                // Non-dream allocation — it constrains dream capacity
                used_gpu_slots += alloc.gpu_slots;
                used_gpu_mem += alloc.gpu_mem_mib;
                used_ram += alloc.ram_mib;
                constrained_by.push(alloc.id.clone());
            }
        }

        DreamCapacity {
            gpu_slots: snapshot.gpu.total_slots.saturating_sub(used_gpu_slots),
            gpu_mem_mib: snapshot.gpu.total_memory_mib.saturating_sub(used_gpu_mem),
            ram_mib: snapshot.ram.total_mib.saturating_sub(used_ram),
            cpu_cores: (snapshot.cpu.total_cores - used_cpu_cores).max(0.0),
            computed_at: Utc::now(),
            constrained_by,
        }
    }
}
```

### 3.3 Preemption Priority

When a new non-dream allocation request arrives and there's insufficient capacity:

1. **First**: Check if dream allocations can be released to make room (they're tagged)
2. **Second**: If releasing dream allocations still isn't enough, reject the request
3. **Never**: Preempt a non-dream allocation for another non-dream allocation (that's the broker's existing logic)

This is the "other arm absorbs torque" property: when the system needs to respond to a new
demand (torque), dream work (the other arm) gives way first.

```rust
impl ResourceBroker {
    /// Try to allocate, preempting dream allocations if needed.
    pub fn allocate_with_dream_preemption(
        &mut self,
        request: AllocationRequest,
        dream_cycle_id: Option<&str>, // The active dream cycle whose allocations can be preempted
    ) -> Result<Allocation, String> {
        // First try normal allocation
        if let Ok(alloc) = self.allocate(&request) {
            return Ok(alloc);
        }

        // Normal allocation failed — try releasing dream allocations to make room
        if let Some(cycle_id) = dream_cycle_id {
            let freed = self.release_dream_allocations_for_capacity(cycle_id, &request);
            if freed > 0 {
                tracing::info!(freed, "dream allocations released to accommodate new request");
                // Retry allocation with freed capacity
                return self.allocate(&request);
            }
        }

        Err("insufficient resources (even after dream preemption)".to_string())
    }
}
```

## §4 API Changes

### 4.1 New Methods

| Method | Params | Returns | Description |
|--------|--------|---------|-------------|
| `dream.capacity` | — | DreamCapacity + state | Current residual capacity and dream schedule state |
| `dream.expand` | `{task_name}` | allocation or error | Explicitly request more capacity for a running dream task (only if residual allows) |
| `dream.contract` | `{allocation_id}` | message | Explicitly release a dream allocation (graceful contraction) |

### 4.2 Modified Methods

- `resources.allocate`: When a new non-dream allocation arrives, it automatically triggers
  dream preemption if needed (the caller doesn't need to know about dreams).
- `dream.status`: Returns the full `DreamScheduleState` including capacity info, not just
  "idle/armed/triggered."

### 4.3 Ledger Events

All capacity changes and state transitions are logged as `DreamCapacityEvent` entries
in the append-only ledger. This makes the gyroscopic coupling **auditable**: you can
replay the ledger and see exactly when an allocation event caused a dream scheduling
change, and how much capacity moved between the two "arms."

## §5 Integration Points

### 5.1 Broker Events → Dream Re-evaluation

The broker's `allocate()`, `release()`, and TTL expiry paths each call:

```rust
// After any allocation state change:
if let Some(dream_scheduler) = &mut state.dream_scheduler {
    let events = dream_scheduler.reevaluate(&broker, &detector, Utc::now());
    for event in events {
        ledger.append(DreamCapacityEvent);
    }
}
```

This is the **torque application point** — every allocation change is a potential
re-evaluation trigger.

### 5.2 Observer Loop → Idle Context

The observer loop still runs and provides the `system_idle` signal, but it's now
**one input among several**, not the sole trigger. The gyroscopic model uses:

- Residual capacity (from broker) — primary signal
- System idle state (from observer) — context/modifier
- Allocation TTL expiries (from broker) — expansion opportunities
- New allocation requests (from API) — contraction triggers

### 5.3 Time-Based Fallback

The existing time-based trigger (idle for N minutes → dream) is preserved as a
**fallback** for the "nobody's here at all" case. In the gyroscopic model, this
corresponds to: all allocations released → residual = 100% → dream capacity maxed out.

The time threshold becomes relevant for *what kind* of dream work runs:
- Short idle (minutes): light tasks (logging analysis, cache warmup)
- Long idle (hours): heavy tasks (training, large-scale analysis)

This is a policy decision made by the consuming client (crabjar), not by rIOd.
rIOd just reports "you have X capacity available for Y minutes of expected idle."

## §6 Migration Path

### Phase 3a: Capacity Tracking (no behavior change)

1. Add `DreamCapacity` type and `compute_residual_capacity()` to dream scheduler
2. Add `dream.capacity` API method
3. Log `DreamCapacityEvent::ScheduleReevaluated` on every broker event (but don't act on it yet)
4. **Zero behavior change** — existing time-based triggering continues unchanged

### Phase 3b: Allocation-Coupled Triggering

1. Add `reevaluate()` to dream scheduler
2. Wire broker events → re-evaluation
3. Dream cycles can now start when residual capacity appears (not just on idle timeout)
4. Existing time-based trigger becomes a fallback
5. **Behavior change**: dream work may start sooner (when allocations release) or later (when system is busy but not "idle" by the old definition)

### Phase 3c: Preemption Integration

1. Add `allocate_with_dream_preemption()` to broker
2. New non-dream allocations automatically preempt dream allocations if needed
3. Add `dream.expand` / `dream.contract` API methods
4. **Behavior change**: dream work is now truly "residual" — it gives way automatically

### Phase 3d: Coupling Tests + Auditability

1. Add integration tests for the coupling test (§0.3)
2. Add ledger query support for `DreamCapacityEvent` filtering
3. Document the gyroscopic model in AGENTS.md

## §7 Testing Strategy

### 7.1 The Gyroscopic Coupling Test (integration)

```rust
#[test]
fn test_gyroscopic_coupling_stall_and_resume() {
    // Setup: broker with 2 GPU slots, dream scheduler enabled
    let mut broker = ResourceBroker::new(test_config());
    let mut dream = GyroscopicDreamScheduler::new(test_dream_config());

    // Step 1: Grant allocation A (GPU slot 0)
    let alloc_a = broker.allocate(&request(1_gpu)).unwrap();

    // Step 2: Dream task D runs on residual (GPU slot 1)
    dream.reevaluate(&broker, &idle_detector_idle(), Utc::now());
    assert!(matches!(dream.state(), DreamScheduleState::Active { .. }));
    let alloc_d = broker.allocate_with_dream_cycle(&request(1_gpu), dream.current_cycle_id()).unwrap();

    // Step 3: Stall allocation A (let TTL expire)
    advance_time_past_ttl(&mut broker, &alloc_a);
    broker.expire_stale_allocations(Utc::now());

    // Step 4: Re-evaluate — dream should expand into freed space
    dream.reevaluate(&broker, &idle_detector_idle(), Utc::now());
    let capacity = dream.current_capacity();
    assert_eq!(capacity.gpu_slots, 2); // Full capacity now available for dream

    // Step 5: Resume allocation A (new request)
    let alloc_a2 = broker.allocate_with_dream_preemption(&request(1_gpu), dream.current_cycle_id()).unwrap();

    // Step 6: Dream should contract — one slot freed up, dream gives it back
    dream.reevaluate(&broker, &idle_detector_idle(), Utc::now());
    let capacity = dream.current_capacity();
    assert_eq!(capacity.gpu_slots, 1); // Back to 1 slot for dream

    // Verify: alloc_d was either released or reduced
    assert!(!broker.is_active(&alloc_d.id) || broker.allocation(&alloc_d.id).unwrap().gpu_slots < 1);
}
```

### 7.2 Unit Tests

- `DreamCapacity::can_fit()` with various requirement combinations
- `compute_residual_capacity()` with mixed dream/non-dream allocations
- State machine transitions for all trigger types
- Preemption priority: dream always yields to non-dream

## §8 Open Questions

1. **Partial contraction**: If a dream task has 2 GPU slots and capacity drops to 1, do we release the whole allocation or try to shrink it? Current design: release whole (simpler), client can reallocate smaller.

2. **Dream task priority**: If multiple dream tasks are queued and capacity increases, which gets it first? Current design: FIFO by registration time. Future: configurable priority in `DreamTaskInfo`.

3. **Cross-machine coordination**: Phase 3+ fleet coordination could extend this to "machine A is idle, machine B is busy → route dream work to A." Out of scope for per-machine rIOd.

4. **Interaction with Phase 2 policy**: Should high-risk allocations (per risk scoring) get priority over dream preemption? Current design: yes — risk score could modulate the preemption aggressiveness.

## Appendix A: Why "Gyroscopic"?

The name comes from the physical property that a gyroscope's response to torque is
**perpendicular** to the applied force. In rIOd:

- **Torque** = allocation lifecycle events (grant, release, TTL)
- **Response** = dream scheduling changes (expand, contract, start, stop)
- **Perpendicularity** = the response is not in the same "axis" as the trigger. An allocation
  being granted doesn't directly change dream state — it changes *capacity*, which then
  causes a scheduling re-evaluation, which may or may not change dream state.

The "stall one pivot point" test (§0.3) is the practical verification: if you can't observe
the other arm responding when you stall one pivot, the coupling isn't real — it's just two
independent timers.

This matters because the alternative (time-based triggering) has a fundamental flaw: it
can't respond to *structural* changes in resource usage. If an allocation releases at
minute 59 of a 60-minute idle threshold, the time-based model waits another minute. The
gyroscopic model responds immediately — the capacity appeared, dream work expands now.

## Appendix B: Relationship to Existing Dream Infrastructure

| Existing | New (Gyroscopic) | Change |
|----------|------------------|--------|
| `DreamScheduler` | `GyroscopicDreamScheduler` (extends/replaces) | Adds capacity tracking + re-evaluation |
| `DreamState` enum | `DreamScheduleState` enum | Richer states including `Contracting` |
| Time-based trigger only | Allocation events + idle state + TTL | Multiple trigger sources |
| Binary: dreaming or not | Continuous capacity spectrum | Dream work scales with residual |
| Preemption on activity only | Preemption on capacity pressure | Finer-grained response |
| No capacity awareness | `DreamCapacity` computed from broker | Knows exactly what's available |

The existing `dream_tasks.rs` (task registry, context, results) is **unchanged** — it's
orthogonal to the scheduling model. The task framework doesn't care *when* tasks run, only
*what* they produce. This spec changes *when*, not *what*.
