//! Dream cycle scheduler (Phase 3).
//!
//! Monitors system idle state and triggers dream cycles after sustained idle.
//! The scheduler does not execute dream tasks itself — it manages the trigger
//! lifecycle and emits ledger events. Dream task execution is handled by the
//! dream task framework (future module).
//!
//! State machine:
//!   Idle → Armed (idle detected, counting toward threshold)
//!   Armed → Triggered (threshold reached, dream cycle starts)
//!   Triggered → Idle (activity resumes or dream cycle completes)
//!   Any state → Idle (system exits idle)

#[allow(unused_imports)]
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};

/// Configuration for the dream scheduler.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DreamConfig {
    /// Enable dream cycles (default: false).
    pub enabled: bool,
    /// Minimum sustained idle time before triggering a dream cycle (default: 120 min).
    pub idle_threshold_mins: u64,
    /// Maximum duration for a single dream cycle (default: 30 mins).
    /// Dream tasks must complete or be preempted within this window.
    pub max_cycle_duration_mins: u64,
    /// Minimum interval between consecutive dream cycles (default: 240 min).
    /// Prevents rapid re-triggering after a short activity burst.
    pub cooldown_mins: u64,
}

impl Default for DreamConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            idle_threshold_mins: 120,
            max_cycle_duration_mins: 30,
            cooldown_mins: 240,
        }
    }
}

/// Current state of the dream scheduler.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DreamState {
    /// Not idle or not yet armed.
    Idle,
    /// System is idle, counting toward threshold.
    Armed {
        /// When the current idle period started.
        idle_since: DateTime<Utc>,
        /// Minutes remaining until trigger.
        mins_remaining: u64,
    },
    /// Dream cycle is actively running.
    Triggered {
        /// When this dream cycle started.
        started_at: DateTime<Utc>,
        /// Maximum duration before forced preemption.
        max_duration_mins: u64,
    },
}

impl Default for DreamState {
    fn default() -> Self {
        DreamState::Idle
    }
}

impl DreamState {
    pub fn is_active(&self) -> bool {
        matches!(self, DreamState::Triggered { .. })
    }

    pub fn is_armed(&self) -> bool {
        matches!(self, DreamState::Armed { .. })
    }
}

/// The dream scheduler. Tracks idle state and manages dream cycle triggers.
pub struct DreamScheduler {
    config: DreamConfig,
    state: DreamState,
    /// When the last dream cycle completed or was preempted.
    last_cycle_end: Option<DateTime<Utc>>,
    /// Total number of dream cycles triggered (lifetime counter).
    cycles_triggered: u64,
    /// Current active dream cycle ID (if any). Used for broker preemption.
    current_cycle_id: Option<String>,
}

impl DreamScheduler {
    pub fn new(config: DreamConfig) -> Self {
        Self {
            config,
            state: DreamState::Idle,
            last_cycle_end: None,
            cycles_triggered: 0,
            current_cycle_id: None,
        }
    }

    /// Current configuration.
    pub fn config(&self) -> &DreamConfig {
        &self.config
    }

    /// Current scheduler state.
    pub fn state(&self) -> &DreamState {
        &self.state
    }

    /// Total dream cycles triggered since daemon start.
    pub fn cycles_triggered(&self) -> u64 {
        self.cycles_triggered
    }

    /// Whether a dream cycle is currently active.
    pub fn is_dreaming(&self) -> bool {
        self.state.is_active()
    }

    /// Get the current active dream cycle ID, if any.
    /// Used by the broker to tag allocations for preemption.
    pub fn current_cycle_id(&self) -> Option<&str> {
        self.current_cycle_id.as_deref()
    }

    /// Update scheduler state based on current system idle status.
    ///
    /// Call this from the observer loop after each poll. Returns an optional
    /// `DreamEvent` if a state transition occurred that should be logged.
    pub fn update(
        &mut self,
        system_idle: bool,
        idle_since: Option<DateTime<Utc>>,
        now: DateTime<Utc>,
    ) -> Option<DreamEvent> {
        if !self.config.enabled {
            return None;
        }

        match &self.state {
            DreamState::Idle => self.handle_idle_state(system_idle, idle_since, now),
            DreamState::Armed { .. } => self.handle_armed_state(system_idle, idle_since, now),
            DreamState::Triggered { started_at, max_duration_mins } => {
                self.handle_triggered_state(system_idle, *started_at, *max_duration_mins, now)
            }
        }
    }

    fn handle_idle_state(
        &mut self,
        system_idle: bool,
        idle_since: Option<DateTime<Utc>>,
        now: DateTime<Utc>,
    ) -> Option<DreamEvent> {
        if !system_idle {
            return None;
        }

        // Check cooldown: don't re-arm too soon after a previous cycle
        if let Some(last_end) = self.last_cycle_end {
            let elapsed_mins = (now - last_end).num_minutes() as u64;
            if elapsed_mins < self.config.cooldown_mins {
                return None;
            }
        }

        // System is idle and cooldown has passed — arm the trigger
        let armed_since = idle_since.unwrap_or(now);
        let elapsed_mins = (now - armed_since).num_minutes().max(0) as u64;
        let mins_remaining = self.config.idle_threshold_mins.saturating_sub(elapsed_mins);

        // If we've already been idle past the threshold, trigger immediately
        if mins_remaining == 0 {
            return self.trigger_dream_cycle(now);
        }

        self.state = DreamState::Armed {
            idle_since: armed_since,
            mins_remaining,
        };
        tracing::info!(
            threshold_mins = self.config.idle_threshold_mins,
            mins_remaining,
            "dream scheduler armed"
        );
        Some(DreamEvent::Armed {
            at: now,
            idle_since: armed_since,
            threshold_mins: self.config.idle_threshold_mins,
        })
    }

    fn handle_armed_state(
        &mut self,
        system_idle: bool,
        idle_since: Option<DateTime<Utc>>,
        now: DateTime<Utc>,
    ) -> Option<DreamEvent> {
        if !system_idle {
            // Activity resumed before threshold — disarm
            tracing::info!("dream scheduler disarmed (activity resumed)");
            self.state = DreamState::Idle;
            return Some(DreamEvent::Disarmed {
                at: now,
                reason: "activity resumed".to_string(),
            });
        }

        // Still idle — check if threshold reached
        let armed_since = idle_since.unwrap_or(now);
        let elapsed_mins = (now - armed_since).num_minutes().max(0) as u64;

        if elapsed_mins >= self.config.idle_threshold_mins {
            return self.trigger_dream_cycle(now);
        }

        // Update remaining time
        let mins_remaining = self.config.idle_threshold_mins.saturating_sub(elapsed_mins);
        self.state = DreamState::Armed {
            idle_since: armed_since,
            mins_remaining,
        };
        None
    }

    fn handle_triggered_state(
        &mut self,
        system_idle: bool,
        started_at: DateTime<Utc>,
        max_duration_mins: u64,
        now: DateTime<Utc>,
    ) -> Option<DreamEvent> {
        if !system_idle {
            // Activity resumed during dream — preempt
            let duration_secs = (now - started_at).num_seconds();
            let cycle_id = self.current_cycle_id.take();
            tracing::info!(duration_secs, ?cycle_id, "dream cycle preempted by activity");
            self.state = DreamState::Idle;
            self.last_cycle_end = Some(now);
            return Some(DreamEvent::Preempted {
                at: now,
                started_at,
                duration_secs,
                reason: "activity resumed".to_string(),
                cycle_id,
            });
        }

        // Check max duration
        let elapsed_mins = (now - started_at).num_minutes() as u64;
        if elapsed_mins >= max_duration_mins {
            let duration_secs = (now - started_at).num_seconds();
            let cycle_id = self.current_cycle_id.take();
            tracing::info!(duration_secs, ?cycle_id, "dream cycle completed (max duration)");
            self.state = DreamState::Idle;
            self.last_cycle_end = Some(now);
            return Some(DreamEvent::Completed {
                at: now,
                started_at,
                duration_secs,
                reason: "max duration reached".to_string(),
                cycle_id,
            });
        }

        None
    }

    fn trigger_dream_cycle(&mut self, now: DateTime<Utc>) -> Option<DreamEvent> {
        self.cycles_triggered += 1;
        let max_duration = self.config.max_cycle_duration_mins;
        let cycle_id = format!("dream-cycle-{:08x}", self.cycles_triggered);
        tracing::info!(
            cycle = self.cycles_triggered,
            cycle_id = %cycle_id,
            max_duration_mins = max_duration,
            "dream cycle triggered"
        );
        self.current_cycle_id = Some(cycle_id.clone());
        self.state = DreamState::Triggered {
            started_at: now,
            max_duration_mins: max_duration,
        };
        Some(DreamEvent::Triggered {
            at: now,
            cycle_number: self.cycles_triggered,
            max_duration_mins: max_duration,
            cycle_id,
        })
    }

    /// Manually cancel an active dream cycle (e.g., via socket API).
    pub fn cancel(&mut self, reason: &str) -> Option<DreamEvent> {
        if let DreamState::Triggered { started_at, .. } = self.state.clone() {
            let now = Utc::now();
            let duration_secs = (now - started_at).num_seconds();
            let cycle_id = self.current_cycle_id.take();
            self.state = DreamState::Idle;
            self.last_cycle_end = Some(now);
            tracing::info!(reason, ?cycle_id, "dream cycle cancelled");
            return Some(DreamEvent::Preempted {
                at: now,
                started_at,
                duration_secs,
                reason: reason.to_string(),
                cycle_id,
            });
        }
        None
    }

    /// Update configuration at runtime.
    pub fn update_config(&mut self, config: DreamConfig) {
        tracing::info!(?config, "dream scheduler config updated");
        self.config = config;
    }
}

/// Events emitted by the dream scheduler (for ledger logging).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum DreamEvent {
    /// Scheduler armed: system idle, counting toward threshold.
    Armed {
        at: DateTime<Utc>,
        idle_since: DateTime<Utc>,
        threshold_mins: u64,
    },
    /// Scheduler disarmed: activity resumed before threshold.
    Disarmed {
        at: DateTime<Utc>,
        reason: String,
    },
    /// Dream cycle triggered.
    Triggered {
        at: DateTime<Utc>,
        cycle_number: u64,
        max_duration_mins: u64,
        /// Unique identifier for this dream cycle. Used by the broker to tag
        /// allocations so they can be released on preemption/completion.
        cycle_id: String,
    },
    /// Dream cycle completed (max duration reached).
    Completed {
        at: DateTime<Utc>,
        started_at: DateTime<Utc>,
        duration_secs: i64,
        reason: String,
        cycle_id: Option<String>,
    },
    /// Dream cycle preempted (activity resumed or manual cancel).
    Preempted {
        at: DateTime<Utc>,
        started_at: DateTime<Utc>,
        duration_secs: i64,
        reason: String,
        cycle_id: Option<String>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> DreamConfig {
        DreamConfig {
            enabled: true,
            idle_threshold_mins: 10,
            max_cycle_duration_mins: 5,
            cooldown_mins: 20,
        }
    }

    #[test]
    fn test_initial_state_is_idle() {
        let scheduler = DreamScheduler::new(test_config());
        assert!(matches!(scheduler.state(), DreamState::Idle));
        assert_eq!(scheduler.cycles_triggered(), 0);
        assert!(!scheduler.is_dreaming());
    }

    #[test]
    fn test_disabled_scheduler_never_triggers() {
        let config = DreamConfig {
            enabled: false,
            ..test_config()
        };
        let mut scheduler = DreamScheduler::new(config);
        let now = Utc::now();
        let idle_since = now - Duration::minutes(120);

        // Even after 2 hours idle, nothing happens when disabled
        let event = scheduler.update(true, Some(idle_since), now);
        assert!(event.is_none());
        assert!(matches!(scheduler.state(), DreamState::Idle));
    }

    #[test]
    fn test_arms_when_idle() {
        let mut scheduler = DreamScheduler::new(test_config());
        let now = Utc::now();
        let idle_since = now - Duration::minutes(2);

        let event = scheduler.update(true, Some(idle_since), now);
        assert!(event.is_some());
        match event.unwrap() {
            DreamEvent::Armed { threshold_mins, .. } => {
                assert_eq!(threshold_mins, 10);
            }
            _ => panic!("expected Armed event"),
        }
        assert!(scheduler.state().is_armed());
    }

    #[test]
    fn test_disarms_on_activity() {
        let mut scheduler = DreamScheduler::new(test_config());
        let now = Utc::now();
        let idle_since = now - Duration::minutes(2);

        // Arm
        scheduler.update(true, Some(idle_since), now);
        assert!(scheduler.state().is_armed());

        // Activity resumes
        let event = scheduler.update(false, None, now + Duration::seconds(30));
        assert!(event.is_some());
        match event.unwrap() {
            DreamEvent::Disarmed { reason, .. } => {
                assert_eq!(reason, "activity resumed");
            }
            _ => panic!("expected Disarmed event"),
        }
        assert!(matches!(scheduler.state(), DreamState::Idle));
    }

    #[test]
    fn test_triggers_after_threshold() {
        let mut scheduler = DreamScheduler::new(test_config());
        let now = Utc::now();
        // Already idle past threshold (10 min config, 15 min elapsed)
        let idle_since = now - Duration::minutes(15);

        let event = scheduler.update(true, Some(idle_since), now);
        assert!(event.is_some());
        match event.unwrap() {
            DreamEvent::Triggered { cycle_number, max_duration_mins, cycle_id, .. } => {
                assert_eq!(cycle_number, 1);
                assert_eq!(max_duration_mins, 5);
                assert!(!cycle_id.is_empty());
            }
            _ => panic!("expected Triggered event"),
        }
        assert!(scheduler.is_dreaming());
        assert_eq!(scheduler.cycles_triggered(), 1);
    }

    #[test]
    fn test_preempts_on_activity_during_dream() {
        let mut scheduler = DreamScheduler::new(test_config());
        let now = Utc::now();
        let idle_since = now - Duration::minutes(15);

        // Trigger dream
        scheduler.update(true, Some(idle_since), now);
        assert!(scheduler.is_dreaming());

        // Activity resumes 2 minutes into the dream
        let event = scheduler.update(false, None, now + Duration::minutes(2));
        assert!(event.is_some());
        match event.unwrap() {
            DreamEvent::Preempted { duration_secs, reason, .. } => {
                assert_eq!(reason, "activity resumed");
                assert!((duration_secs - 120).abs() < 5); // ~2 minutes
            }
            _ => panic!("expected Preempted event"),
        }
        assert!(!scheduler.is_dreaming());
    }

    #[test]
    fn test_completes_at_max_duration() {
        let mut scheduler = DreamScheduler::new(test_config());
        let now = Utc::now();
        let idle_since = now - Duration::minutes(15);

        // Trigger dream
        scheduler.update(true, Some(idle_since), now);
        assert!(scheduler.is_dreaming());

        // Still idle at max duration (5 min)
        let event = scheduler.update(true, Some(idle_since), now + Duration::minutes(6));
        assert!(event.is_some());
        match event.unwrap() {
            DreamEvent::Completed { reason, .. } => {
                assert_eq!(reason, "max duration reached");
            }
            _ => panic!("expected Completed event"),
        }
        assert!(!scheduler.is_dreaming());
    }

    #[test]
    fn test_cooldown_prevents_immediate_retrigger() {
        let mut scheduler = DreamScheduler::new(test_config());
        let now = Utc::now();
        let idle_since = now - Duration::minutes(15);

        // Trigger and complete a dream cycle
        scheduler.update(true, Some(idle_since), now);
        scheduler.update(true, Some(idle_since), now + Duration::minutes(6));
        assert_eq!(scheduler.cycles_triggered(), 1);

        // Activity resumes, then idle again after only 5 minutes (within 20 min cooldown)
        let resume = now + Duration::minutes(7);
        scheduler.update(false, None, resume);
        let reidle = resume + Duration::minutes(5);
        let event = scheduler.update(true, Some(reidle), reidle);

        // Should NOT arm because we're still in cooldown
        assert!(event.is_none());
        assert!(matches!(scheduler.state(), DreamState::Idle));
    }

    #[test]
    fn test_manual_cancel() {
        let mut scheduler = DreamScheduler::new(test_config());
        let now = Utc::now();
        let idle_since = now - Duration::minutes(15);

        // Trigger dream
        scheduler.update(true, Some(idle_since), now);
        assert!(scheduler.is_dreaming());

        // Manual cancel
        let event = scheduler.cancel("admin request");
        assert!(event.is_some());
        match event.unwrap() {
            DreamEvent::Preempted { reason, .. } => {
                assert_eq!(reason, "admin request");
            }
            _ => panic!("expected Preempted event"),
        }
        assert!(!scheduler.is_dreaming());
    }

    #[test]
    fn test_multiple_cycles_count() {
        let mut scheduler = DreamScheduler::new(test_config());
        let now = Utc::now();
        let idle_since = now - Duration::minutes(15);

        // Cycle 1
        scheduler.update(true, Some(idle_since), now);
        scheduler.update(true, Some(idle_since), now + Duration::minutes(6));

        // Activity, then cooldown passes (20 min)
        let resume = now + Duration::minutes(7);
        scheduler.update(false, None, resume);
        let reidle = resume + Duration::minutes(30); // 23 min later, past cooldown
        let new_idle_since = reidle - Duration::minutes(15);

        // Cycle 2
        scheduler.update(true, Some(new_idle_since), reidle);
        assert_eq!(scheduler.cycles_triggered(), 2);
    }
}
