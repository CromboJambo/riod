//! Risk scoring for resource allocation requests.
//!
//! Quantifies how much system capacity is at stake if an allocation goes wrong.
//! Scores are 0-100 across four weighted factors: resource intensity, duration,
//! purpose weight, and current system load. The resulting risk level influences
//! policy evaluation (higher risk = stricter conditions).

use crate::observe::ResourceSnapshot;
use crate::policy::Purpose;
use serde::{Deserialize, Serialize};

/// Risk level derived from a score.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RiskLevel {
    Low,      // 0-30: auto-grant with standard conditions
    Medium,   // 31-60: grant with stricter conditions or shorter TTL
    High,     // 61-80: require justification or human approval
    Critical, // 81-100: deny unless explicit override
}

impl RiskLevel {
    /// Map a numeric score (0-100) to a risk level.
    pub fn from_score(score: u8, low_max: u8, medium_max: u8, high_max: u8) -> Self {
        if score <= low_max {
            RiskLevel::Low
        } else if score <= medium_max {
            RiskLevel::Medium
        } else if score <= high_max {
            RiskLevel::High
        } else {
            RiskLevel::Critical
        }
    }
}

impl std::fmt::Display for RiskLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RiskLevel::Low => write!(f, "low"),
            RiskLevel::Medium => write!(f, "medium"),
            RiskLevel::High => write!(f, "high"),
            RiskLevel::Critical => write!(f, "critical"),
        }
    }
}

/// A single risk factor contributing to the overall score.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskFactor {
    /// Factor name (e.g., "resource_intensity", "duration").
    pub name: String,
    /// Points contributed by this factor (0-100).
    pub points: u8,
    /// Human-readable explanation of why this factor scored as it did.
    pub detail: String,
}

/// Complete risk assessment for an allocation request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskScore {
    /// Total score (0-100).
    pub total: u8,
    /// Derived risk level.
    pub level: RiskLevel,
    /// Breakdown by factor.
    pub factors: Vec<RiskFactor>,
}

/// Configuration for the risk assessor.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskConfig {
    /// Maximum score for Low risk (default 30).
    #[serde(default = "default_low_max")]
    pub low_max: u8,
    /// Maximum score for Medium risk (default 60).
    #[serde(default = "default_medium_max")]
    pub medium_max: u8,
    /// Maximum score for High risk (default 80). Above this is Critical.
    #[serde(default = "default_high_max")]
    pub high_max: u8,
}

fn default_low_max() -> u8 {
    30
}
fn default_medium_max() -> u8 {
    60
}
fn default_high_max() -> u8 {
    80
}

impl Default for RiskConfig {
    fn default() -> Self {
        Self {
            low_max: 30,
            medium_max: 60,
            high_max: 80,
        }
    }
}

/// Request details needed for risk assessment.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskInput {
    /// Number of GPUs requested.
    pub gpu_count: usize,
    /// RAM in MB requested.
    pub ram_mb: u64,
    /// CPU percentage requested (0-100).
    pub cpu_percent: f32,
    /// Requested TTL in seconds.
    pub ttl_secs: u64,
    /// Purpose of the allocation.
    pub purpose: Purpose,
}

/// The risk assessor. Computes risk scores for allocation requests.
pub struct RiskAssessor {
    config: RiskConfig,
}

impl RiskAssessor {
    /// Create a new assessor with default thresholds.
    pub fn new() -> Self {
        Self {
            config: RiskConfig::default(),
        }
    }

    /// Create a new assessor with custom thresholds.
    pub fn with_config(config: RiskConfig) -> Self {
        Self { config }
    }

    /// Assess the risk of an allocation request against current system state.
    pub fn assess(&self, input: &RiskInput, snapshot: &ResourceSnapshot) -> RiskScore {
        let factors = vec![
            self.score_resource_intensity(input, snapshot),
            self.score_duration(input.ttl_secs),
            self.score_purpose(&input.purpose),
            self.score_system_load(snapshot),
        ];

        let total: u8 = factors.iter().map(|f| f.points as u16).sum::<u16>().min(100) as u8;
        let level = RiskLevel::from_score(
            total,
            self.config.low_max,
            self.config.medium_max,
            self.config.high_max,
        );

        RiskScore {
            total,
            level,
            factors,
        }
    }

    /// Factor 1: Resource intensity (max 40 points).
    /// Based on what fraction of total system resources are requested.
    fn score_resource_intensity(&self, input: &RiskInput, snapshot: &ResourceSnapshot) -> RiskFactor {
        let mut points = 0u8;
        let mut details = Vec::new();

        // GPU contribution (max 20): fraction of total GPUs requested
        if !snapshot.gpus.is_empty() {
            let total_gpus = snapshot.gpus.len();
            let requested = input.gpu_count;
            let gpu_points = ((requested as f32 * 20.0) / total_gpus as f32).round().min(20.0) as u8;
            points += gpu_points;
            details.push(format!("GPU {}/{}", requested, total_gpus));
        } else if input.gpu_count > 0 {
            // Requesting GPUs on a machine with none — that's invalid, score high
            points += 20;
            details.push("GPU requested but none available".to_string());
        }

        // RAM contribution (max 15): fraction of total RAM requested
        if snapshot.ram_total_mb > 0 && input.ram_mb > 0 {
            let ram_points = ((input.ram_mb as f32 * 15.0) / snapshot.ram_total_mb as f32).round().min(15.0) as u8;
            points += ram_points;
            details.push(format!("RAM {}MB/{}MB", input.ram_mb, snapshot.ram_total_mb));
        }

        // CPU contribution (max 5): fraction of total CPU requested
        if input.cpu_percent > 0.0 {
            let cpu_points = ((input.cpu_percent.min(100.0) / 100.0) * 5.0).round() as u8;
            points += cpu_points;
            details.push(format!("CPU {}%", input.cpu_percent));
        }

        RiskFactor {
            name: "resource_intensity".to_string(),
            points: points.min(40),
            detail: if details.is_empty() {
                "no resources specified".to_string()
            } else {
                details.join(", ")
            },
        }
    }

    /// Factor 2: Duration (max 20 points).
    /// Longer allocations are riskier — harder to preempt, more contention window.
    fn score_duration(&self, ttl_secs: u64) -> RiskFactor {
        let points = if ttl_secs <= 300 {
            0 // 5 min or less — negligible
        } else if ttl_secs <= 3600 {
            // 5 min to 1 hour: linear 0-10
            (((ttl_secs - 300) as f32 / 3300.0) * 10.0).round().min(10.0) as u8
        } else if ttl_secs <= 14400 {
            // 1 hour to 4 hours: linear 10-15
            (10.0 + ((ttl_secs - 3600) as f32 / 10800.0) * 5.0).round().min(15.0) as u8
        } else {
            // Over 4 hours: 15-20, capped
            (15.0 + ((ttl_secs - 14400) as f32 / 28800.0) * 5.0).round().min(20.0) as u8
        };

        RiskFactor {
            name: "duration".to_string(),
            points,
            detail: format!("TTL {}s", ttl_secs),
        }
    }

    /// Factor 3: Purpose weight (max 20 points).
    /// Inherent risk by purpose type.
    fn score_purpose(&self, purpose: &Purpose) -> RiskFactor {
        let (points, detail) = match purpose {
            Purpose::Inference => (5, "inference — typically short-lived, low impact".to_string()),
            Purpose::Dreaming => (10, "dreaming — background cognition, preemptible".to_string()),
            Purpose::Maintenance => (15, "maintenance — variable scope".to_string()),
            Purpose::Training => (20, "training — high resource consumption, long duration".to_string()),
            Purpose::Custom(name) => (15, format!("custom purpose '{}'", name)),
        };

        RiskFactor {
            name: "purpose".to_string(),
            points,
            detail,
        }
    }

    /// Factor 4: System load (max 20 points).
    /// Current utilization at request time — busier system = riskier new allocation.
    fn score_system_load(&self, snapshot: &ResourceSnapshot) -> RiskFactor {
        let mut points = 0u8;
        let mut details = Vec::new();

        // CPU load (max 10): >50% starts contributing
        if snapshot.cpu_usage_percent > 50.0 {
            let cpu_points = (((snapshot.cpu_usage_percent - 50.0) / 50.0) * 10.0).round().min(10.0) as u8;
            points += cpu_points;
            details.push(format!("CPU {}%", snapshot.cpu_usage_percent));
        }

        // GPU load (max 5): any GPU above 60% utilization
        if !snapshot.gpus.is_empty() {
            let busy_gpus = snapshot
                .gpus
                .iter()
                .filter(|g| g.utilization_percent > 60)
                .count();
            if busy_gpus > 0 {
                let gpu_points = (busy_gpus as u8 * 5).min(5);
                points += gpu_points;
                details.push(format!("{} GPU(s) >60%", busy_gpus));
            }
        }

        // RAM load (max 5): >70% usage starts contributing
        if snapshot.ram_total_mb > 0 {
            let ram_percent = (snapshot.ram_used_mb * 100) / snapshot.ram_total_mb;
            if ram_percent > 70 {
                let ram_points = (((ram_percent - 70) as f32 / 30.0) * 5.0).round().min(5.0) as u8;
                points += ram_points;
                details.push(format!("RAM {}%", ram_percent));
            }
        }

        RiskFactor {
            name: "system_load".to_string(),
            points: points.min(20),
            detail: if details.is_empty() {
                "system idle".to_string()
            } else {
                details.join(", ")
            },
        }
    }
}

impl Default for RiskAssessor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::observe::{GpuInfo, ObserveConfig};
    use chrono::Utc;

    fn test_snapshot() -> ResourceSnapshot {
        ResourceSnapshot {
            timestamp: Utc::now(),
            cpu_usage_percent: 10.0,
            ram_total_mb: 32768,
            ram_used_mb: 8192,
            gpus: vec![GpuInfo {
                index: 0,
                name: "Test GPU".into(),
                vram_total_mb: 16000,
                vram_used_mb: 4000,
                utilization_percent: 20,
            }],
        }
    }

    fn busy_snapshot() -> ResourceSnapshot {
        ResourceSnapshot {
            timestamp: Utc::now(),
            cpu_usage_percent: 85.0,
            ram_total_mb: 32768,
            ram_used_mb: 28000,
            gpus: vec![GpuInfo {
                index: 0,
                name: "Test GPU".into(),
                vram_total_mb: 16000,
                vram_used_mb: 14000,
                utilization_percent: 95,
            }],
        }
    }

    #[test]
    fn test_low_risk_inference() {
        let assessor = RiskAssessor::new();
        let input = RiskInput {
            gpu_count: 0,
            ram_mb: 1024,
            cpu_percent: 10.0,
            ttl_secs: 300,
            purpose: Purpose::Inference,
        };
        let score = assessor.assess(&input, &test_snapshot());

        assert!(score.total <= 30, "expected low risk, got {}", score.total);
        assert_eq!(score.level, RiskLevel::Low);
    }

    #[test]
    fn test_high_risk_training_on_busy_system() {
        let assessor = RiskAssessor::new();
        let input = RiskInput {
            gpu_count: 1,
            ram_mb: 24000,
            cpu_percent: 80.0,
            ttl_secs: 7200,
            purpose: Purpose::Training,
        };
        let score = assessor.assess(&input, &busy_snapshot());

        assert!(score.total >= 61, "expected high risk, got {}", score.total);
        assert!(matches!(score.level, RiskLevel::High | RiskLevel::Critical));
    }

    #[test]
    fn test_duration_scoring() {
        let assessor = RiskAssessor::new();
        let snap = test_snapshot();
        let base_input = RiskInput {
            gpu_count: 0,
            ram_mb: 0,
            cpu_percent: 0.0,
            ttl_secs: 300,
            purpose: Purpose::Inference,
        };

        // Short TTL — minimal duration points
        let short = assessor.score_duration(300);
        assert_eq!(short.points, 0);

        // Medium TTL — some points
        let medium = assessor.score_duration(1800);
        assert!(medium.points > 0 && medium.points <= 10);

        // Long TTL — more points
        let long = assessor.score_duration(7200);
        assert!(long.points > medium.points && long.points <= 15);

        // Very long TTL — maxed
        let very_long = assessor.score_duration(86400);
        assert_eq!(very_long.points, 20);
    }

    #[test]
    fn test_purpose_weights() {
        let assessor = RiskAssessor::new();

        let inference = assessor.score_purpose(&Purpose::Inference);
        let dreaming = assessor.score_purpose(&Purpose::Dreaming);
        let maintenance = assessor.score_purpose(&Purpose::Maintenance);
        let training = assessor.score_purpose(&Purpose::Training);
        let custom = assessor.score_purpose(&Purpose::Custom("experiment".into()));

        assert_eq!(inference.points, 5);
        assert_eq!(dreaming.points, 10);
        assert_eq!(maintenance.points, 15);
        assert_eq!(training.points, 20);
        assert_eq!(custom.points, 15);
    }

    #[test]
    fn test_resource_intensity_full_gpu() {
        let assessor = RiskAssessor::new();
        let snap = test_snapshot(); // 1 GPU, 32GB RAM

        // Request the only GPU + half the RAM
        let input = RiskInput {
            gpu_count: 1,
            ram_mb: 16384,
            cpu_percent: 50.0,
            ttl_secs: 300,
            purpose: Purpose::Inference,
        };

        let factor = assessor.score_resource_intensity(&input, &snap);
        // GPU: 1/1 * 20 = 20, RAM: 16384/32768 * 15 = 7.5 → 8, CPU: 50% * 5 = 2.5 → 3
        // Total: ~31 (capped at 40)
        assert!(factor.points >= 25 && factor.points <= 40);
    }

    #[test]
    fn test_system_load_scoring() {
        let assessor = RiskAssessor::new();

        // Idle system — no load points
        let idle = assessor.score_system_load(&test_snapshot());
        assert_eq!(idle.points, 0);

        // Busy system — significant load points
        let busy = assessor.score_system_load(&busy_snapshot());
        assert!(busy.points >= 10, "expected high load points, got {}", busy.points);
    }

    #[test]
    fn test_risk_level_boundaries() {
        let config = RiskConfig::default();
        assert_eq!(RiskLevel::from_score(30, config.low_max, config.medium_max, config.high_max), RiskLevel::Low);
        assert_eq!(RiskLevel::from_score(31, config.low_max, config.medium_max, config.high_max), RiskLevel::Medium);
        assert_eq!(RiskLevel::from_score(60, config.low_max, config.medium_max, config.high_max), RiskLevel::Medium);
        assert_eq!(RiskLevel::from_score(61, config.low_max, config.medium_max, config.high_max), RiskLevel::High);
        assert_eq!(RiskLevel::from_score(80, config.low_max, config.medium_max, config.high_max), RiskLevel::High);
        assert_eq!(RiskLevel::from_score(81, config.low_max, config.medium_max, config.high_max), RiskLevel::Critical);
        assert_eq!(RiskLevel::from_score(100, config.low_max, config.medium_max, config.high_max), RiskLevel::Critical);
    }

    #[test]
    fn test_no_gpus_machine() {
        let assessor = RiskAssessor::new();
        let mut snap = test_snapshot();
        snap.gpus = vec![];

        // CPU-only request on a GPU-less machine — should work fine
        let input = RiskInput {
            gpu_count: 0,
            ram_mb: 4096,
            cpu_percent: 25.0,
            ttl_secs: 600,
            purpose: Purpose::Inference,
        };
        let score = assessor.assess(&input, &snap);
        assert!(score.total <= 30, "CPU-only request should be low risk, got {}", score.total);

        // GPU request on a GPU-less machine — should flag high resource intensity
        let bad_input = RiskInput {
            gpu_count: 1,
            ram_mb: 4096,
            cpu_percent: 25.0,
            ttl_secs: 600,
            purpose: Purpose::Training,
        };
        let bad_score = assessor.assess(&bad_input, &snap);
        assert!(bad_score.total > score.total, "GPU request on GPU-less machine should be riskier");
    }

    #[test]
    fn test_score_never_exceeds_100() {
        let assessor = RiskAssessor::new();
        let input = RiskInput {
            gpu_count: 8, // Way more than available
            ram_mb: 999999,
            cpu_percent: 100.0,
            ttl_secs: 86400 * 7,
            purpose: Purpose::Training,
        };
        let score = assessor.assess(&input, &busy_snapshot());
        assert!(score.total <= 100);
    }

    #[test]
    fn test_risk_score_serialization() {
        let assessor = RiskAssessor::new();
        let input = RiskInput {
            gpu_count: 1,
            ram_mb: 8192,
            cpu_percent: 50.0,
            ttl_secs: 3600,
            purpose: Purpose::Training,
        };
        let score = assessor.assess(&input, &test_snapshot());

        // Should serialize to JSON without error
        let json = serde_json::to_string(&score).unwrap();
        assert!(json.contains("resource_intensity"));
        assert!(json.contains("duration"));
        assert!(json.contains("purpose"));
        assert!(json.contains("system_load"));

        // And deserialize back
        let deserialized: RiskScore = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.total, score.total);
        assert_eq!(deserialized.level, score.level);
    }
}
