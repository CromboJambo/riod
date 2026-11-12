//! Cgroup v2 enforcement for resource allocations.
//!
//! When an allocation is granted, a cgroup is created under
//! `/sys/fs/cgroup/riod/<allocation_id>/` with memory and CPU limits
//! matching the request. Clients receive the cgroup path and can add
//! their processes to it (rIOd never moves processes itself).

use crate::broker::{Allocation, AllocationRequest};
use std::fs;
use std::path::{Path, PathBuf};

const CGROUP_ROOT: &str = "/sys/fs/cgroup/riod";

/// Error type for cgroup operations.
#[derive(Debug, thiserror::Error)]
pub enum CgroupError {
    #[error("cgroup v2 not available at {0}")]
    NotAvailable(String),

    #[error("failed to create cgroup directory: {0}")]
    CreateFailed(String),

    #[error("failed to write cgroup parameter: {0}")]
    WriteFailed(String),

    #[error("cgroup cleanup failed: {0}")]
    CleanupFailed(String),

    /// Advisory mode: cgroup operations are logged but not enforced.
    #[error("cgroup enforcement disabled (advisory mode)")]
    Disabled,
}

/// Cgroup manager for allocation enforcement.
pub struct CgroupManager {
    root: PathBuf,
    enabled: bool,
}

impl CgroupManager {
    /// Create a new cgroup manager.
    ///
    /// If `enabled` is false, all operations are no-ops (advisory mode).
    pub fn new(enabled: bool) -> Self {
        let root = PathBuf::from(CGROUP_ROOT);

        if enabled {
            // Ensure the riod cgroup exists and is delegated to us.
            fs::create_dir_all(&root).ok();
            Self::ensure_subtree_control(&root);
        }

        Self { root, enabled }
    }

    /// Check if cgroup enforcement is active.
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Create a cgroup for an allocation with the specified limits.
    /// Returns the cgroup path on success.
    pub fn create_for_allocation(
        &self,
        allocation: &Allocation,
        request: &AllocationRequest,
    ) -> Result<PathBuf, CgroupError> {
        if !self.enabled {
            return Err(CgroupError::Disabled);
        }

        let cgroup_path = self.root.join(&allocation.id);

        // Create the cgroup directory.
        fs::create_dir_all(&cgroup_path)
            .map_err(|e| CgroupError::CreateFailed(e.to_string()))?;

        // Enable memory and cpu controllers for this cgroup.
        Self::write_param(
            &cgroup_path.join("cgroup.subtree_control"),
            "+memory +cpu",
        )
        .ok(); // May fail if already set or not delegated — non-fatal.

        // Set memory limit (bytes).
        if let Some(ram_mb) = request.ram_mb {
            let bytes = ram_mb * 1024 * 1024;
            Self::write_param(
                &cgroup_path.join("memory.max"),
                &bytes.to_string(),
            )
            .map_err(|e| CgroupError::WriteFailed(e))?;
        }

        // Set CPU quota: cpu_percent of one core, per second.
        // cpu.max format: "QUOTA PERIOD" where QUOTA is in microseconds.
        if let Some(cpu_percent) = request.cpu_percent {
            let period = 100_000u64; // 100ms
            let quota = (cpu_percent / 100.0 * period as f32) as u64;
            Self::write_param(
                &cgroup_path.join("cpu.max"),
                &format!("{quota} {period}"),
            )
            .map_err(|e| CgroupError::WriteFailed(e))?;
        }

        Ok(cgroup_path)
    }

    /// Add a process PID to the cgroup.
    pub fn add_process(&self, allocation_id: &str, pid: u32) -> Result<(), CgroupError> {
        if !self.enabled {
            return Err(CgroupError::Disabled);
        }

        let cgroup_path = self.root.join(allocation_id);
        Self::write_param(
            &cgroup_path.join("cgroup.procs"),
            &pid.to_string(),
        )
        .map_err(|e| CgroupError::WriteFailed(e))
    }

    /// Remove and clean up a cgroup.
    pub fn remove(&self, allocation_id: &str) -> Result<(), CgroupError> {
        if !self.enabled {
            return Ok(());
        }

        let cgroup_path = self.root.join(allocation_id);

        if !cgroup_path.exists() {
            return Ok(()); // Already gone.
        }

        // Remove the directory (must be empty of processes).
        fs::remove_dir(&cgroup_path)
            .map_err(|e| CgroupError::CleanupFailed(e.to_string()))?;

        Ok(())
    }

    /// Get the cgroup path for an allocation (even if enforcement is disabled).
    pub fn path_for(&self, allocation_id: &str) -> PathBuf {
        self.root.join(allocation_id)
    }

    /// Ensure subtree_control has memory and cpu enabled on the root.
    fn ensure_subtree_control(root: &Path) {
        let control_path = root.join("cgroup.subtree_control");
        // Read current state.
        let current = fs::read_to_string(&control_path).unwrap_or_default();
        if !current.contains("memory") {
            Self::write_param(&control_path, "+memory").ok();
        }
        if !current.contains("cpu") {
            Self::write_param(&control_path, "+cpu").ok();
        }
    }

    /// Write a value to a cgroup parameter file.
    fn write_param(path: &Path, value: &str) -> Result<(), String> {
        fs::write(path, value).map_err(|e| e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_disabled_mode() {
        let mgr = CgroupManager::new(false);
        assert!(!mgr.is_enabled());

        // Operations should return Disabled error.
        let alloc = Allocation {
            id: "test".into(),
            client_id: "client".into(),
            purpose: "test".into(),
            gpus: vec![0],
            ram_mb: 1024,
            cpu_percent: 50.0,
            granted_at: chrono::Utc::now(),
            expires_at: chrono::Utc::now() + chrono::Duration::seconds(60),
            last_heartbeat: chrono::Utc::now(),
            cgroup_path: None,
            dream_cycle_id: None,
        };
        let req = AllocationRequest {
            client_id: "client".into(),
            purpose: "test".into(),
            gpus: Some(vec![0]),
            gpu_count: None,
            cpu_percent: Some(50.0),
            ram_mb: Some(1024),
            ttl_secs: 60,
            intent_id: None,
            dream_cycle_id: None,
        };

        assert!(matches!(
            mgr.create_for_allocation(&alloc, &req),
            Err(CgroupError::Disabled)
        ));
    }

    #[test]
    fn test_path_for() {
        let mgr = CgroupManager::new(false);
        let path = mgr.path_for("alloc-123");
        assert_eq!(path, PathBuf::from("/sys/fs/cgroup/riod/alloc-123"));
    }
}
