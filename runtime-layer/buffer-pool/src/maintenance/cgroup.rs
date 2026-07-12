//! cgroup-aware memory detection for container deployments (Layer 23).
//!
//! Production servers run inside containers (Docker, Kubernetes) that
//! enforce memory limits via **cgroups**. If the buffer pool is sized
//! larger than the cgroup limit, the kernel OOM-kills the process
//! before the pool's own backpressure can react.
//!
//! This module reads the cgroup limit at startup so the buffer pool
//! can auto-size, and reads `memory.current` at runtime so the
//! pressure monitor can detect real OOM proximity.
//!
//! ## cgroup v2 (unified hierarchy — default since Linux 4.15 / systemd v240)
//!
//! | File | Content |
//! |-----|---------|
//! | `/sys/fs/cgroup/memory.max` | Limit in bytes, or `max` for unlimited |
//! | `/sys/fs/cgroup/memory.current` | Current usage in bytes |
//!
//! ## cgroup v1 (legacy hierarchy)
//!
//! | File | Content |
//! |-----|---------|
//! | `/sys/fs/cgroup/memory/memory.limit_in_bytes` | Limit |
//! | `/sys/fs/cgroup/memory/memory.usage_in_bytes` | Current usage |

use crate::buffer::config::DEFAULT_MAX_MEMORY;
use crate::buffer::policy::PressureLevel;
use crate::maintenance::pressure::{classify_pressure, resident_memory};

/// Fraction of the cgroup memory limit allocated to the buffer pool.
///
/// The remaining memory covers quiche connection state (~100 KiB/conn),
/// carryover buffers, session/stream layer state, Rust runtime, thread
/// stacks, and code/static data.
const POOL_BUDGET_FRACTION: f64 = 0.50;

/// Clamp the auto-computed budget to avoid degenerate cases.
const MIN_POOL_BUDGET: usize = 64 << 20; // 64 MiB
const MAX_POOL_BUDGET: usize = 2 << 30; // 2 GiB

/// cgroup v1 reports a very large value (typically `LONG_MAX` ≈ 8 EiB)
/// when the limit is "unlimited". Anything above 1 TiB is treated as
/// unlimited — real container limits are always much smaller.
const UNLIMITED_THRESHOLD: usize = 1 << 40; // 1 TiB

/// Detects the cgroup memory limit in bytes.
///
/// Tries cgroup v2 first, then v1. Returns `None` when not running
/// inside a cgroup, when the files are unreadable, or when the limit
/// is "unlimited" (no effective cap).
#[must_use]
pub fn cgroup_memory_limit() -> Option<usize> {
    read_cgroup_v2_limit()
        .or_else(read_cgroup_v1_limit)
        .filter(|&limit| limit < UNLIMITED_THRESHOLD)
}

/// Reads the current memory usage according to the cgroup.
///
/// Falls back to VmRSS (`resident_memory`) when cgroup files are
/// unavailable. Returns the most accurate available measure of
/// "how much memory this process is actually using right now".
#[must_use]
pub fn cgroup_memory_usage() -> Option<usize> {
    read_cgroup_v2_current()
        .or_else(read_cgroup_v1_usage)
        .or_else(resident_memory)
}

/// Classifies real memory pressure by comparing cgroup current usage
/// against the cgroup limit.
///
/// Unlike [`BufferPool::pressure_level`](crate::BufferPool::pressure_level)
/// (which compares pool-internal bytes against the pool budget), this
/// reflects how close the **entire process** is to being OOM-killed
/// by the kernel.
///
/// Returns [`PressureLevel::Low`] when cgroup files are unavailable
/// (no signal — the caller should rely on pool-internal pressure instead).
#[must_use]
pub fn cgroup_pressure_level() -> PressureLevel {
    match (cgroup_memory_usage(), cgroup_memory_limit()) {
        (Some(used), Some(max)) => classify_pressure(used, max),
        _ => PressureLevel::Low,
    }
}

/// Computes a safe buffer-pool budget from the detected cgroup limit.
///
/// Uses 50% of the cgroup limit, clamped to [64 MiB, 2 GiB].
/// Falls back to [`DEFAULT_MAX_MEMORY`] (256 MiB) when no cgroup
/// limit is detected (bare-metal dev machine without containers).
#[must_use]
pub fn recommended_pool_budget() -> usize {
    match cgroup_memory_limit() {
        Some(limit) => {
            let budget = (limit as f64 * POOL_BUDGET_FRACTION) as usize;
            budget.clamp(MIN_POOL_BUDGET, MAX_POOL_BUDGET)
        }
        None => DEFAULT_MAX_MEMORY,
    }
}

// ── cgroup v2 readers ──────────────────────────────────────────

#[cfg(target_os = "linux")]
fn read_cgroup_v2_limit() -> Option<usize> {
    let content = std::fs::read_to_string("/sys/fs/cgroup/memory.max").ok()?;
    let trimmed = content.trim();
    // v2 reports "max" for unlimited.
    if trimmed.eq_ignore_ascii_case("max") {
        return None;
    }
    trimmed.parse().ok()
}

#[cfg(target_os = "linux")]
fn read_cgroup_v2_current() -> Option<usize> {
    std::fs::read_to_string("/sys/fs/cgroup/memory.current")
        .ok()?
        .trim()
        .parse()
        .ok()
}

// ── cgroup v1 readers ──────────────────────────────────────────

#[cfg(target_os = "linux")]
fn read_cgroup_v1_limit() -> Option<usize> {
    std::fs::read_to_string("/sys/fs/cgroup/memory/memory.limit_in_bytes")
        .ok()?
        .trim()
        .parse()
        .ok()
}

#[cfg(target_os = "linux")]
fn read_cgroup_v1_usage() -> Option<usize> {
    std::fs::read_to_string("/sys/fs/cgroup/memory/memory.usage_in_bytes")
        .ok()?
        .trim()
        .parse()
        .ok()
}

// ── non-Linux stubs ────────────────────────────────────────────

#[cfg(not(target_os = "linux"))]
fn read_cgroup_v2_limit() -> Option<usize> {
    None
}

#[cfg(not(target_os = "linux"))]
fn read_cgroup_v2_current() -> Option<usize> {
    None
}

#[cfg(not(target_os = "linux"))]
fn read_cgroup_v1_limit() -> Option<usize> {
    None
}

#[cfg(not(target_os = "linux"))]
fn read_cgroup_v1_usage() -> Option<usize> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recommended_pool_budget_returns_default_without_cgroup() {
        // On bare metal (no cgroup limit detected), falls back to default.
        let budget = recommended_pool_budget();
        assert!(budget >= 64 << 20, "budget should be at least 64 MiB");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn cgroup_detection_runs_without_panic() {
        // These should never panic, even if the files don't exist.
        let _ = cgroup_memory_limit();
        let _ = cgroup_memory_usage();
        let _ = cgroup_pressure_level();
    }

    #[test]
    fn unlimited_threshold_is_reasonable() {
        // 1 TiB — anything above this is "unlimited" for practical purposes.
        assert_eq!(UNLIMITED_THRESHOLD, 1_099_511_627_776);
    }
}
