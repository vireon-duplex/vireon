//! [`ConfigWatcher`] — configuration hot reload watcher.
//!
//! See `DESIGN.md` Part 11 (hot reload).
//!
//! # Phase 0
//!
//! Phase 0 ships only a typed skeleton. Every method returns a stub
//! error so that callers can code against the API contract today.
//! Phase 1 fills in the real logic.

use crate::error::ConfigError;
use crate::health::{HealthReport, HealthState};
use crate::stats::ConfigStats;

/// Configuration watcher for hot reload.
///
/// Monitors configuration sources for changes and triggers reloads.
/// Phase 0 ships a typed skeleton; Phase 1+ populates real logic.
#[derive(Debug)]
pub struct ConfigWatcher {
    /// Poll interval in milliseconds.
    #[allow(dead_code)]
    interval_ms: u64,
    /// Stats for watcher operations.
    stats: ConfigStats,
}

impl ConfigWatcher {
    /// Construct a watcher with the given poll interval.
    #[must_use]
    pub const fn new(interval_ms: u64) -> Self {
        Self {
            interval_ms,
            stats: ConfigStats::new(),
        }
    }

    /// Poll for configuration changes.
    ///
    /// Returns the number of changes detected.
    ///
    /// # Errors
    /// Always returns an error in Phase 0.
    pub fn poll(&mut self) -> Result<u64, ConfigError> {
        // TODO(phase-1): check sources for changes, trigger reloads.
        self.stats.record_watch_event();
        Err(ConfigError::ShuttingDown)
    }

    /// Snapshot the watcher's health.
    #[must_use]
    pub fn health(&self) -> HealthReport {
        HealthReport {
            state: HealthState::Healthy,
            reads_total: self.stats.snapshot().reads_total,
            hit_rate_bps: 10_000,
            parse_errors: 0,
            reloads: 0,
        }
    }

    /// Reference to the underlying stats (for testing / metrics).
    #[must_use]
    pub fn stats(&self) -> &ConfigStats {
        &self.stats
    }
}

impl Default for ConfigWatcher {
    fn default() -> Self {
        Self::new(5_000)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn watcher_constructs_with_interval() {
        let watcher = ConfigWatcher::new(10_000);
        assert_eq!(watcher.interval_ms, 10_000);
    }

    #[test]
    fn default_watcher_has_5s_interval() {
        let watcher = ConfigWatcher::default();
        assert_eq!(watcher.interval_ms, 5_000);
    }

    #[test]
    fn poll_phase0_stub() {
        let mut watcher = ConfigWatcher::new(1_000);
        assert!(watcher.poll().is_err());
    }

    #[test]
    fn health_snapshot_works() {
        let watcher = ConfigWatcher::new(1_000);
        let report = watcher.health();
        assert_eq!(report.state, HealthState::Healthy);
    }

    #[test]
    fn stats_are_accessible() {
        let watcher = ConfigWatcher::new(1_000);
        let stats = watcher.stats();
        let snap = stats.snapshot();
        assert_eq!(snap.reads_total, 0);
    }
}
