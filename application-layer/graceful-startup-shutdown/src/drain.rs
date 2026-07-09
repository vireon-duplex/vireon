//! [`ConnectionDrainer`] — manages connection draining during shutdown.
//!
//! See `DESIGN.md` Part 7 (connection draining).
//!
//! # Design
//!
//! The drainer tracks active connections and ensures they are given
//! time to complete gracefully before forcing them closed. During
//! Phase 0, this is a skeleton that returns appropriate errors.

use crate::config::GracefulConfig;
use crate::error::ShutdownError;
use crate::stats::GracefulStats;

/// Manager for connection draining during shutdown.
pub struct ConnectionDrainer {
    /// Configuration for drain timeout and behavior.
    cfg: GracefulConfig,
    /// Statistics tracking.
    stats: GracefulStats,
    /// Number of active connections.
    #[allow(dead_code)]
    active_connections: u64,
}

impl ConnectionDrainer {
    /// Create a new connection drainer with the given configuration.
    #[must_use]
    pub fn new(cfg: GracefulConfig) -> Self {
        Self {
            cfg,
            stats: GracefulStats::new(),
            active_connections: 0,
        }
    }

    /// Begin draining connections.
    ///
    /// # Errors
    /// Returns [`ShutdownError::ShuttingDown`] if the system is
    /// shutting down (skeleton behavior).
    ///
    /// # Returns
    /// The number of active connections being drained.
    pub fn begin_drain(&mut self) -> Result<u64, ShutdownError> {
        // In Phase 0, this is a skeleton that returns an error.
        Err(ShutdownError::ShuttingDown)
    }

    /// Perform a single drain step.
    ///
    /// # Errors
    /// Returns [`ShutdownError::ShuttingDown`] if the system is
    /// shutting down (skeleton behavior).
    ///
    /// # Returns
    /// The number of remaining connections.
    pub fn drain_step(&mut self) -> Result<u64, ShutdownError> {
        // In Phase 0, this is a skeleton that returns an error.
        Err(ShutdownError::ShuttingDown)
    }

    /// Check if all connections have been drained.
    ///
    /// # Returns
    /// `true` if no connections remain, `false` otherwise.
    #[must_use]
    pub fn is_drained(&self) -> bool {
        // In Phase 0, this always returns false (skeleton behavior).
        false
    }

    /// Force close all remaining connections.
    ///
    /// # Errors
    /// Returns [`ShutdownError::ShuttingDown`] if the system is
    /// shutting down (skeleton behavior).
    ///
    /// # Returns
    /// The number of connections that were closed.
    pub fn force_close(&mut self) -> Result<u64, ShutdownError> {
        // In Phase 0, this is a skeleton that returns an error.
        Err(ShutdownError::ShuttingDown)
    }

    /// Get the number of active connections.
    #[must_use]
    pub const fn active_connections(&self) -> u64 {
        self.active_connections
    }

    /// Get a reference to the statistics.
    #[must_use]
    pub const fn stats(&self) -> &GracefulStats {
        &self.stats
    }

    /// Get a reference to the configuration.
    #[must_use]
    pub const fn config(&self) -> &GracefulConfig {
        &self.cfg
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::GracefulConfig;

    #[test]
    fn new_drainer_has_no_active_connections() {
        let drainer = ConnectionDrainer::new(GracefulConfig::new());
        assert_eq!(drainer.active_connections(), 0);
    }

    #[test]
    fn begin_drain_returns_skeleton_error() {
        let mut drainer = ConnectionDrainer::new(GracefulConfig::new());
        assert!(matches!(
            drainer.begin_drain(),
            Err(ShutdownError::ShuttingDown)
        ));
    }

    #[test]
    fn drain_step_returns_skeleton_error() {
        let mut drainer = ConnectionDrainer::new(GracefulConfig::new());
        assert!(matches!(
            drainer.drain_step(),
            Err(ShutdownError::ShuttingDown)
        ));
    }

    #[test]
    fn is_drained_returns_false() {
        let drainer = ConnectionDrainer::new(GracefulConfig::new());
        assert!(!drainer.is_drained());
    }

    #[test]
    fn force_close_returns_skeleton_error() {
        let mut drainer = ConnectionDrainer::new(GracefulConfig::new());
        assert!(matches!(
            drainer.force_close(),
            Err(ShutdownError::ShuttingDown)
        ));
    }

    #[test]
    fn stats_returns_reference() {
        let drainer = ConnectionDrainer::new(GracefulConfig::new());
        let stats = drainer.stats();
        assert_eq!(stats.snapshot().connections_drained, 0);
    }

    #[test]
    fn config_returns_reference() {
        let drainer = ConnectionDrainer::new(GracefulConfig::new());
        let cfg = drainer.config();
        assert_eq!(cfg.drain_timeout_ms, 10_000);
    }

    #[test]
    fn drainer_with_custom_config() {
        let cfg = GracefulConfig::new().with_drain_timeout_ms(20_000);
        let drainer = ConnectionDrainer::new(cfg);
        assert_eq!(drainer.config().drain_timeout_ms, 20_000);
    }

    #[test]
    fn active_connections_getter() {
        let drainer = ConnectionDrainer::new(GracefulConfig::new());
        assert_eq!(drainer.active_connections(), 0);
    }
}
