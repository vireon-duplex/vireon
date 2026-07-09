//! [`StartupShutdownCoordinator`] — orchestrates phase-ordered startup
//! and shutdown.
//!
//! See `DESIGN.md` Part 4 (startup sequence) and Part 5 (shutdown sequence).
//!
//! # Design
//!
//! The coordinator manages the transition between startup and shutdown
//! phases, ensuring that each phase completes before moving to the next.
//! During Phase 0, this is a skeleton that returns appropriate errors.

use crate::config::GracefulConfig;
use crate::error::ShutdownError;
use crate::health::HealthReport;
use crate::phase::{ShutdownPhase, StartupPhase};
use crate::stats::GracefulStats;

/// Orchestrator for phase-ordered startup and shutdown.
pub struct StartupShutdownCoordinator {
    /// Configuration for timeouts and behavior.
    cfg: GracefulConfig,
    /// Statistics tracking.
    stats: GracefulStats,
    /// Current startup phase.
    current_startup: StartupPhase,
    /// Current shutdown phase.
    #[allow(dead_code)]
    current_shutdown: ShutdownPhase,
}

impl StartupShutdownCoordinator {
    /// Create a new coordinator with the given configuration.
    #[must_use]
    pub fn new(cfg: GracefulConfig) -> Self {
        Self {
            cfg,
            stats: GracefulStats::new(),
            current_startup: StartupPhase::default(),
            current_shutdown: ShutdownPhase::default(),
        }
    }

    /// Begin the startup sequence.
    ///
    /// # Errors
    /// Returns [`ShutdownError::ShuttingDown`] if the system is
    /// already shutting down.
    pub fn startup(&mut self) -> Result<StartupPhase, ShutdownError> {
        // In Phase 0, we reject if shutting down (skeleton behavior).
        if self.current_shutdown != ShutdownPhase::Running {
            return Err(ShutdownError::ShuttingDown);
        }
        self.current_startup = StartupPhase::Init;
        Ok(self.current_startup)
    }

    /// Advance to the next startup phase.
    ///
    /// # Errors
    /// Returns [`ShutdownError::ShuttingDown`] if the system is
    /// shutting down.
    pub fn advance_startup(&mut self) -> Result<StartupPhase, ShutdownError> {
        // In Phase 0, we reject if shutting down (skeleton behavior).
        if self.current_shutdown != ShutdownPhase::Running {
            return Err(ShutdownError::ShuttingDown);
        }
        // Advance to next phase if possible.
        let next = match self.current_startup {
            StartupPhase::Init => StartupPhase::LoadConfig,
            StartupPhase::LoadConfig => StartupPhase::BindPorts,
            StartupPhase::BindPorts => StartupPhase::AcceptConnections,
            StartupPhase::AcceptConnections => StartupPhase::Ready,
            StartupPhase::Ready => StartupPhase::Ready, // Already at final phase
        };
        self.current_startup = next;
        Ok(next)
    }

    /// Begin the shutdown sequence.
    ///
    /// # Errors
    /// Returns [`ShutdownError::AlreadyShuttingDown`] if shutdown is
    /// already in progress.
    pub fn begin_shutdown(&mut self) -> Result<ShutdownPhase, ShutdownError> {
        // In Phase 0, we reject if already shutting down.
        if self.current_shutdown != ShutdownPhase::Running {
            return Err(ShutdownError::AlreadyShuttingDown);
        }
        self.current_shutdown = ShutdownPhase::StopAccepting;
        Ok(self.current_shutdown)
    }

    /// Advance to the next shutdown phase.
    ///
    /// # Errors
    /// Returns [`ShutdownError::ShuttingDown`] if the system is
    /// shutting down (this is a skeleton error).
    pub fn advance_shutdown(&mut self) -> Result<ShutdownPhase, ShutdownError> {
        // In Phase 0, we return an error to indicate skeleton behavior.
        if self.current_shutdown != ShutdownPhase::Running {
            // This is the skeleton implementation - normally would advance
        }
        Err(ShutdownError::ShuttingDown)
    }

    /// Get the current startup phase.
    #[must_use]
    pub const fn current_startup_phase(&self) -> StartupPhase {
        self.current_startup
    }

    /// Get the current shutdown phase.
    #[must_use]
    pub const fn current_shutdown_phase(&self) -> ShutdownPhase {
        self.current_shutdown
    }

    /// Get a health report for the coordinator.
    #[must_use]
    pub fn health(&self) -> HealthReport {
        let snapshot = self.stats.snapshot();
        let shutdowns_in_progress = if self.current_shutdown != ShutdownPhase::Running {
            1
        } else {
            0
        };
        HealthReport::derive(&snapshot, shutdowns_in_progress)
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
    fn new_coordinator_starts_at_init() {
        let coordinator = StartupShutdownCoordinator::new(GracefulConfig::new());
        assert_eq!(coordinator.current_startup_phase(), StartupPhase::Init);
        assert_eq!(coordinator.current_shutdown_phase(), ShutdownPhase::Running);
    }

    #[test]
    fn startup_begins_startup_sequence() {
        let mut coordinator = StartupShutdownCoordinator::new(GracefulConfig::new());
        let phase = coordinator.startup().unwrap();
        assert_eq!(phase, StartupPhase::Init);
    }

    #[test]
    fn startup_fails_when_shutting_down() {
        let mut coordinator = StartupShutdownCoordinator::new(GracefulConfig::new());
        // Begin shutdown first
        let _ = coordinator.begin_shutdown();
        // Now startup should fail
        assert!(coordinator.startup().is_err());
    }

    #[test]
    fn advance_startup_progresses_phases() {
        let mut coordinator = StartupShutdownCoordinator::new(GracefulConfig::new());
        let _ = coordinator.startup();

        assert_eq!(
            coordinator.advance_startup().unwrap(),
            StartupPhase::LoadConfig
        );
        assert_eq!(
            coordinator.advance_startup().unwrap(),
            StartupPhase::BindPorts
        );
        assert_eq!(
            coordinator.advance_startup().unwrap(),
            StartupPhase::AcceptConnections
        );
        assert_eq!(coordinator.advance_startup().unwrap(), StartupPhase::Ready);
    }

    #[test]
    fn advance_startup_stays_at_ready() {
        let mut coordinator = StartupShutdownCoordinator::new(GracefulConfig::new());
        let _ = coordinator.startup();
        let _ = coordinator.advance_startup();
        let _ = coordinator.advance_startup();
        let _ = coordinator.advance_startup();
        let _ = coordinator.advance_startup();

        // Already at Ready, should stay there
        assert_eq!(coordinator.advance_startup().unwrap(), StartupPhase::Ready);
    }

    #[test]
    fn advance_startup_fails_when_shutting_down() {
        let mut coordinator = StartupShutdownCoordinator::new(GracefulConfig::new());
        let _ = coordinator.startup();
        let _ = coordinator.begin_shutdown(); // This will fail but sets shutdown phase

        // Now advance_startup should fail
        assert!(coordinator.advance_startup().is_err());
    }

    #[test]
    fn begin_shutdown_starts_shutdown_sequence() {
        let mut coordinator = StartupShutdownCoordinator::new(GracefulConfig::new());
        let phase = coordinator.begin_shutdown().unwrap();
        assert_eq!(phase, ShutdownPhase::StopAccepting);
    }

    #[test]
    fn begin_shutdown_fails_if_already_shutting_down() {
        let mut coordinator = StartupShutdownCoordinator::new(GracefulConfig::new());
        let _ = coordinator.begin_shutdown();
        assert!(coordinator.begin_shutdown().is_err());
    }

    #[test]
    fn advance_shutdown_returns_skeleton_error() {
        let mut coordinator = StartupShutdownCoordinator::new(GracefulConfig::new());
        let _ = coordinator.begin_shutdown();
        // In Phase 0, this returns ShuttingDown error
        assert!(matches!(
            coordinator.advance_shutdown(),
            Err(ShutdownError::ShuttingDown)
        ));
    }

    #[test]
    fn health_report_is_healthy_initially() {
        let coordinator = StartupShutdownCoordinator::new(GracefulConfig::new());
        let health = coordinator.health();
        assert!(health.is_healthy());
    }

    #[test]
    fn health_report_shows_shutdown_in_progress() {
        let mut coordinator = StartupShutdownCoordinator::new(GracefulConfig::new());
        let _ = coordinator.begin_shutdown();
        let health = coordinator.health();
        assert_eq!(health.shutdowns_in_progress, 1);
    }

    #[test]
    fn stats_returns_reference() {
        let coordinator = StartupShutdownCoordinator::new(GracefulConfig::new());
        let stats = coordinator.stats();
        assert_eq!(stats.snapshot().startups_total, 0);
    }

    #[test]
    fn config_returns_reference() {
        let coordinator = StartupShutdownCoordinator::new(GracefulConfig::new());
        let cfg = coordinator.config();
        assert_eq!(cfg.startup_timeout_ms, 30_000);
    }

    #[test]
    fn coordinator_with_custom_config() {
        let cfg = GracefulConfig::new().with_startup_timeout_ms(45_000);
        let coordinator = StartupShutdownCoordinator::new(cfg);
        assert_eq!(coordinator.config().startup_timeout_ms, 45_000);
    }
}
