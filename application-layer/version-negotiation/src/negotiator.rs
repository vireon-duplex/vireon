//! [`VersionNegotiator`] — the main version negotiation facade.
//!
//! See `DESIGN.md` Part 1 (architecture), Part 9 (negotiator).
//!
//! # Phase 0
//!
//! Phase 0 ships only a typed skeleton. Every method returns a stub
//! error so that callers can code against the API contract today.
//! Phase 1 fills in the real logic.

use crate::config::VersionNegotiationConfig;
use crate::error::VersionNegotiationError;
use crate::health::HealthReport;
use crate::range::VersionRange;
use crate::stats::VersionNegotiationStats;
use crate::version::ProtocolVersion;

/// The version negotiator facade.
///
/// Composes the config, compatibility matrix, and stats.
/// Phase 0 ships a typed skeleton; Phase 1+ populates real logic.
#[derive(Debug)]
pub struct VersionNegotiator {
    /// Static configuration.
    #[allow(dead_code)]
    cfg: VersionNegotiationConfig,
    /// Atomic observability counters.
    stats: VersionNegotiationStats,
}

impl VersionNegotiator {
    /// Construct a negotiator from validated configuration.
    pub fn new(cfg: VersionNegotiationConfig) -> Result<Self, VersionNegotiationError> {
        cfg.validate()?;
        Ok(Self {
            cfg,
            stats: VersionNegotiationStats::new(),
        })
    }

    /// Negotiate the best compatible version.
    ///
    /// # Errors
    /// Always returns an error in Phase 0.
    pub fn negotiate(
        &self,
        _local_range: VersionRange,
        _remote_versions: &[ProtocolVersion],
    ) -> Result<ProtocolVersion, VersionNegotiationError> {
        // TODO(phase-1): version range intersection + compatibility check.
        self.stats.record_negotiation(false);
        Err(VersionNegotiationError::ShuttingDown)
    }

    /// Select a fallback version when negotiation fails.
    ///
    /// # Errors
    /// Always returns an error in Phase 0.
    pub fn select_fallback(&self) -> Result<ProtocolVersion, VersionNegotiationError> {
        // TODO(phase-1): return lowest supported version.
        self.stats.record_fallback();
        Err(VersionNegotiationError::ShuttingDown)
    }

    /// Snapshot the negotiator's health.
    pub fn health(&self) -> HealthReport {
        HealthReport::derive(&self.stats.snapshot())
    }

    /// Reference to the underlying stats (for testing / metrics).
    pub fn stats(&self) -> &VersionNegotiationStats {
        &self.stats
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Preset;

    fn make_negotiator() -> VersionNegotiator {
        let cfg = Preset::Strict.to_config().unwrap();
        VersionNegotiator::new(cfg).unwrap()
    }

    #[test]
    fn negotiator_constructs_from_valid_config() {
        let neg = make_negotiator();
        let h = neg.health();
        assert!(h.is_healthy());
    }

    #[test]
    fn negotiate_phase0_stub() {
        let neg = make_negotiator();
        let range = VersionRange::default();
        let versions = vec![ProtocolVersion::STABLE_1_0];
        assert!(neg.negotiate(range, &versions).is_err());
    }

    #[test]
    fn select_fallback_phase0_stub() {
        let neg = make_negotiator();
        assert!(neg.select_fallback().is_err());
    }

    #[test]
    fn health_snapshot_works() {
        let neg = make_negotiator();
        let report = neg.health();
        assert_eq!(report.state, crate::health::HealthState::Healthy);
    }
}
