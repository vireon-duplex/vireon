//! [`AdvertisedVersions`] — list of advertised protocol versions.
//!
//! See `DESIGN.md` Part 10 (advertised versions).
//!
//! # Phase 0
//!
//! Phase 0 ships only a typed skeleton. Every method returns a stub
//! error so that callers can code against the API contract today.
//! Phase 1 fills in the real logic.

use crate::config::VersionNegotiationConfig;
use crate::error::VersionNegotiationError;
use crate::health::{HealthReport, HealthState};
use crate::version::ProtocolVersion;

/// List of advertised protocol versions.
///
/// Maintains a list of versions that a node advertises to peers.
/// Phase 0 ships a typed skeleton; Phase 1+ populates real logic.
#[derive(Debug)]
pub struct AdvertisedVersions {
    /// Static configuration.
    #[allow(dead_code)]
    cfg: VersionNegotiationConfig,
    /// Number of advertised versions.
    #[allow(dead_code)]
    count: u32,
}

impl AdvertisedVersions {
    /// Construct advertised versions from configuration.
    pub fn new(cfg: VersionNegotiationConfig) -> Self {
        Self { cfg, count: 0 }
    }

    /// Add a version to the advertised list.
    ///
    /// # Errors
    /// Always returns an error in Phase 0.
    pub fn add(&mut self, _version: ProtocolVersion) -> Result<(), VersionNegotiationError> {
        // TODO(phase-1): add to list, validate against config.
        Err(VersionNegotiationError::ShuttingDown)
    }

    /// List all advertised versions.
    ///
    /// # Errors
    /// Always returns an error in Phase 0.
    pub fn list(&self) -> Result<Vec<ProtocolVersion>, VersionNegotiationError> {
        // TODO(phase-1): return vector of advertised versions.
        Err(VersionNegotiationError::ShuttingDown)
    }

    /// Get the health status.
    pub fn health(&self) -> HealthReport {
        HealthReport {
            state: HealthState::Healthy,
            negotiations_total: 0,
            success_rate_bps: 10_000,
            fallback_rate_bps: 0,
            deprecated_warnings: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Preset;

    #[test]
    fn advertised_versions_constructs() {
        let cfg = Preset::Strict.to_config().unwrap();
        let adv = AdvertisedVersions::new(cfg);
        assert_eq!(adv.count, 0);
    }

    #[test]
    fn add_phase0_stub() {
        let cfg = Preset::Strict.to_config().unwrap();
        let mut adv = AdvertisedVersions::new(cfg);
        assert!(adv.add(ProtocolVersion::STABLE_1_0).is_err());
    }

    #[test]
    fn list_phase0_stub() {
        let cfg = Preset::Strict.to_config().unwrap();
        let adv = AdvertisedVersions::new(cfg);
        assert!(adv.list().is_err());
    }

    #[test]
    fn health_snapshot_works() {
        let cfg = Preset::Strict.to_config().unwrap();
        let adv = AdvertisedVersions::new(cfg);
        let report = adv.health();
        assert_eq!(report.state, HealthState::Healthy);
    }
}
