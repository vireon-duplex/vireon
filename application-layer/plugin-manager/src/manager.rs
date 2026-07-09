//! [`PluginManager`] — the main plugin manager facade.
//!
//! See `DESIGN.md` Part 1 (architecture), Part 8 (loading lifecycle),
//! Part 10 (failure modes).
//!
//! # Phase 0
//!
//! Phase 0 ships only a typed skeleton. Every method returns a
//! stub error so that callers can code against the API contract
//! today. Phase 1 fills in the real logic.

use crate::config::PluginManagerConfig;
use crate::descriptor::PluginDescriptor;
use crate::error::PluginError;
use crate::health::HealthReport;
use crate::id::PluginId;
use crate::stats::PluginManagerStats;

/// The plugin manager facade.
///
/// Composes the loader, stats, and health tracking. Phase 0 ships a
/// typed skeleton; Phase 1+ populates real logic.
#[derive(Debug)]
pub struct PluginManager {
    /// Static configuration.
    #[allow(dead_code)]
    cfg: PluginManagerConfig,
    /// Atomic observability counters.
    stats: PluginManagerStats,
}

impl PluginManager {
    /// Construct a manager from validated configuration.
    ///
    /// # Errors
    /// Returns [`PluginError::LoadFailed`] if the config fails
    /// validation.
    pub fn new(cfg: PluginManagerConfig) -> Result<Self, PluginError> {
        cfg.validate()?;
        Ok(Self {
            cfg,
            stats: PluginManagerStats::new(),
        })
    }

    /// Load a plugin from its descriptor.
    ///
    /// # Errors
    /// Always returns an error in Phase 0.
    pub fn load(&mut self, _desc: PluginDescriptor) -> Result<(), PluginError> {
        // TODO(phase-1): dependency check + API check + load.
        Err(PluginError::ShuttingDown)
    }

    /// Unload a plugin by ID.
    ///
    /// # Errors
    /// Always returns [`PluginError::NotFound`] in Phase 0.
    pub fn unload(&mut self, id: PluginId) -> Result<(), PluginError> {
        // TODO(phase-1): unload + cascade to dependents.
        Err(PluginError::NotFound(id))
    }

    /// Reload a plugin (hot reload).
    ///
    /// # Errors
    /// Always returns [`PluginError::NotFound`] in Phase 0.
    pub fn reload(&mut self, id: PluginId) -> Result<(), PluginError> {
        // TODO(phase-1): unload + load + update dependents.
        Err(PluginError::NotFound(id))
    }

    /// Get a plugin's descriptor by ID.
    ///
    /// # Errors
    /// Always returns [`PluginError::NotFound`] in Phase 0.
    pub fn get(&self, id: PluginId) -> Result<PluginDescriptor, PluginError> {
        // TODO(phase-1): lookup in loaded plugins map.
        Err(PluginError::NotFound(id))
    }

    /// Snapshot the manager's health.
    #[must_use]
    pub fn health(&self) -> HealthReport {
        HealthReport::derive(&self.stats.snapshot())
    }

    /// Reference to the underlying stats (for testing / metrics).
    #[must_use]
    pub fn stats(&self) -> &PluginManagerStats {
        &self.stats
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Preset;

    fn make_manager() -> PluginManager {
        let cfg = Preset::Strict.to_config();
        PluginManager::new(cfg).unwrap()
    }

    #[test]
    fn manager_constructs_from_valid_config() {
        let manager = make_manager();
        let h = manager.health();
        assert!(h.is_healthy());
    }

    #[test]
    fn manager_rejects_invalid_config() {
        let cfg = Preset::Strict.to_config();
        let mut invalid = cfg;
        invalid.max_plugins = 0;
        assert!(PluginManager::new(invalid).is_err());
    }

    #[test]
    fn load_phase0_stub() {
        let mut manager = make_manager();
        let desc = PluginDescriptor::new(PluginId::nil(), crate::id::PluginVersion::new(1, 0, 0));
        assert!(manager.load(desc).is_err());
    }

    #[test]
    fn unload_phase0_stub() {
        let mut manager = make_manager();
        assert!(manager.unload(PluginId::nil()).is_err());
    }

    #[test]
    fn reload_phase0_stub() {
        let mut manager = make_manager();
        assert!(manager.reload(PluginId::nil()).is_err());
    }

    #[test]
    fn get_phase0_stub() {
        let manager = make_manager();
        assert!(manager.get(PluginId::nil()).is_err());
    }

    #[test]
    fn health_returns_healthy_by_default() {
        let manager = make_manager();
        let health = manager.health();
        assert_eq!(health.state, crate::health::HealthState::Healthy);
    }

    #[test]
    fn stats_returns_stats_reference() {
        let manager = make_manager();
        let stats = manager.stats();
        assert_eq!(stats.snapshot().plugins_loaded, 0);
    }
}
