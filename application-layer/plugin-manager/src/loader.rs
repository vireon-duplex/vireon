//! [`PluginLoader`] — plugin discovery and loading.
//!
//! See `DESIGN.md` Part 8 (loading lifecycle), Part 9 (hot reload).
//!
//! # Phase 0
//!
//! Phase 0 ships a typed skeleton with real API version checking.
//! Discovery and loading logic land in Phase 1+.

use crate::api_version::CURRENT_API;
use crate::config::PluginManagerConfig;
use crate::descriptor::PluginDescriptor;
use crate::error::PluginError;

/// The plugin loader (discover + load).
///
/// Phase 0 ships a typed skeleton with real API compatibility checking.
/// Phase 1+ populates discovery and loading logic.
#[derive(Debug)]
pub struct PluginLoader {
    /// Static configuration.
    #[allow(dead_code)]
    cfg: PluginManagerConfig,
    /// Hash of the scan path for change detection.
    #[allow(dead_code)]
    scan_path_hash: u64,
}

impl PluginLoader {
    /// Construct a loader from configuration and scan path.
    #[must_use]
    pub const fn new(cfg: PluginManagerConfig, scan_path_hash: u64) -> Self {
        Self {
            cfg,
            scan_path_hash,
        }
    }

    /// Discover plugins in the configured scan path.
    ///
    /// Returns the count of discovered plugins.
    ///
    /// # Errors
    /// Always returns an error in Phase 0.
    pub fn discover(&mut self) -> Result<u32, PluginError> {
        // TODO(phase-1): scan path + load descriptors.
        Err(PluginError::ShuttingDown)
    }

    /// Load a plugin from its path hash.
    ///
    /// Returns the plugin descriptor on success.
    ///
    /// # Errors
    /// Always returns an error in Phase 0.
    pub fn load_path(&mut self, _path_hash: u64) -> Result<PluginDescriptor, PluginError> {
        // TODO(phase-1): load binary + extract descriptor.
        Err(PluginError::ShuttingDown)
    }

    /// Check if a plugin's API version is compatible.
    ///
    /// Returns `Ok(true)` if compatible, `Ok(false)` if not but
    /// strict checking is disabled, or `Err` if the mismatch is
    /// fatal (strict mode enabled).
    ///
    /// # Errors
    /// Returns [`PluginError::ApiMismatch`] if strict checking is
    /// enabled and the versions are incompatible.
    pub fn check_api(&self, desc: &PluginDescriptor) -> Result<bool, PluginError> {
        let compatible = CURRENT_API.is_backward_compatible_with(&desc.api_version);
        if !compatible && self.cfg.strict_api_check {
            return Err(PluginError::ApiMismatch(
                desc.api_version.major,
                CURRENT_API.major,
            ));
        }
        Ok(compatible)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api_version::ApiVersion;
    use crate::config::Preset;
    use crate::id::{PluginId, PluginVersion};

    fn make_loader() -> PluginLoader {
        let cfg = Preset::Strict.to_config();
        PluginLoader::new(cfg, 42)
    }

    fn make_desc(api_major: u16, api_minor: u8) -> PluginDescriptor {
        PluginDescriptor {
            id: PluginId::nil(),
            version: PluginVersion::new(1, 0, 0),
            name: [0u8; 64],
            name_len: 0,
            api_version: ApiVersion::new(api_major, api_minor),
            dependencies: [PluginId::nil(); 8],
            dep_count: 0,
            capabilities: 0,
        }
    }

    #[test]
    fn loader_constructs() {
        let loader = make_loader();
        assert_eq!(loader.scan_path_hash, 42);
    }

    #[test]
    fn discover_phase0_stub() {
        let mut loader = make_loader();
        assert!(loader.discover().is_err());
    }

    #[test]
    fn load_path_phase0_stub() {
        let mut loader = make_loader();
        assert!(loader.load_path(123).is_err());
    }

    #[test]
    fn check_api_compatible_same_version() {
        let loader = make_loader();
        let desc = make_desc(1, 0); // Same as CURRENT_API
        assert!(loader.check_api(&desc).is_ok());
        assert_eq!(loader.check_api(&desc).unwrap(), true);
    }

    #[test]
    fn check_api_compatible_runtime_higher_minor() {
        let loader = make_loader();
        let desc = make_desc(1, 0); // Runtime is 1.0, plugin needs 1.0
        assert!(loader.check_api(&desc).is_ok());
    }

    #[test]
    fn check_api_incompatible_plugin_higher_minor() {
        let loader = make_loader();
        let desc = make_desc(1, 5); // Plugin needs 1.5, runtime is 1.0
        let result = loader.check_api(&desc);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            PluginError::ApiMismatch(1, 1)
        ));
    }

    #[test]
    fn check_api_incompatible_different_major() {
        let loader = make_loader();
        let desc = make_desc(2, 0); // Plugin needs 2.0, runtime is 1.0
        let result = loader.check_api(&desc);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            PluginError::ApiMismatch(2, 1)
        ));
    }

    #[test]
    fn check_api_lenient_mode_returns_false() {
        let cfg = Preset::Lenient.to_config(); // strict_api_check = false
        let loader = PluginLoader::new(cfg, 42);
        let desc = make_desc(1, 5); // Incompatible but lenient
        let result = loader.check_api(&desc);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), false);
    }

    #[test]
    fn check_api_strict_mode_returns_error() {
        let cfg = Preset::Strict.to_config(); // strict_api_check = true
        let loader = PluginLoader::new(cfg, 42);
        let desc = make_desc(1, 5); // Incompatible and strict
        assert!(loader.check_api(&desc).is_err());
    }
}
