//! [`FlagProvider`], [`StaticProvider`] — trait and skeleton for flag
//! state sources.
//!
//! # Phase 0
//!
//! Phase 0 ships only a typed skeleton. The trait defines the API
//! contract; the static provider stubs out all methods. Phase 1+
//! fills in real logic.

use crate::config::FeatureFlagsConfig;
use crate::error::FlagError;
use crate::flag::FlagKey;
use crate::flag::FlagState;

/// Trait for flag state providers.
///
/// A provider supplies flag states from some source (file, database,
/// remote service, etc.). Phase 0 defines the contract; Phase 1+
/// implements real providers.
pub trait FlagProvider: Send + Sync {
    /// Get the current state of a flag.
    ///
    /// # Errors
    /// Returns `FlagError::NotFound` if the flag doesn't exist,
    /// or `FlagError::ShuttingDown` if the provider is shutting down.
    fn get(&self, key: FlagKey) -> Result<FlagState, FlagError>;
}

/// Static flag provider stub.
///
/// Phase 0 ships a skeleton that always returns an error.
/// Phase 1+ will load flags from a static source (file, config map).
#[derive(Debug)]
pub struct StaticProvider {
    /// Static configuration.
    #[allow(dead_code)]
    cfg: FeatureFlagsConfig,
}

impl StaticProvider {
    /// Construct a new static provider.
    #[must_use]
    pub fn new(cfg: FeatureFlagsConfig) -> Self {
        Self { cfg }
    }
}

impl FlagProvider for StaticProvider {
    /// Get a flag state.
    ///
    /// # Errors
    /// Always returns an error in Phase 0.
    fn get(&self, _key: FlagKey) -> Result<FlagState, FlagError> {
        // TODO(phase-1): static lookup.
        Err(FlagError::ShuttingDown)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Preset;

    #[test]
    fn static_provider_constructs() {
        let cfg = Preset::Default.to_config();
        let provider = StaticProvider::new(cfg);
        assert_eq!(provider.cfg.max_flags, 10_000);
    }

    #[test]
    fn static_provider_get_stub() {
        let cfg = Preset::Default.to_config();
        let provider = StaticProvider::new(cfg);
        let key = FlagKey::new("test").unwrap();
        assert!(provider.get(key).is_err());
    }

    #[test]
    fn static_provider_is_send_sync() {
        // Ensure the provider is Send + Sync (required for the trait).
        fn assert_send_sync<T: Send + Sync>() {}
        let cfg = Preset::Default.to_config();
        let provider = StaticProvider::new(cfg);
        assert_send_sync::<StaticProvider>();
        let _ = provider; // suppress unused warning.
    }
}
