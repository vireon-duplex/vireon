//! [`RouterBuilder`] — fluent builder for [`RouterConfig`] and
//! the resolver itself.
//!
//! See `DESIGN.md` Part 12 (configuration & presets).
//!
//! # Why a builder?
//!
//! `RouterConfig` has many fields and most callers only need to
//! override a handful. The builder lets them start from a preset
//! or sensible defaults and chain overrides without touching
//! every field.

use crate::config::{Preset, RouterConfig};
use crate::decision::RoutingMode;
use crate::error::RouterError;
use crate::resolver::RouteResolver;

/// Fluent builder for [`RouterConfig`].
#[derive(Debug, Clone)]
pub struct RouterBuilder {
    cfg: RouterConfig,
}

impl RouterBuilder {
    /// Start a new builder with default (`Balanced`) configuration.
    #[must_use]
    pub fn new() -> Self {
        Self {
            cfg: RouterConfig::default(),
        }
    }

    /// Start from a preset.
    #[must_use]
    pub fn from_preset(preset: Preset) -> Self {
        Self {
            cfg: preset.to_config(),
        }
    }

    /// Override the cache TTL (ms).
    #[must_use]
    pub const fn cache_ttl_ms(mut self, ms: u64) -> Self {
        self.cfg = self.cfg.with_cache_ttl_ms(ms);
        self
    }

    /// Override the cache max entries.
    #[must_use]
    pub const fn cache_max_entries(mut self, n: u32) -> Self {
        self.cfg = self.cfg.with_cache_max_entries(n);
        self
    }

    /// Override the default routing mode.
    #[must_use]
    pub const fn default_mode(mut self, mode: RoutingMode) -> Self {
        self.cfg = self.cfg.with_default_mode(mode);
        self
    }

    /// Override the RPC timeout (ms).
    #[must_use]
    pub const fn rpc_timeout_ms(mut self, ms: u64) -> Self {
        self.cfg = self.cfg.with_rpc_timeout_ms(ms);
        self
    }

    /// Override the max retries.
    #[must_use]
    pub const fn max_retries(mut self, n: u8) -> Self {
        self.cfg = self.cfg.with_max_retries(n);
        self
    }

    /// Enable or disable anycast.
    #[must_use]
    pub const fn anycast(mut self, on: bool) -> Self {
        self.cfg = self.cfg.with_anycast(on);
        self
    }

    /// Enable or disable broadcast.
    #[must_use]
    pub const fn broadcast(mut self, on: bool) -> Self {
        self.cfg = self.cfg.with_broadcast(on);
        self
    }

    /// Override the health check interval (ms).
    #[must_use]
    pub const fn health_check_interval_ms(mut self, ms: u64) -> Self {
        self.cfg = self.cfg.with_health_check_interval_ms(ms);
        self
    }

    /// Validate and produce the final config.
    ///
    /// # Errors
    /// Returns [`RouterError::InvalidConfig`] if any field is out
    /// of range.
    pub fn build(self) -> Result<RouterConfig, RouterError> {
        self.cfg.validate()?;
        Ok(self.cfg)
    }

    /// Validate, build config, and construct a [`RouteResolver`].
    ///
    /// # Errors
    /// Returns [`RouterError::InvalidConfig`] if validation fails.
    pub fn build_resolver(self) -> Result<RouteResolver, RouterError> {
        let cfg = self.build()?;
        RouteResolver::new(cfg)
    }
}

impl Default for RouterBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_defaults_to_balanced() {
        let cfg = RouterBuilder::new().build().unwrap();
        assert_eq!(cfg, Preset::Balanced.to_config());
    }

    #[test]
    fn builder_from_preset() {
        let cfg = RouterBuilder::from_preset(Preset::Cacheless)
            .build()
            .unwrap();
        assert!(!cfg.cache_enabled());
    }

    #[test]
    fn builder_chains_overrides() {
        let cfg = RouterBuilder::new()
            .cache_ttl_ms(30_000)
            .rpc_timeout_ms(5)
            .max_retries(5)
            .build()
            .unwrap();
        assert_eq!(cfg.cache_ttl_ms, 30_000);
        assert_eq!(cfg.rpc_timeout_ms, 5);
        assert_eq!(cfg.max_retries, 5);
    }

    #[test]
    fn builder_validates_on_build() {
        let res = RouterBuilder::new().rpc_timeout_ms(0).build();
        assert!(res.is_err());
    }

    #[test]
    fn builder_builds_resolver() {
        let resolver = RouterBuilder::from_preset(Preset::Balanced)
            .build_resolver()
            .unwrap();
        let h = resolver.health();
        assert!(h.is_healthy());
    }
}
