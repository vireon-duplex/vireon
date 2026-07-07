//! [`RouteResolver`] — the main routing facade.
//!
//! See `DESIGN.md` Part 1 (overview) and Part 4 (hot-path optimization).
//!
//! # Role
//!
//! `RouteResolver` is the single entry point that callers use to
//! resolve a `SessionId` to a [`RoutingDecision`]. It composes:
//!
//! - a [`RoutingCache`] (TTL-based look-aside cache),
//! - [`RouterStats`] (observability),
//! - [`RouterConfig`] (static configuration).
//!
//! In Phase 1 it will also compose the `LocalRouter`, `RemoteRouter`,
//! and `AnycastRouter` sub-components.
//!
//! # Phase 0
//!
//! Phase 0 ships only a typed skeleton. Every method returns a
//! stub error so that callers can code against the API contract
//! today. Phase 1 fills in the real logic.

use crate::cache::RoutingCache;
use crate::config::RouterConfig;
use crate::decision::RoutingDecision;
use crate::error::RouterError;
use crate::health::HealthReport;
use crate::stats::RouterStats;
use session_manager::SessionId;

/// The route resolver facade.
///
/// Composes the cache, stats, and config. Phase 0 ships a typed
/// skeleton; Phase 1+ populates real logic.
#[derive(Debug)]
pub struct RouteResolver {
    /// Static configuration.
    #[allow(dead_code)]
    cfg: RouterConfig,
    /// TTL-based routing cache.
    #[allow(dead_code)]
    cache: RoutingCache,
    /// Atomic observability counters.
    stats: RouterStats,
}

impl RouteResolver {
    /// Construct a resolver from validated configuration.
    ///
    /// # Errors
    /// Returns [`RouterError::InvalidConfig`] if the config fails
    /// validation.
    pub fn new(cfg: RouterConfig) -> Result<Self, RouterError> {
        cfg.validate()?;
        let cache = RoutingCache::new(cfg.cache_max_entries, cfg.cache_ttl_ms)?;
        Ok(Self {
            cfg,
            cache,
            stats: RouterStats::new(),
        })
    }

    /// Resolve a session to a routing decision.
    ///
    /// This is the **hot path** — must be O(1) in steady state
    /// (cache hit). Phase 1 will add cache → registry → fallback.
    ///
    /// # Errors
    /// Always returns [`RouterError::ShuttingDown`] in Phase 0.
    pub fn resolve(&self, id: SessionId) -> Result<RoutingDecision, RouterError> {
        // TODO(phase-1): cache → local check → registry lookup.
        self.stats.record_lookup(false);
        Err(RouterError::NoRoute(id))
    }

    /// Invalidate a cached route entry.
    ///
    /// # Errors
    /// Always returns [`RouterError::ShuttingDown`] in Phase 0.
    pub fn invalidate(&self, id: SessionId) -> Result<(), RouterError> {
        // TODO(phase-1): cache.invalidate(id).
        let _ = id;
        Err(RouterError::ShuttingDown)
    }

    /// Snapshot the resolver's health.
    #[must_use]
    pub fn health(&self) -> HealthReport {
        HealthReport::derive(&self.stats.snapshot())
    }

    /// Reference to the underlying stats (for testing / metrics).
    #[must_use]
    pub fn stats(&self) -> &RouterStats {
        &self.stats
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Preset;

    fn make_resolver() -> RouteResolver {
        let cfg = Preset::Balanced.to_config();
        RouteResolver::new(cfg).unwrap()
    }

    #[test]
    fn resolver_constructs_from_valid_config() {
        let r = make_resolver();
        let h = r.health();
        assert!(h.is_healthy());
    }

    #[test]
    fn resolver_rejects_invalid_config() {
        let mut cfg = Preset::Balanced.to_config();
        cfg.rpc_timeout_ms = 0;
        assert!(RouteResolver::new(cfg).is_err());
    }

    #[test]
    fn resolve_phase0_stub() {
        let r = make_resolver();
        let res = r.resolve(SessionId::from_pair(1, 1));
        assert!(matches!(res, Err(RouterError::NoRoute(_))));
    }

    #[test]
    fn invalidate_phase0_stub() {
        let r = make_resolver();
        let res = r.invalidate(SessionId::from_pair(1, 1));
        assert!(matches!(res, Err(RouterError::ShuttingDown)));
    }
}
