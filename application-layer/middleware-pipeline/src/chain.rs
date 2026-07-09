//! [`MiddlewareChain`] — middleware registration and ordering.
//!
//! See `DESIGN.md` Part 9 (middleware chain).
//!
//! # Role
//!
//! `MiddlewareChain` maintains the set of registered middleware
//! and provides operations for adding, removing, and querying.
//!
//! # Phase 0
//!
//! Phase 0 ships only a typed skeleton. Every method returns a
//! stub error so that callers can code against the API contract
//! today. Phase 1 fills in the real logic.

use crate::config::MiddlewareConfig;
use crate::error::MiddlewareError;
use crate::id::MiddlewareId;
use crate::stage::MiddlewareStage;

/// Middleware chain registry.
///
/// Tracks registered middleware and their stage assignments.
/// Phase 0 ships a typed skeleton; Phase 1+ populates real logic.
#[derive(Debug)]
pub struct MiddlewareChain {
    /// Static configuration.
    #[allow(dead_code)]
    cfg: MiddlewareConfig,
    /// Number of middlewares in the chain.
    #[allow(dead_code)]
    count: u32,
}

impl MiddlewareChain {
    /// Construct a new empty chain.
    #[must_use]
    pub fn new(cfg: MiddlewareConfig) -> Self {
        Self { cfg, count: 0 }
    }

    /// Register a middleware with the given id and stage.
    ///
    /// # Errors
    /// Always returns an error in Phase 0.
    pub fn register(
        &mut self,
        _id: MiddlewareId,
        _stage: MiddlewareStage,
    ) -> Result<(), MiddlewareError> {
        // TODO(phase-1): add to chain + validate per-stage limit.
        Err(MiddlewareError::ShuttingDown)
    }

    /// Remove a middleware from the chain.
    ///
    /// # Errors
    /// Returns [`MiddlewareError::NotFound`] if the middleware is not
    /// registered.
    pub fn remove(&mut self, id: MiddlewareId) -> Result<(), MiddlewareError> {
        // TODO(phase-1): remove from chain.
        Err(MiddlewareError::NotFound(id))
    }

    /// Get the number of registered middlewares.
    #[must_use]
    pub const fn count(&self) -> u32 {
        self.count
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Preset;

    fn make_chain() -> MiddlewareChain {
        let cfg = Preset::Standard.to_config();
        MiddlewareChain::new(cfg)
    }

    #[test]
    fn chain_constructs_from_config() {
        let chain = make_chain();
        assert_eq!(chain.count(), 0);
    }

    #[test]
    fn register_phase0_stub() {
        let mut chain = make_chain();
        let id = MiddlewareId::new(1);
        assert!(chain.register(id, crate::MiddlewareStage::AuthN).is_err());
    }

    #[test]
    fn remove_phase0_stub() {
        let mut chain = make_chain();
        let id = MiddlewareId::new(1);
        assert!(matches!(
            chain.remove(id),
            Err(MiddlewareError::NotFound(_))
        ));
    }

    #[test]
    fn count_returns_zero() {
        let chain = make_chain();
        assert_eq!(chain.count(), 0);
    }
}
