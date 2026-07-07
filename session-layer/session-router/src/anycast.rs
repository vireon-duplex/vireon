//! [`AnycastRouter`] — selects the healthiest candidate node.
//!
//! See `DESIGN.md` Part 5 (anycast selection).
//!
//! # Role
//!
//! The `AnycastRouter` handles the anycast decision: given a set
//! of candidate nodes, pick the one with the best health, lowest
//! latency, and least load. In Phase 1 it will use a weighted
//! scoring algorithm over health reports, RTT measurements, and
//! capacity gauges.
//!
//! # Phase 0
//!
//! Skeleton only — every `select` call returns a stub error.

use crate::error::RouterError;
use session_registry::NodeId;

/// Selects the healthiest candidate for anycast routing.
///
/// Phase 0 ships only a typed skeleton.
#[derive(Debug)]
pub struct AnycastRouter {
    /// Whether anycast is enabled.
    #[allow(dead_code)]
    enabled: bool,
}

impl AnycastRouter {
    /// Construct an anycast router.
    #[must_use]
    pub const fn new(enabled: bool) -> Self {
        Self { enabled }
    }

    /// Select the best candidate from the given list.
    ///
    /// # Errors
    /// Always returns [`RouterError::NoHealthyNodes`] in Phase 0.
    pub fn select(&self, _candidates: &[NodeId]) -> Result<NodeId, RouterError> {
        // TODO(phase-1): health-aware, latency-aware, load-aware selection.
        Err(RouterError::NoHealthyNodes)
    }
}

impl Default for AnycastRouter {
    fn default() -> Self {
        Self::new(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn construct_anycast_router() {
        let r = AnycastRouter::new(true);
        let _ = format!("{r:?}");
    }

    #[test]
    fn select_phase0_stub() {
        let r = AnycastRouter::default();
        let res = r.select(&[NodeId::new(1), NodeId::new(2)]);
        assert!(matches!(res, Err(RouterError::NoHealthyNodes)));
    }

    #[test]
    fn select_with_empty_candidates() {
        let r = AnycastRouter::default();
        let res = r.select(&[]);
        assert!(matches!(res, Err(RouterError::NoHealthyNodes)));
    }
}
