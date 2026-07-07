//! [`BroadcastRouter`] — fans out to all nodes.
//!
//! See `DESIGN.md` Part 6 (broadcast & multicast).
//!
//! # Role
//!
//! The `BroadcastRouter` handles the broadcast decision: deliver
//! the datagram to every node in the cluster. In Phase 1 it will
//! iterate the membership list and schedule one RPC per target.
//!
//! # Phase 0
//!
//! Skeleton only — every `route_all` call returns a stub error.

use crate::error::RouterError;
use session_registry::NodeId;

/// Routes datagrams to every node in the cluster.
///
/// Phase 0 ships only a typed skeleton.
#[derive(Debug)]
pub struct BroadcastRouter {
    /// Whether broadcast is enabled.
    #[allow(dead_code)]
    enabled: bool,
}

impl BroadcastRouter {
    /// Construct a broadcast router.
    #[must_use]
    pub const fn new(enabled: bool) -> Self {
        Self { enabled }
    }

    /// Route to all nodes in the given list.
    ///
    /// # Errors
    /// Always returns [`RouterError::ShuttingDown`] in Phase 0.
    pub fn route_all(&self, _endpoints: &[NodeId]) -> Result<Vec<NodeId>, RouterError> {
        // TODO(phase-1): fan-out + per-target scheduling.
        Err(RouterError::ShuttingDown)
    }
}

impl Default for BroadcastRouter {
    fn default() -> Self {
        Self::new(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn construct_broadcast_router() {
        let r = BroadcastRouter::new(true);
        let _ = format!("{r:?}");
    }

    #[test]
    fn route_all_phase0_stub() {
        let r = BroadcastRouter::new(true);
        let res = r.route_all(&[NodeId::new(1), NodeId::new(2)]);
        assert!(matches!(res, Err(RouterError::ShuttingDown)));
    }

    #[test]
    fn route_all_with_empty_list() {
        let r = BroadcastRouter::default();
        let res = r.route_all(&[]);
        assert!(res.is_err());
    }
}
