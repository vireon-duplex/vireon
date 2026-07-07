//! [`LocalRouter`] — routes sessions that are owned by this node.
//!
//! See `DESIGN.md` Part 2 (routing modes).
//!
//! # Role
//!
//! The `LocalRouter` handles the simplest routing decision: the
//! session is owned locally and the datagram is delivered in-process.
//! In Phase 1 it will consult the ownership table to determine
//! whether a `SessionId` is local.
//!
//! # Phase 0
//!
//! Skeleton only — every `route` call returns a stub error.

use crate::decision::RoutingDecision;
use crate::error::RouterError;
use session_manager::SessionId;
use session_registry::NodeId;

/// Routes sessions that are owned by the local node.
///
/// Phase 0 ships only a typed skeleton.
#[derive(Debug)]
pub struct LocalRouter {
    /// The local node id.
    #[allow(dead_code)]
    local_node: NodeId,
}

impl LocalRouter {
    /// Construct a local router for the given node.
    #[must_use]
    pub const fn new(local_node: NodeId) -> Self {
        Self { local_node }
    }

    /// Route a session locally.
    ///
    /// # Errors
    /// Always returns [`RouterError::ShuttingDown`] in Phase 0.
    pub fn route(&self, _id: SessionId) -> Result<RoutingDecision, RouterError> {
        // TODO(phase-1): check ownership table → RoutingDecision::Local.
        Err(RouterError::ShuttingDown)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn construct_local_router() {
        let r = LocalRouter::new(NodeId::new(1));
        let _ = format!("{r:?}");
    }

    #[test]
    fn route_phase0_stub() {
        let r = LocalRouter::new(NodeId::new(1));
        let res = r.route(SessionId::nil());
        assert!(matches!(res, Err(RouterError::ShuttingDown)));
    }

    #[test]
    fn route_returns_decision_type() {
        let r = LocalRouter::new(NodeId::new(1));
        let res = r.route(SessionId::from_pair(1, 1));
        assert!(res.is_err());
    }
}
