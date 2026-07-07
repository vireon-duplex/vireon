//! [`RemoteRouter`] — forwards sessions to a target node.
//!
//! See `DESIGN.md` Part 2 (routing modes) and Part 7 (RPC protocol).
//!
//! # Role
//!
//! The `RemoteRouter` handles the forward decision: the session is
//! owned by another node and the datagram must be sent over the
//! network. In Phase 1 it will resolve the target endpoint and
//! schedule the RPC.
//!
//! # Phase 0
//!
//! Skeleton only — every `route` call returns a stub error.

use crate::decision::RoutingDecision;
use crate::error::RouterError;
use session_manager::SessionId;
use session_registry::NodeId;

/// Routes sessions to a remote target node.
///
/// Phase 0 ships only a typed skeleton.
#[derive(Debug)]
pub struct RemoteRouter {
    /// The local node id.
    #[allow(dead_code)]
    local_node: NodeId,
}

impl RemoteRouter {
    /// Construct a remote router for the given local node.
    #[must_use]
    pub const fn new(local_node: NodeId) -> Self {
        Self { local_node }
    }

    /// Route a session to the specified target node.
    ///
    /// # Errors
    /// Always returns [`RouterError::ShuttingDown`] in Phase 0.
    pub fn route(&self, _id: SessionId, _target: NodeId) -> Result<RoutingDecision, RouterError> {
        // TODO(phase-1): resolve endpoint → schedule forward.
        Err(RouterError::ShuttingDown)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn construct_remote_router() {
        let r = RemoteRouter::new(NodeId::new(1));
        let _ = format!("{r:?}");
    }

    #[test]
    fn route_phase0_stub() {
        let r = RemoteRouter::new(NodeId::new(1));
        let res = r.route(SessionId::nil(), NodeId::new(2));
        assert!(matches!(res, Err(RouterError::ShuttingDown)));
    }

    #[test]
    fn route_with_valid_ids() {
        let r = RemoteRouter::new(NodeId::new(1));
        let res = r.route(SessionId::from_pair(42, 99), NodeId::new(7));
        assert!(res.is_err());
    }
}
