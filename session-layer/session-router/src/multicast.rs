//! [`MulticastRouter`] — fans out to a specific group.
//!
//! See `DESIGN.md` Part 6 (broadcast & multicast).
//!
//! # Role
//!
//! The `MulticastRouter` handles the multicast decision: deliver
//! the datagram to every node that is a member of the specified
//! [`MulticastGroup`]. Groups can represent regions, zones, or
//! application-defined subsets. In Phase 1 it will look up the
//! group membership and schedule one RPC per member.
//!
//! # Phase 0
//!
//! Skeleton only — every `route_group` call returns a stub error.

use crate::decision::MulticastGroup;
use crate::error::RouterError;
use session_registry::NodeId;

/// Routes datagrams to a specific multicast group.
///
/// Phase 0 ships only a typed skeleton.
#[derive(Debug)]
pub struct MulticastRouter {
    /// The default multicast group.
    #[allow(dead_code)]
    default_group: MulticastGroup,
}

impl MulticastRouter {
    /// Construct a multicast router.
    #[must_use]
    pub const fn new(default_group: MulticastGroup) -> Self {
        Self { default_group }
    }

    /// Route to all members of the given group.
    ///
    /// # Errors
    /// Always returns [`RouterError::GroupNotFound`] in Phase 0.
    pub fn route_group(&self, group: MulticastGroup) -> Result<Vec<NodeId>, RouterError> {
        // TODO(phase-1): look up group membership → fan-out.
        Err(RouterError::GroupNotFound(group))
    }
}

impl Default for MulticastRouter {
    fn default() -> Self {
        Self::new(MulticastGroup::NIL)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn construct_multicast_router() {
        let r = MulticastRouter::new(MulticastGroup::new(5));
        let _ = format!("{r:?}");
    }

    #[test]
    fn route_group_phase0_stub() {
        let r = MulticastRouter::default();
        let res = r.route_group(MulticastGroup::new(3));
        assert!(matches!(res, Err(RouterError::GroupNotFound(_))));
    }

    #[test]
    fn route_group_with_nil_group() {
        let r = MulticastRouter::default();
        let res = r.route_group(MulticastGroup::NIL);
        assert!(matches!(res, Err(RouterError::GroupNotFound(g)) if g.is_nil()));
    }
}
