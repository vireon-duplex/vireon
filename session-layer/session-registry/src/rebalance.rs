//! [`RebalancePlanner`] — computes the set of ownership moves
//! needed after a topology change.
//!
//! See `DESIGN.md` Part 8 (rebalancing).
//!
//! # Role
//!
//! When a node joins or leaves, the consistent-hash ring shifts:
//! some sessions that were owned by node A must now be owned by
//! node B. The rebalance planner computes that diff so the
//! migration manager can execute the moves.
//!
//! # Algorithm (Phase 1+)
//!
//! 1. For every session in the ownership table, re-evaluate its
//!    ring owner under the new topology.
//! 2. If the new owner differs from the recorded owner, emit an
//!    `OwnershipEntry` for the new owner.
//! 3. Return the list of moves; the migration manager drains it.
//!
//! # Phase 0
//!
//! Skeleton only.

use crate::error::RegistryError;
use crate::membership::MembershipList;
use crate::ownership::OwnershipEntry;
use crate::ring::ConsistentHash;

/// Computes the ownership moves needed after a topology change.
///
/// Phase 0 ships only a typed skeleton.
#[derive(Debug, Default)]
pub struct RebalancePlanner;

impl RebalancePlanner {
    /// Construct a new planner.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }

    /// Plan the set of ownership moves required to bring the
    /// ownership table in line with the current ring topology.
    ///
    /// Returns a list of new `OwnershipEntry` records; each
    /// represents one session whose owner changed.
    ///
    /// # Errors
    /// Always returns an error in Phase 0.
    pub fn plan(
        &self,
        _ring: &ConsistentHash,
        _membership: &MembershipList,
    ) -> Result<Vec<OwnershipEntry>, RegistryError> {
        // TODO(phase-1): iterate ownership table, re-lookup each
        // session on the ring, emit moves for changed owners.
        Err(RegistryError::ShuttingDown)
    }

    /// Estimate the number of sessions that would move if a new
    /// node were added at the given id (without actually adding it).
    ///
    /// Used for capacity planning: "if we add node N, how many
    /// sessions move?"
    ///
    /// # Errors
    /// Always returns an error in Phase 0.
    pub fn estimate_impact_of_add(
        &self,
        _ring: &ConsistentHash,
        _new_node_id: crate::node::NodeId,
        _sample_size: usize,
    ) -> Result<f64, RegistryError> {
        // TODO(phase-1): sample N sessions, count how many would
        // move, return ratio.
        Err(RegistryError::ShuttingDown)
    }

    /// Estimate the number of sessions that would move if a node
    /// were removed.
    ///
    /// # Errors
    /// Always returns an error in Phase 0.
    pub fn estimate_impact_of_remove(
        &self,
        _ring: &ConsistentHash,
        _node_id: crate::node::NodeId,
        _sample_size: usize,
    ) -> Result<f64, RegistryError> {
        // TODO(phase-1): as above but for removal.
        Err(RegistryError::ShuttingDown)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_planner_is_default() {
        let p = RebalancePlanner::new();
        let _default: RebalancePlanner = RebalancePlanner::default();
        // Both should be constructible and equal-by-debug.
        let _ = format!("{p:?}");
        let _ = format!("{_default:?}");
    }

    #[test]
    fn plan_phase0_stub() {
        let p = RebalancePlanner::new();
        let ring = ConsistentHash::default();
        let membership = MembershipList::new();
        assert!(p.plan(&ring, &membership).is_err());
    }

    #[test]
    fn estimate_add_phase0_stub() {
        let p = RebalancePlanner::new();
        let ring = ConsistentHash::default();
        assert!(
            p.estimate_impact_of_add(&ring, crate::node::NodeId::new(1), 1000,)
                .is_err()
        );
    }

    #[test]
    fn estimate_remove_phase0_stub() {
        let p = RebalancePlanner::new();
        let ring = ConsistentHash::default();
        assert!(
            p.estimate_impact_of_remove(&ring, crate::node::NodeId::new(1), 1000,)
                .is_err()
        );
    }
}
