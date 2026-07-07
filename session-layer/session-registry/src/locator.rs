//! [`SessionLocator`] — the O(1) lookup result.
//!
//! See `DESIGN.md` Part 9 (session locator) and Part 1 (architecture
//! goals).
//!
//! # Role
//!
//! Given a `SessionId`, the registry resolves a `SessionLocator`
//! pointing to:
//!
//! - The owning node (`NodeId`).
//! - The bound QUIC connection (`ConnectionId`).
//! - The reactor core owning that connection.
//! - The version + lease for staleness checking.
//!
//! This is the hot-path lookup result, designed to fit in a cache
//! line (≤ 64 bytes).

use crate::lease::LeaseInfo;
use crate::node::NodeId;
use crate::version::MonotonicVersion;
use connection_shard::ConnectionId;
use session_manager::SessionId;

/// Resolved location of a session in the cluster.
///
/// This is the O(1) lookup result. Callers should check
/// `is_expired()` before using the locator, and re-resolve if
/// stale.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SessionLocator {
    /// The session this locator points to.
    pub session_id: SessionId,
    /// Node currently owning the session.
    pub owner_node: NodeId,
    /// Bound QUIC connection on the owner node.
    pub connection_id: ConnectionId,
    /// Reactor core on the owner node.
    pub reactor_id: u8,
    /// Version of this locator (incremented on each change).
    pub version: MonotonicVersion,
    /// Ownership lease (for staleness checking).
    pub lease: LeaseInfo,
}

impl SessionLocator {
    /// Construct a new locator.
    #[must_use]
    pub const fn new(
        session_id: SessionId,
        owner_node: NodeId,
        connection_id: ConnectionId,
        reactor_id: u8,
        version: MonotonicVersion,
        lease: LeaseInfo,
    ) -> Self {
        Self {
            session_id,
            owner_node,
            connection_id,
            reactor_id,
            version,
            lease,
        }
    }

    /// Whether this locator points to the local node.
    #[must_use]
    pub const fn is_local(self, self_node: NodeId) -> bool {
        self.owner_node.raw() == self_node.raw()
    }

    /// Whether the lease has expired at `now_ns`.
    #[must_use]
    pub const fn is_expired(self, now_ns: u64) -> bool {
        self.lease.is_expired(now_ns)
    }

    /// Whether the locator is valid (non-nil session + non-nil owner).
    #[must_use]
    pub fn is_valid(self) -> bool {
        !self.session_id.is_nil() && !self.owner_node.is_nil()
    }

    /// Whether `self` is strictly newer (later version) than `other`.
    /// Useful for cache invalidation: replace the cached locator
    /// only if the new one is newer.
    #[must_use]
    pub const fn is_newer_than(self, other: Self) -> bool {
        self.version.is_after(other.version)
    }
}

impl std::fmt::Display for SessionLocator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "locator({} → {} conn:{} core:{} {})",
            self.session_id, self.owner_node, self.connection_id, self.reactor_id, self.version,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lease::LeaseInfo;
    use crate::node::NodeId;
    use crate::version::Generation;
    use session_manager::SessionId;

    fn sample_locator(owner: NodeId, version: u64) -> SessionLocator {
        SessionLocator::new(
            SessionId::from_pair(1, 1),
            owner,
            ConnectionId::from_raw(42),
            3,
            MonotonicVersion::new(version),
            LeaseInfo::new(owner, Generation::INITIAL, 0, 30_000),
        )
    }

    #[test]
    fn default_is_invalid() {
        let l = SessionLocator::default();
        assert!(!l.is_valid());
        assert!(l.session_id.is_nil());
        assert!(l.owner_node.is_nil());
    }

    #[test]
    fn is_local_when_owner_matches() {
        let l = sample_locator(NodeId::new(5), 1);
        assert!(l.is_local(NodeId::new(5)));
        assert!(!l.is_local(NodeId::new(6)));
    }

    #[test]
    fn is_expired_after_lease_expiry() {
        let l = sample_locator(NodeId::new(1), 1);
        // Lease TTL was 30s starting at 0; expires at 30s.
        assert!(l.is_expired(31_000_000_000));
        assert!(!l.is_expired(5_000_000_000));
    }

    #[test]
    fn is_valid_requires_non_nil() {
        let mut l = sample_locator(NodeId::new(1), 1);
        assert!(l.is_valid());
        l.owner_node = NodeId::NIL;
        assert!(!l.is_valid());
    }

    #[test]
    fn newer_version_supersedes() {
        let l1 = sample_locator(NodeId::new(1), 1);
        let l2 = sample_locator(NodeId::new(2), 2);
        assert!(l2.is_newer_than(l1));
        assert!(!l1.is_newer_than(l2));
    }

    #[test]
    fn display_includes_session_and_owner() {
        let l = sample_locator(NodeId::new(7), 1);
        let s = format!("{l}");
        assert!(s.contains("node#7"));
        assert!(s.contains("v#1"));
    }
}
