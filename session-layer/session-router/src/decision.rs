//! [`RoutingDecision`], [`RoutingMode`], [`MulticastGroup`] — the
//! typed output of the routing layer.
//!
//! See `DESIGN.md` Part 2 (routing modes) and Part 6 (broadcast &
//! multicast).
//!
//! # Routing modes
//!
//! | Mode        | Decision                | When to use                |
//! |-------------|-------------------------|----------------------------|
//! | `Local`     | Deliver on this node    | Session owned locally      |
//! | `Remote`    | Forward to target       | Session owned by peer      |
//! | `Anycast`   | Pick healthiest node    | No affinity, latency-first |
//! | `Broadcast` | Fan-out to all nodes    | Cluster-wide control msg   |
//! | `Multicast` | Fan-out to group        | Region or zone-scoped msg  |

use session_registry::NodeId;

// ── MulticastGroup ──────────────────────────────────────────────

/// A multicast group identifier (newtype over `u16`).
///
/// Group `0` is reserved as the nil sentinel ("no group").
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct MulticastGroup(u16);

impl MulticastGroup {
    /// The nil sentinel — "no group".
    pub const NIL: Self = Self(0);

    /// Construct from a raw `u16`.
    #[must_use]
    pub const fn new(id: u16) -> Self {
        Self(id)
    }

    /// Whether this is the nil sentinel.
    #[must_use]
    pub const fn is_nil(self) -> bool {
        self.0 == 0
    }

    /// The raw `u16` value.
    #[must_use]
    pub const fn raw(self) -> u16 {
        self.0
    }
}

impl Default for MulticastGroup {
    fn default() -> Self {
        Self::NIL
    }
}

impl std::fmt::Display for MulticastGroup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "mcast#{}", self.0)
    }
}

// ── RoutingMode ─────────────────────────────────────────────────

/// The configured routing mode for a session or request.
///
/// Determines which router sub-component handles the lookup.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum RoutingMode {
    /// Unicast: deliver to exactly one node (the owner).
    #[default]
    Unicast,
    /// Anycast: deliver to the healthiest candidate.
    Anycast,
    /// Broadcast: deliver to every node.
    Broadcast,
    /// Multicast: deliver to a specific group.
    Multicast,
}

impl RoutingMode {
    /// Whether this is unicast mode.
    #[must_use]
    pub const fn is_unicast(self) -> bool {
        matches!(self, Self::Unicast)
    }

    /// Whether this is anycast mode.
    #[must_use]
    pub const fn is_anycast(self) -> bool {
        matches!(self, Self::Anycast)
    }

    /// Whether this is broadcast mode.
    #[must_use]
    pub const fn is_broadcast(self) -> bool {
        matches!(self, Self::Broadcast)
    }

    /// Whether this mode fans out to multiple recipients.
    #[must_use]
    pub const fn is_fan_out(self) -> bool {
        matches!(self, Self::Broadcast | Self::Multicast)
    }
}

// ── RoutingDecision ─────────────────────────────────────────────

/// The resolved routing decision: where to send the datagram next.
///
/// Produced by the [`RouteResolver`](crate::resolver::RouteResolver)
/// after consulting the cache, the registry, and the health
/// subsystem.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RoutingDecision {
    /// Deliver the datagram locally (this node owns the session).
    Local,
    /// Forward the datagram to the specified target node.
    Remote(NodeId),
    /// Anycast: deliver to the healthiest node (selected at resolve time).
    Anycast,
    /// Broadcast: fan-out to every node in the cluster.
    Broadcast,
    /// Multicast: fan-out to nodes in the specified group.
    Multicast(MulticastGroup),
}

impl RoutingDecision {
    /// Whether this decision means local delivery.
    #[must_use]
    pub const fn is_local(&self) -> bool {
        matches!(self, Self::Local)
    }

    /// Whether this decision means remote forwarding.
    #[must_use]
    pub const fn is_remote(&self) -> bool {
        matches!(self, Self::Remote(_))
    }

    /// Whether this decision requires an RPC to another node.
    ///
    /// `Local` delivery never requires RPC; all other modes do.
    #[must_use]
    pub const fn requires_rpc(&self) -> bool {
        !matches!(self, Self::Local)
    }

    /// The target node id, if this is a `Remote` decision.
    ///
    /// Returns `None` for `Local`, `Anycast`, `Broadcast`, and
    /// `Multicast` (which do not have a single target).
    #[must_use]
    pub const fn target_node(&self) -> Option<NodeId> {
        match self {
            Self::Remote(node) => Some(*node),
            _ => None,
        }
    }
}

impl Default for RoutingDecision {
    fn default() -> Self {
        Self::Local
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_decision_predicates() {
        let d = RoutingDecision::Local;
        assert!(d.is_local());
        assert!(!d.is_remote());
        assert!(!d.requires_rpc());
        assert_eq!(d.target_node(), None);
    }

    #[test]
    fn remote_decision_predicates() {
        let node = NodeId::new(7);
        let d = RoutingDecision::Remote(node);
        assert!(!d.is_local());
        assert!(d.is_remote());
        assert!(d.requires_rpc());
        assert_eq!(d.target_node(), Some(node));
    }

    #[test]
    fn anycast_decision_predicates() {
        let d = RoutingDecision::Anycast;
        assert!(!d.is_local());
        assert!(!d.is_remote());
        assert!(d.requires_rpc());
        assert_eq!(d.target_node(), None);
    }

    #[test]
    fn broadcast_decision_predicates() {
        let d = RoutingDecision::Broadcast;
        assert!(!d.is_local());
        assert!(d.requires_rpc());
        assert_eq!(d.target_node(), None);
    }

    #[test]
    fn multicast_decision_predicates() {
        let g = MulticastGroup::new(5);
        let d = RoutingDecision::Multicast(g);
        assert!(!d.is_local());
        assert!(d.requires_rpc());
        assert_eq!(d.target_node(), None);
    }

    #[test]
    fn multicast_group_nil() {
        assert!(MulticastGroup::NIL.is_nil());
        assert!(MulticastGroup::default().is_nil());
        assert!(!MulticastGroup::new(1).is_nil());
    }

    #[test]
    fn multicast_group_raw_roundtrips() {
        let g = MulticastGroup::new(42);
        assert_eq!(g.raw(), 42);
    }

    #[test]
    fn routing_mode_defaults_to_unicast() {
        assert_eq!(RoutingMode::default(), RoutingMode::Unicast);
        assert!(RoutingMode::Unicast.is_unicast());
        assert!(!RoutingMode::Unicast.is_fan_out());
    }

    #[test]
    fn routing_mode_fan_out() {
        assert!(RoutingMode::Broadcast.is_fan_out());
        assert!(RoutingMode::Multicast.is_fan_out());
        assert!(!RoutingMode::Unicast.is_fan_out());
        assert!(!RoutingMode::Anycast.is_fan_out());
    }
}
