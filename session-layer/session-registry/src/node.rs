//! [`NodeId`], [`NodeInfo`], [`NodeState`], [`Region`], [`Zone`] —
//! cluster topology primitives.
//!
//! See `DESIGN.md` Part 2 (cluster model) and Part 6 (membership).
//!
//! # Why these are newtypes?
//!
//! Plain integers (u32, u16, u8) are ambiguous at API boundaries.
//! Newtypes prevent accidentally passing a `Region` where a `Zone`
//! is expected. The wrapping is zero-cost.

// ── NodeId ──────────────────────────────────────────────────────

/// Cluster-wide node identifier. `0` is reserved as nil
/// (unassigned). u32 supports up to ~4 billion nodes, well beyond
/// any single Vireon deployment.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NodeId(u32);

impl NodeId {
    /// The nil sentinel — "no node".
    pub const NIL: Self = Self(0);

    /// Construct from a raw u32. Returns `NIL` if `id == 0` is
    /// passed; callers should check `is_nil()`.
    #[must_use]
    pub const fn new(id: u32) -> Self {
        Self(id)
    }

    /// The nil sentinel.
    #[must_use]
    pub const fn nil() -> Self {
        Self::NIL
    }

    /// Whether this is the nil sentinel.
    #[must_use]
    pub const fn is_nil(self) -> bool {
        self.0 == 0
    }

    /// The raw u32 value.
    #[must_use]
    pub const fn raw(self) -> u32 {
        self.0
    }
}

impl Default for NodeId {
    fn default() -> Self {
        Self::NIL
    }
}

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "node#{}", self.0)
    }
}

// ── Region ──────────────────────────────────────────────────────

/// Geographic region identifier. `0` = global/unspecified.
/// u16 supports up to 65K regions.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Region(u16);

impl Region {
    /// Global/unspecified region.
    pub const GLOBAL: Self = Self(0);

    /// Construct from a raw u16.
    #[must_use]
    pub const fn new(id: u16) -> Self {
        Self(id)
    }

    /// The raw u16 value.
    #[must_use]
    pub const fn raw(self) -> u16 {
        self.0
    }

    /// Whether this is the global sentinel.
    #[must_use]
    pub const fn is_global(self) -> bool {
        self.0 == 0
    }
}

impl std::fmt::Display for Region {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "region#{}", self.0)
    }
}

// ── Zone ────────────────────────────────────────────────────────

/// Availability zone within a region. `0` = default.
/// u8 supports up to 256 zones per region (AWS tops out at ~6).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Zone(u8);

impl Zone {
    /// Default zone.
    pub const DEFAULT: Self = Self(0);

    /// Construct from a raw u8.
    #[must_use]
    pub const fn new(id: u8) -> Self {
        Self(id)
    }

    /// The raw u8 value.
    #[must_use]
    pub const fn raw(self) -> u8 {
        self.0
    }

    /// Whether this is the default sentinel.
    #[must_use]
    pub const fn is_default(self) -> bool {
        self.0 == 0
    }
}

impl std::fmt::Display for Zone {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "zone#{}", self.0)
    }
}

// ── NodeState ───────────────────────────────────────────────────

/// Node lifecycle state. Modeled after SWIM failure detector
/// states.
///
/// See `DESIGN.md` Part 6 (membership) for the transition table.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum NodeState {
    /// Node has announced join but hasn't completed the handshake.
    #[default]
    Joining,
    /// Node is reachable and accepting sessions.
    Alive,
    /// Node missed heartbeats; under suspicion.
    Suspect,
    /// Node is confirmed dead (quorum of suspicion reached).
    Dead,
    /// Node has gracefully left the cluster.
    Left,
}

impl NodeState {
    /// Whether the node is considered alive (membership operations
    /// allowed).
    #[must_use]
    pub const fn is_alive(self) -> bool {
        matches!(self, Self::Alive)
    }

    /// Whether the node is in a healthy state (Alive or Joining).
    #[must_use]
    pub const fn is_healthy(self) -> bool {
        matches!(self, Self::Alive | Self::Joining)
    }

    /// Whether the node can accept new sessions.
    #[must_use]
    pub const fn can_accept_sessions(self) -> bool {
        matches!(self, Self::Alive)
    }

    /// Whether this state is terminal (no further transitions).
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Left)
    }

    /// Whether a transition from `self` to `target` is legal.
    #[must_use]
    pub const fn can_transition_to(self, target: Self) -> bool {
        use NodeState as S;
        match (self, target) {
            (S::Joining, S::Alive | S::Dead | S::Left) => true,
            (S::Alive, S::Suspect | S::Dead | S::Left) => true,
            (S::Suspect, S::Alive | S::Dead | S::Left) => true,
            (S::Dead, S::Left) => true,
            (S::Left, _) => false,
            _ => false,
        }
    }

    /// Human-readable name for metrics labels.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Joining => "joining",
            Self::Alive => "alive",
            Self::Suspect => "suspect",
            Self::Dead => "dead",
            Self::Left => "left",
        }
    }
}

// ── NodeAddress ─────────────────────────────────────────────────

/// Node network address. IPv6-ready (IPv4-mapped addresses fit).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct NodeAddress {
    /// 16-byte IPv6 address (IPv4 uses `::ffff:a.b.c.d` mapping).
    pub host: [u8; 16],
    /// TCP/UDP/QUIC port.
    pub port: u16,
}

impl NodeAddress {
    /// Construct from a host array and port.
    #[must_use]
    pub const fn new(host: [u8; 16], port: u16) -> Self {
        Self { host, port }
    }

    /// Loopback address on a given port — useful for tests.
    #[must_use]
    pub const fn loopback(port: u16) -> Self {
        // ::1
        let mut host = [0u8; 16];
        host[15] = 1;
        Self { host, port }
    }
}

impl Default for NodeAddress {
    fn default() -> Self {
        Self::loopback(0)
    }
}

// ── NodeCapacity ────────────────────────────────────────────────

/// Static + dynamic capacity descriptors for a node. Used by the
/// scheduler and the rebalance planner.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct NodeCapacity {
    /// Maximum concurrent sessions this node supports.
    pub max_sessions: u32,
    /// Maximum outbound bandwidth (bytes/sec).
    pub max_bandwidth_bps: u64,
    /// Maximum CPU budget for session layer (percent).
    pub max_cpu_pct: u8,
    /// Currently-active sessions on this node.
    pub current_sessions: u32,
}

impl NodeCapacity {
    /// Construct zeroed capacity.
    #[must_use]
    pub const fn empty() -> Self {
        Self {
            max_sessions: 0,
            max_bandwidth_bps: 0,
            max_cpu_pct: 0,
            current_sessions: 0,
        }
    }

    /// Whether the node has any remaining session capacity.
    #[must_use]
    pub const fn has_capacity(self) -> bool {
        self.current_sessions < self.max_sessions
    }

    /// Session utilization fraction numerator (out of 1000 for
    /// fixed-point use).
    #[must_use]
    pub fn utilization_per_mille(self) -> u32 {
        if self.max_sessions == 0 {
            0
        } else {
            ((self.current_sessions as u64 * 1_000) / self.max_sessions as u64) as u32
        }
    }
}

// ── NodeInfo ────────────────────────────────────────────────────

/// Static + dynamic info about a cluster node.
#[derive(Clone, Debug)]
pub struct NodeInfo {
    /// Cluster-wide unique node id.
    pub id: NodeId,
    /// Geographic region.
    pub region: Region,
    /// Availability zone within `region`.
    pub zone: Zone,
    /// Routable network address.
    pub address: NodeAddress,
    /// Capacity descriptors.
    pub capacity: NodeCapacity,
    /// WFQ weight (higher = more sessions on the ring).
    pub weight: u32,
    /// Current lifecycle state.
    pub state: NodeState,
    /// Nanoseconds since cluster epoch when the node joined.
    pub joined_ns: u64,
}

impl NodeInfo {
    /// Construct a new node info with sensible defaults.
    #[must_use]
    pub fn new(id: NodeId, region: Region, zone: Zone, address: NodeAddress) -> Self {
        Self {
            id,
            region,
            zone,
            address,
            capacity: NodeCapacity::empty(),
            weight: 1,
            state: NodeState::Joining,
            joined_ns: 0,
        }
    }

    /// Whether the node can accept new sessions right now.
    #[must_use]
    pub const fn can_accept_sessions(&self) -> bool {
        self.state.can_accept_sessions() && self.capacity.has_capacity()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_id_nil_sentinel() {
        assert!(NodeId::NIL.is_nil());
        assert!(NodeId::nil().is_nil());
        assert!(!NodeId::new(1).is_nil());
        assert_eq!(NodeId::default(), NodeId::NIL);
    }

    #[test]
    fn node_id_raw_roundtrips() {
        let id = NodeId::new(42);
        assert_eq!(id.raw(), 42);
    }

    #[test]
    fn node_id_display() {
        assert_eq!(format!("{}", NodeId::new(7)), "node#7");
        assert_eq!(format!("{}", NodeId::NIL), "node#0");
    }

    #[test]
    fn node_id_ordering() {
        assert!(NodeId::new(1) < NodeId::new(2));
    }

    #[test]
    fn region_global_sentinel() {
        assert!(Region::GLOBAL.is_global());
        assert!(Region::default().is_global());
        assert!(!Region::new(1).is_global());
    }

    #[test]
    fn zone_default_sentinel() {
        assert!(Zone::DEFAULT.is_default());
        assert!(Zone::default().is_default());
        assert!(!Zone::new(1).is_default());
    }

    #[test]
    fn node_state_transitions() {
        use NodeState as S;
        assert!(S::Joining.can_transition_to(S::Alive));
        assert!(S::Alive.can_transition_to(S::Suspect));
        assert!(S::Suspect.can_transition_to(S::Alive));
        assert!(S::Alive.can_transition_to(S::Dead));
        assert!(S::Dead.can_transition_to(S::Left));
        assert!(!S::Left.can_transition_to(S::Alive));
        assert!(!S::Alive.can_transition_to(S::Joining));
    }

    #[test]
    fn node_state_predicates() {
        assert!(NodeState::Alive.is_alive());
        assert!(NodeState::Alive.can_accept_sessions());
        assert!(!NodeState::Suspect.can_accept_sessions());
        assert!(NodeState::Joining.is_healthy());
        assert!(!NodeState::Dead.is_healthy());
        assert!(NodeState::Left.is_terminal());
        assert!(!NodeState::Alive.is_terminal());
    }

    #[test]
    fn node_state_as_str() {
        assert_eq!(NodeState::Alive.as_str(), "alive");
        assert_eq!(NodeState::Suspect.as_str(), "suspect");
        assert_eq!(NodeState::Left.as_str(), "left");
    }

    #[test]
    fn node_address_loopback() {
        let a = NodeAddress::loopback(443);
        assert_eq!(a.port, 443);
        assert_eq!(a.host[15], 1);
        assert_eq!(a.host[0], 0);
    }

    #[test]
    fn node_capacity_utilization() {
        let mut c = NodeCapacity::empty();
        assert!(!c.has_capacity());
        c.max_sessions = 100;
        assert!(c.has_capacity());
        c.current_sessions = 25;
        assert_eq!(c.utilization_per_mille(), 250);
        c.current_sessions = 100;
        assert!(!c.has_capacity());
        assert_eq!(c.utilization_per_mille(), 1_000);
    }

    #[test]
    fn node_capacity_zero_max_utilization() {
        let c = NodeCapacity::empty();
        assert_eq!(c.utilization_per_mille(), 0);
    }

    #[test]
    fn node_info_new_defaults() {
        let n = NodeInfo::new(
            NodeId::new(1),
            Region::new(1),
            Zone::new(1),
            NodeAddress::loopback(443),
        );
        assert_eq!(n.weight, 1);
        assert_eq!(n.state, NodeState::Joining);
        assert!(!n.can_accept_sessions()); // Joining + empty capacity
    }

    #[test]
    fn node_info_can_accept_when_alive_with_capacity() {
        let mut n = NodeInfo::new(
            NodeId::new(1),
            Region::GLOBAL,
            Zone::DEFAULT,
            NodeAddress::loopback(443),
        );
        n.state = NodeState::Alive;
        n.capacity.max_sessions = 10;
        assert!(n.can_accept_sessions());
        n.capacity.current_sessions = 10;
        assert!(!n.can_accept_sessions());
    }
}
