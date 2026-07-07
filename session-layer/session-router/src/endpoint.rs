//! [`NodeEndpoint`], [`EndpointProtocol`] — network address and
//! transport protocol for routing decisions.
//!
//! See `DESIGN.md` Part 2 (routing modes) and Part 7 (RPC protocol).
//!
//! # Role
//!
//! A `NodeEndpoint` is the resolved network address for a target
//! node. It is produced by the routing layer after a routing
//! decision identifies the next hop, and consumed by the connection
//! shard to open or reuse a transport connection.

use session_registry::NodeId;

// ── EndpointProtocol ────────────────────────────────────────────

/// The transport protocol used for a node endpoint.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum EndpointProtocol {
    /// QUIC (RFC 9000) — the default and preferred protocol.
    #[default]
    Quic,
    /// TCP fallback for legacy nodes.
    Tcp,
    /// UDP for fire-and-forget datagrams.
    Udp,
}

impl EndpointProtocol {
    /// Whether this endpoint uses QUIC.
    #[must_use]
    pub const fn is_quic(self) -> bool {
        matches!(self, Self::Quic)
    }

    /// Whether this endpoint uses TCP.
    #[must_use]
    pub const fn is_tcp(self) -> bool {
        matches!(self, Self::Tcp)
    }

    /// Whether this endpoint uses UDP.
    #[must_use]
    pub const fn is_udp(self) -> bool {
        matches!(self, Self::Udp)
    }

    /// Human-readable name for metrics labels.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Quic => "quic",
            Self::Tcp => "tcp",
            Self::Udp => "udp",
        }
    }
}

impl std::fmt::Display for EndpointProtocol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

// ── NodeEndpoint ────────────────────────────────────────────────

/// A resolved network endpoint for a cluster node.
///
/// Combines the node id with its IPv6 address, port, transport
/// protocol, and a WFQ-style weight for load-aware selection.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct NodeEndpoint {
    /// The cluster-wide node id.
    pub node_id: NodeId,
    /// 16-byte IPv6 address (IPv4 uses `::ffff:a.b.c.d` mapping).
    pub address: [u8; 16],
    /// Transport port (QUIC/TCP/UDP).
    pub port: u16,
    /// Transport protocol for this endpoint.
    pub protocol: EndpointProtocol,
    /// Weighted Fair Queuing weight (higher = more traffic).
    pub weight: u32,
}

impl NodeEndpoint {
    /// Construct a new endpoint.
    #[must_use]
    pub const fn new(
        node_id: NodeId,
        address: [u8; 16],
        port: u16,
        protocol: EndpointProtocol,
        weight: u32,
    ) -> Self {
        Self {
            node_id,
            address,
            port,
            protocol,
            weight,
        }
    }

    /// Whether this endpoint uses QUIC.
    #[must_use]
    pub const fn is_quic(&self) -> bool {
        self.protocol.is_quic()
    }

    /// Loopback endpoint on the given port — useful for tests.
    #[must_use]
    pub fn loopback(node_id: NodeId, port: u16) -> Self {
        // ::1
        let mut address = [0u8; 16];
        address[15] = 1;
        Self::new(node_id, address, port, EndpointProtocol::Quic, 100)
    }
}

impl Default for NodeEndpoint {
    fn default() -> Self {
        Self::loopback(NodeId::new(1), 443)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn protocol_defaults_to_quic() {
        assert_eq!(EndpointProtocol::default(), EndpointProtocol::Quic);
        assert!(EndpointProtocol::Quic.is_quic());
    }

    #[test]
    fn protocol_predicates() {
        assert!(EndpointProtocol::Quic.is_quic());
        assert!(EndpointProtocol::Tcp.is_tcp());
        assert!(EndpointProtocol::Udp.is_udp());
        assert!(!EndpointProtocol::Tcp.is_quic());
    }

    #[test]
    fn protocol_as_str() {
        assert_eq!(EndpointProtocol::Quic.as_str(), "quic");
        assert_eq!(EndpointProtocol::Tcp.as_str(), "tcp");
        assert_eq!(EndpointProtocol::Udp.as_str(), "udp");
    }

    #[test]
    fn endpoint_loopback() {
        let ep = NodeEndpoint::loopback(NodeId::new(5), 443);
        assert_eq!(ep.node_id, NodeId::new(5));
        assert_eq!(ep.port, 443);
        assert!(ep.is_quic());
        assert_eq!(ep.address[15], 1);
        assert_eq!(ep.weight, 100);
    }

    #[test]
    fn endpoint_new() {
        let addr = [0u8; 16];
        let ep = NodeEndpoint::new(NodeId::new(2), addr, 8443, EndpointProtocol::Tcp, 50);
        assert_eq!(ep.node_id, NodeId::new(2));
        assert_eq!(ep.port, 8443);
        assert!(!ep.is_quic());
        assert_eq!(ep.weight, 50);
    }
}
