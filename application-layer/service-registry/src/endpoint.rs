//! [`ServiceEndpoint`] — network address and metadata for a service
//! instance.
//!
//! See `DESIGN.md` Part 4 (endpoint model).

use session_registry::node::NodeId;

// ── ServiceEndpoint ─────────────────────────────────────────────────

/// Network endpoint where a service instance is available.
///
/// Combines a node identifier, IP address, port, and weight for
/// load balancing decisions.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ServiceEndpoint {
    /// Node hosting this service instance.
    pub node_id: NodeId,
    /// IP address (IPv6-ready, 16 bytes).
    pub address: [u8; 16],
    /// TCP/UDP/QUIC port.
    pub port: u16,
    /// Load balancer weight (higher = more traffic).
    pub weight: u32,
    /// Whether the endpoint is currently healthy.
    pub is_healthy: bool,
}

impl ServiceEndpoint {
    /// Construct a new endpoint.
    #[must_use]
    pub const fn new(node_id: NodeId, address: [u8; 16], port: u16) -> Self {
        Self {
            node_id,
            address,
            port,
            weight: 100, // default weight
            is_healthy: true,
        }
    }

    /// Builder-style: set the weight.
    #[must_use]
    pub const fn with_weight(mut self, weight: u32) -> Self {
        self.weight = weight;
        self
    }

    /// Builder-style: set health status.
    #[must_use]
    pub const fn with_healthy(mut self, healthy: bool) -> Self {
        self.is_healthy = healthy;
        self
    }

    /// Check if this endpoint uses QUIC (typical QUIC ports: 443, 8443).
    #[must_use]
    pub const fn is_quic(&self) -> bool {
        self.port == 443 || self.port == 8443
    }

    /// Get the address as a byte slice.
    #[must_use]
    pub const fn address_bytes(&self) -> &[u8; 16] {
        &self.address
    }

    /// Get the port.
    #[must_use]
    pub const fn port(&self) -> u16 {
        self.port
    }

    /// Get the node ID.
    #[must_use]
    pub const fn node_id(&self) -> NodeId {
        self.node_id
    }

    /// Get the weight.
    #[must_use]
    pub const fn weight(&self) -> u32 {
        self.weight
    }

    /// Whether this endpoint is healthy.
    #[must_use]
    pub const fn is_healthy(&self) -> bool {
        self.is_healthy
    }
}

impl Default for ServiceEndpoint {
    fn default() -> Self {
        Self::new(NodeId::nil(), [0u8; 16], 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoint_new_with_defaults() {
        let endpoint = ServiceEndpoint::new(
            NodeId::new(1),
            [127, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            8080,
        );
        assert_eq!(endpoint.node_id, NodeId::new(1));
        assert_eq!(endpoint.port, 8080);
        assert_eq!(endpoint.weight, 100);
        assert!(endpoint.is_healthy);
    }

    #[test]
    fn endpoint_builder_methods() {
        let endpoint = ServiceEndpoint::new(NodeId::new(1), [0u8; 16], 443)
            .with_weight(200)
            .with_healthy(false);
        assert_eq!(endpoint.weight, 200);
        assert!(!endpoint.is_healthy);
    }

    #[test]
    fn endpoint_is_quic() {
        let quic_endpoint = ServiceEndpoint::new(NodeId::new(1), [0u8; 16], 443);
        assert!(quic_endpoint.is_quic());

        let quic_endpoint_alt = ServiceEndpoint::new(NodeId::new(1), [0u8; 16], 8443);
        assert!(quic_endpoint_alt.is_quic());

        let tcp_endpoint = ServiceEndpoint::new(NodeId::new(1), [0u8; 16], 8080);
        assert!(!tcp_endpoint.is_quic());
    }

    #[test]
    fn endpoint_address_bytes() {
        let addr = [192, 168, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        let endpoint = ServiceEndpoint::new(NodeId::new(1), addr, 443);
        assert_eq!(endpoint.address_bytes(), &addr);
    }

    #[test]
    fn endpoint_accessors() {
        let endpoint = ServiceEndpoint::new(NodeId::new(42), [0u8; 16], 9090)
            .with_weight(150)
            .with_healthy(true);

        assert_eq!(endpoint.node_id(), NodeId::new(42));
        assert_eq!(endpoint.port(), 9090);
        assert_eq!(endpoint.weight(), 150);
        assert!(endpoint.is_healthy());
    }

    #[test]
    fn endpoint_default() {
        let endpoint = ServiceEndpoint::default();
        assert_eq!(endpoint.node_id, NodeId::nil());
        assert_eq!(endpoint.port, 0);
        assert_eq!(endpoint.address, [0u8; 16]);
    }

    #[test]
    fn endpoint_equality() {
        let endpoint1 = ServiceEndpoint::new(NodeId::new(1), [0u8; 16], 443);
        let endpoint2 = ServiceEndpoint::new(NodeId::new(1), [0u8; 16], 443);
        let endpoint3 = ServiceEndpoint::new(NodeId::new(2), [0u8; 16], 443);

        assert_eq!(endpoint1, endpoint2);
        assert_ne!(endpoint1, endpoint3);
    }

    #[test]
    fn endpoint_hash() {
        use std::collections::HashSet;
        let endpoint1 = ServiceEndpoint::new(NodeId::new(1), [0u8; 16], 443);
        let endpoint2 = ServiceEndpoint::new(NodeId::new(1), [0u8; 16], 443);
        let endpoint3 = ServiceEndpoint::new(NodeId::new(2), [0u8; 16], 443);

        let mut set = HashSet::new();
        set.insert(endpoint1);
        set.insert(endpoint2);
        set.insert(endpoint3);
        assert_eq!(set.len(), 2); // endpoint1 == endpoint2
    }

    #[test]
    fn endpoint_clone() {
        let endpoint = ServiceEndpoint::new(
            NodeId::new(1),
            [127, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            443,
        )
        .with_weight(200);
        let cloned = endpoint;
        assert_eq!(endpoint, cloned);
    }

    #[test]
    fn endpoint_const_new() {
        const ENDPOINT: ServiceEndpoint = ServiceEndpoint::new(NodeId::new(1), [0u8; 16], 443);
        assert_eq!(ENDPOINT.port(), 443);
    }
}
