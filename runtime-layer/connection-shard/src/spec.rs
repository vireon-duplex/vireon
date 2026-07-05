//! [`ConnectionSpec`] — input for connection creation (Layer 8).
//!
//! The caller provides a `ConnectionSpec` when calling
//! [`ConnectionShard::create`](crate::shard::ConnectionShard::create).
//! The shard fills in internal fields (owner_cpu, timestamps, etc.).

use std::net::SocketAddr;

use crate::priority::ConnectionPriority;

/// Specification for a new connection.
///
/// All fields are required at creation time. The shard supplements
/// these with `owner_cpu`, `numa_node`, `created_at_ns`, and lifecycle
/// state derived from its own context.
#[derive(Clone, Debug)]
pub struct ConnectionSpec {
    /// The logical stream identifier from the protocol frame header.
    pub stream_id: u64,
    /// Remote peer address.
    pub peer_addr: SocketAddr,
    /// Scheduling priority for this connection.
    pub priority: ConnectionPriority,
    /// Tenant identifier (for multi-tenant isolation).
    pub tenant_id: u32,
}

impl ConnectionSpec {
    /// Creates a spec with `Normal` priority and tenant 0.
    #[must_use]
    pub fn new(stream_id: u64, peer_addr: SocketAddr) -> Self {
        Self {
            stream_id,
            peer_addr,
            priority: ConnectionPriority::Normal,
            tenant_id: 0,
        }
    }

    /// Sets the priority.
    #[must_use]
    pub fn with_priority(mut self, priority: ConnectionPriority) -> Self {
        self.priority = priority;
        self
    }

    /// Sets the tenant id.
    #[must_use]
    pub fn with_tenant(mut self, tenant_id: u32) -> Self {
        self.tenant_id = tenant_id;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_addr() -> SocketAddr {
        SocketAddr::from(([10, 0, 0, 1], 443))
    }

    #[test]
    fn new_defaults_to_normal() {
        let s = ConnectionSpec::new(42, sample_addr());
        assert_eq!(s.stream_id, 42);
        assert_eq!(s.priority, ConnectionPriority::Normal);
        assert_eq!(s.tenant_id, 0);
    }

    #[test]
    fn builder_chaining() {
        let s = ConnectionSpec::new(1, sample_addr())
            .with_priority(ConnectionPriority::RealTime)
            .with_tenant(99);
        assert_eq!(s.priority, ConnectionPriority::RealTime);
        assert_eq!(s.tenant_id, 99);
    }
}
