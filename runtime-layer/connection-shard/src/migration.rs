//! [`MigrationRequest`] — explicit ownership transfer (Layer 9).
//!
//! Migration is the **only** mechanism by which a connection changes
//! owning shard. The protocol is strictly message-based — no shared
//! mutable state:
//!
//! ```text
//! Source shard                          Target shard
//! ────────────                          ────────────
//! 1. prepare_migration(id, target)
//!    → state = Migrating
//!    → freeze: no new traffic accepted
//!    → serialize into MigrationRequest
//!
//! 2. [runtime delivers req] ──────►   3. accept_migration(req)
//!                                      → allocate new slot
//!                                      → state = Active
//!                                      → return new ConnectionId
//!
//! 4. complete_migration_out(id)
//!    → remove from source slab
//!    → bump generation
//! ```

use std::net::SocketAddr;

use crate::priority::ConnectionPriority;

/// A serialized connection state for cross-shard transfer.
///
/// Produced by [`ConnectionShard::prepare_migration`](crate::shard::ConnectionShard::prepare_migration)
/// on the source shard and consumed by
/// [`ConnectionShard::accept_migration`](crate::shard::ConnectionShard::accept_migration)
/// on the target shard.
#[derive(Clone, Debug)]
pub struct MigrationRequest {
    /// The logical stream identifier.
    pub stream_id: u64,
    /// Remote peer address.
    pub peer_addr: SocketAddr,
    /// The shard that previously owned this connection.
    pub source_cpu: u32,
    /// The shard that will own this connection.
    pub target_cpu: u32,
    /// Total bytes sent (carried over from source).
    pub bytes_sent: u64,
    /// Total bytes received (carried over from source).
    pub bytes_recv: u64,
    /// Original creation timestamp (monotonic nanos).
    pub created_at_ns: u64,
    /// Scheduling priority.
    pub priority: ConnectionPriority,
    /// Tenant identifier.
    pub tenant_id: u32,
}

impl MigrationRequest {
    /// Returns `true` if the request targets `cpu`.
    #[inline]
    #[must_use]
    pub fn targets(&self, cpu: u32) -> bool {
        self.target_cpu == cpu
    }

    /// Returns `true` if the request originates from `cpu`.
    #[inline]
    #[must_use]
    pub fn originates_from(&self, cpu: u32) -> bool {
        self.source_cpu == cpu
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_request() -> MigrationRequest {
        MigrationRequest {
            stream_id: 42,
            peer_addr: "10.0.0.1:443".parse().unwrap(),
            source_cpu: 0,
            target_cpu: 1,
            bytes_sent: 1000,
            bytes_recv: 2000,
            created_at_ns: 12345,
            priority: ConnectionPriority::High,
            tenant_id: 7,
        }
    }

    #[test]
    fn targets_check() {
        let r = sample_request();
        assert!(r.targets(1));
        assert!(!r.targets(0));
    }

    #[test]
    fn originates_check() {
        let r = sample_request();
        assert!(r.originates_from(0));
        assert!(!r.originates_from(1));
    }

    #[test]
    fn fields_preserved() {
        let r = sample_request();
        assert_eq!(r.stream_id, 42);
        assert_eq!(r.bytes_sent, 1000);
        assert_eq!(r.bytes_recv, 2000);
        assert_eq!(r.priority, ConnectionPriority::High);
        assert_eq!(r.tenant_id, 7);
    }
}
