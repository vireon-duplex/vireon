//! [`Connection`] — per-connection entry stored in the slab (Layer 7).
//!
//! Each [`ConnectionEntry`] is cache-aligned (`#[repr(C, align(64))]`) to
//! prevent false sharing between adjacent slots. The slab is a flat
//! `Vec<ConnectionEntry>` indexed by slot number — O(1) access with
//! minimal pointer chasing.

use std::net::SocketAddr;

use crate::priority::ConnectionPriority;
use crate::state::ConnectionState;

/// A single connection record stored in the per-shard slab.
///
/// This type is internal to the shard; callers interact with connections
/// via [`ConnectionId`](crate::id::ConnectionId) and
/// [`ConnectionSnapshot`].
#[repr(C, align(64))]
#[derive(Clone, Debug)]
pub(crate) struct ConnectionEntry {
    /// Current lifecycle state.
    pub state: ConnectionState,
    /// Scheduling priority class.
    pub priority: ConnectionPriority,
    /// The logical stream identifier from the protocol frame header.
    pub stream_id: u64,
    /// CPU / core that owns this connection.
    pub owner_cpu: u32,
    /// NUMA node of the owning core.
    pub numa_node: u32,
    /// Tenant identifier (for multi-tenant isolation).
    pub tenant_id: u32,
    /// Monotonic nanos when the connection was created.
    pub created_at_ns: u64,
    /// Monotonic nanos of last activity (recv or send).
    pub last_active_ns: u64,
    /// Total bytes sent on this connection.
    pub bytes_sent: u64,
    /// Total bytes received on this connection.
    pub bytes_recv: u64,
}

impl ConnectionEntry {
    /// Creates a new entry in the `Init` state.
    pub(crate) fn new(
        stream_id: u64,
        owner_cpu: u32,
        numa_node: u32,
        tenant_id: u32,
        priority: ConnectionPriority,
        now_ns: u64,
    ) -> Self {
        Self {
            state: ConnectionState::Init,
            priority,
            stream_id,
            owner_cpu,
            numa_node,
            tenant_id,
            created_at_ns: now_ns,
            last_active_ns: now_ns,
            bytes_sent: 0,
            bytes_recv: 0,
        }
    }

    /// Adds `bytes` to the received counter.
    pub(crate) fn add_recv(&mut self, bytes: u64, now_ns: u64) {
        self.bytes_recv = self.bytes_recv.saturating_add(bytes);
        self.last_active_ns = now_ns;
    }

    /// Adds `bytes` to the sent counter.
    pub(crate) fn add_send(&mut self, bytes: u64, now_ns: u64) {
        self.bytes_sent = self.bytes_sent.saturating_add(bytes);
        self.last_active_ns = now_ns;
    }

    /// Resets the entry to a fresh state for slot reuse.
    pub(crate) fn reset_for_reuse(&mut self) {
        self.state = ConnectionState::Init;
        self.priority = ConnectionPriority::Normal;
        self.stream_id = 0;
        self.owner_cpu = 0;
        self.numa_node = 0;
        self.tenant_id = 0;
        self.created_at_ns = 0;
        self.last_active_ns = 0;
        self.bytes_sent = 0;
        self.bytes_recv = 0;
    }

    /// Produces a read-only [`ConnectionSnapshot`] copy.
    ///
    /// `peer_addr` is passed from the shard's parallel side table.
    pub(crate) fn snapshot(&self, peer_addr: SocketAddr) -> ConnectionSnapshot {
        ConnectionSnapshot {
            state: self.state,
            priority: self.priority,
            stream_id: self.stream_id,
            peer_addr,
            owner_cpu: self.owner_cpu,
            numa_node: self.numa_node,
            tenant_id: self.tenant_id,
            created_at_ns: self.created_at_ns,
            last_active_ns: self.last_active_ns,
            bytes_sent: self.bytes_sent,
            bytes_recv: self.bytes_recv,
        }
    }
}

impl Default for ConnectionEntry {
    fn default() -> Self {
        Self::new(0, 0, 0, 0, ConnectionPriority::Normal, 0)
    }
}

/// A `Copy`-able, `Clone`-able snapshot of a connection's public state.
///
/// Returned by [`ConnectionShard::view`](crate::shard::ConnectionShard::view).
/// Contains all connection metadata but does not borrow from the shard.
#[derive(Clone, Copy, Debug)]
pub struct ConnectionSnapshot {
    /// Current lifecycle state.
    pub state: ConnectionState,
    /// Scheduling priority class.
    pub priority: ConnectionPriority,
    /// The logical stream identifier.
    pub stream_id: u64,
    /// Remote peer address.
    pub peer_addr: SocketAddr,
    /// Owning CPU / core.
    pub owner_cpu: u32,
    /// NUMA node of the owning core.
    pub numa_node: u32,
    /// Tenant identifier.
    pub tenant_id: u32,
    /// Creation time (monotonic nanos).
    pub created_at_ns: u64,
    /// Last activity time (monotonic nanos).
    pub last_active_ns: u64,
    /// Total bytes sent.
    pub bytes_sent: u64,
    /// Total bytes received.
    pub bytes_recv: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_addr() -> SocketAddr {
        SocketAddr::from(([192, 168, 1, 1], 8080))
    }

    #[test]
    fn new_entry_is_init() {
        let e = ConnectionEntry::new(42, 0, 0, 0, ConnectionPriority::Normal, 1000);
        assert_eq!(e.state, ConnectionState::Init);
        assert_eq!(e.stream_id, 42);
        assert_eq!(e.bytes_sent, 0);
        assert_eq!(e.bytes_recv, 0);
        assert_eq!(e.created_at_ns, 1000);
        assert_eq!(e.last_active_ns, 1000);
    }

    #[test]
    fn add_recv_updates_bytes_and_time() {
        let mut e = ConnectionEntry::new(1, 0, 0, 0, ConnectionPriority::Normal, 100);
        e.add_recv(1024, 200);
        assert_eq!(e.bytes_recv, 1024);
        assert_eq!(e.last_active_ns, 200);
    }

    #[test]
    fn add_send_updates_bytes_and_time() {
        let mut e = ConnectionEntry::new(1, 0, 0, 0, ConnectionPriority::Normal, 100);
        e.add_send(2048, 300);
        assert_eq!(e.bytes_sent, 2048);
        assert_eq!(e.last_active_ns, 300);
    }

    #[test]
    fn byte_counters_saturate() {
        let mut e = ConnectionEntry::new(1, 0, 0, 0, ConnectionPriority::Normal, 0);
        e.add_recv(u64::MAX, 0);
        e.add_recv(1, 0);
        assert_eq!(e.bytes_recv, u64::MAX);
    }

    #[test]
    fn reset_clears_all_fields() {
        let mut e = ConnectionEntry::new(99, 3, 1, 5, ConnectionPriority::High, 999);
        e.add_recv(500, 1000);
        e.reset_for_reuse();
        assert_eq!(e.state, ConnectionState::Init);
        assert_eq!(e.stream_id, 0);
        assert_eq!(e.bytes_recv, 0);
        assert_eq!(e.priority, ConnectionPriority::Normal);
    }

    #[test]
    fn snapshot_copies_all_fields() {
        let e = ConnectionEntry::new(7, 2, 1, 3, ConnectionPriority::RealTime, 500);
        let s = e.snapshot(sample_addr());
        assert_eq!(s.stream_id, 7);
        assert_eq!(s.peer_addr, sample_addr());
        assert_eq!(s.owner_cpu, 2);
        assert_eq!(s.numa_node, 1);
        assert_eq!(s.tenant_id, 3);
        assert_eq!(s.priority, ConnectionPriority::RealTime);
        assert_eq!(s.created_at_ns, 500);
    }

    #[test]
    fn entry_is_cache_aligned() {
        assert_eq!(std::mem::align_of::<ConnectionEntry>(), 64);
    }
}
