//! [`StatsCollector`] + [`StatsSnapshot`] — atomic counters,
//! monitor-readable.
//!
//! Phase 14 — Observability. See `DESIGN.md` §4.14.
//!
//! Mirrors the pattern established by `cqe-processor` and
//! `connection-shard`: counters use `AtomicU64` with `Relaxed`
//! ordering (single-writer per shard on the hot path; the snapshot
//! is `Copy` so monitoring threads don't need to borrow).
//!
//! Categories:
//!
//! - **Connections** — created, destroyed, migrated, currently active
//! - **Packets** — recv, sent, dropped (with reason breakdown)
//! - **Bytes** — recv, sent
//! - **Streams** — opened, closed, currently active
//! - **Loss / CC** — packets lost, spurious losses, ECN marks, cwnd
//! - **Crypto** — handshake attempts, successes, 0-RTT accepted,
//!   key updates
//! - **Flow control** — blocked events, window updates
//! - **Path** — migrations, NAT rebindings, path validations

use std::sync::atomic::{AtomicU64, Ordering};

/// Atomic counter collection. Phase 0 ships the type and a
/// snapshot; Phase 14 will populate the full set of fields.
#[derive(Default)]
pub struct StatsCollector {
    /// Connections created (cumulative).
    pub connections_created: AtomicU64,
    /// Connections destroyed (cumulative).
    pub connections_destroyed: AtomicU64,
    /// Packets received (cumulative).
    pub packets_recv: AtomicU64,
    /// Packets sent (cumulative).
    pub packets_sent: AtomicU64,
    /// Packets dropped due to error (cumulative).
    pub packets_dropped: AtomicU64,
    /// Total bytes received (cumulative).
    pub bytes_recv: AtomicU64,
    /// Total bytes sent (cumulative).
    pub bytes_sent: AtomicU64,
}

impl StatsCollector {
    /// Increment `connections_created` by 1.
    pub fn on_connect(&self) {
        self.connections_created.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment `connections_destroyed` by 1.
    pub fn on_disconnect(&self) {
        self.connections_destroyed.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a received packet of `bytes` length.
    pub fn on_recv(&self, bytes: u64) {
        self.packets_recv.fetch_add(1, Ordering::Relaxed);
        self.bytes_recv.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Record a sent packet of `bytes` length.
    pub fn on_send(&self, bytes: u64) {
        self.packets_sent.fetch_add(1, Ordering::Relaxed);
        self.bytes_sent.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Record a dropped packet.
    pub fn on_drop(&self) {
        self.packets_dropped.fetch_add(1, Ordering::Relaxed);
    }

    /// Take a CoW snapshot. Phase 14 will expand this.
    #[must_use]
    pub fn snapshot(&self) -> StatsSnapshot {
        StatsSnapshot {
            connections_created: self.connections_created.load(Ordering::Relaxed),
            connections_destroyed: self.connections_destroyed.load(Ordering::Relaxed),
            packets_recv: self.packets_recv.load(Ordering::Relaxed),
            packets_sent: self.packets_sent.load(Ordering::Relaxed),
            packets_dropped: self.packets_dropped.load(Ordering::Relaxed),
            bytes_recv: self.bytes_recv.load(Ordering::Relaxed),
            bytes_sent: self.bytes_sent.load(Ordering::Relaxed),
        }
    }
}

/// Copy snapshot for monitor threads.
#[derive(Clone, Copy, Debug, Default)]
pub struct StatsSnapshot {
    pub connections_created: u64,
    pub connections_destroyed: u64,
    pub packets_recv: u64,
    pub packets_sent: u64,
    pub packets_dropped: u64,
    pub bytes_recv: u64,
    pub bytes_sent: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn placeholder() {
        let s = StatsCollector::default();
        s.on_connect();
        s.on_recv(1024);
        s.on_send(512);
        let snap = s.snapshot();
        assert_eq!(snap.connections_created, 1);
        assert_eq!(snap.packets_recv, 1);
        assert_eq!(snap.bytes_recv, 1024);
        // TODO(phase-14): full stat set, Prometheus exposition,
        // OpenTelemetry integration.
    }
}
