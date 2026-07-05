//! [`QuicEngine`] — the main facade, one per Reactor core.
//!
//! Phase 13 — Runtime Integration. See `DESIGN.md` §4.13.
//!
//! `QuicEngine` owns the per-shard state for the QUIC layer:
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                       QuicEngine (Reactor N)                    │
//! │                                                                 │
//! │  conn_id_table  ◄── packet.rs routes incoming UDP datagrams    │
//! │       │                                                         │
//! │       ▼                                                         │
//! │  slab: Vec<QuicConnection>  (Phase 1)                           │
//! │       │                                                         │
//! │       ├─ stream_scheduler (Phase 8)                             │
//! │       ├─ packet_scheduler (Phase 10)                            │
//! │       ├─ timer_binding (Phase 11)                               │
//! │       └─ shard_binding (Phase 13)                               │
//! │                                                                 │
//! │  Hot path (per tick):                                           │
//! │    on_recv(datagram) ──► route ──► conn.recv(datagram)          │
//! │    on_send_tick()     ──► schedule ──► send-queue.submit_gso()  │
//! │    on_timer_fire()    ──► conn.timeout()                        │
//! └─────────────────────────────────────────────────────────────────┘
//! ```
//!
//! Phase 0 ships only the type so other modules can reference it
//! (and so the doc-test in `lib.rs` is meaningful). Real
//! implementation lands in Phase 13.

use crate::config::QuicConfig;

/// Phase-0 skeleton. Phase 13 will populate this with the
/// connection slab, CID table, packet router, schedulers, and the
/// runtime-primitive bindings.
#[derive(Default, Debug)]
pub struct QuicEngine {
    /// Reactor / shard identifier (one engine per Reactor core).
    pub reactor_id: u32,
    /// Engine-wide configuration (shared by all connections).
    pub config: QuicConfig,
    /// Number of currently-active connections.
    pub active_count: usize,
}

impl QuicEngine {
    /// TODO(phase-13): full constructor with all subsystems wired.
    #[must_use]
    pub fn placeholder(reactor_id: u32) -> Self {
        Self {
            reactor_id,
            config: QuicConfig::default(),
            active_count: 0,
        }
    }

    /// TODO(phase-13): `fn on_recv(&mut self, datagram: &mut [u8], peer:
    /// SocketAddr, now_ns: u64)`, `fn on_send_tick(&mut self, sq: &mut
    /// send_queue::SendQueue, now_ns: u64)`, `fn on_timer(&mut self,
    /// timer_id: u64)`.
    pub fn _placeholder(&self) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn placeholder() {
        let e = QuicEngine::placeholder(0);
        assert_eq!(e.reactor_id, 0);
        assert_eq!(e.active_count, 0);
        // TODO(phase-13): full engine — wire conn_id_table, slab,
        // packet router, schedulers, shard binding, timer binding.
    }
}
