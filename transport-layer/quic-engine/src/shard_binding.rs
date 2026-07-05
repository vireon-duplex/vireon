//! [`ShardBinding`] — reconcile [`QuicConnection`] with the
//! runtime-layer [`connection_shard::ConnectionShard`].
//!
//! Phase 13 — Runtime Integration. See `DESIGN.md` §4.13.
//!
//! Two state machines run in parallel for each QUIC connection:
//!
//! 1. **QUIC lifecycle** — [`crate::state::QuicConnState`] (handshake
//!    / connected / closing / draining / closed)
//! 2. **Vireon ownership** —
//!    [`connection_shard::ConnectionState`] (Init / Active /
//!    Backpressured / Migrating / Closing / Closed)
//!
//! The shard binding translates between them. The shard sees a
//! stream_id-style handle that the QUIC engine registers; the QUIC
//! engine queries the shard for backpressure / migration signals.
//!
//! ```text
//! quiche event        QuicConnState   ConnectionState   Shard op
//! ────────────────    ─────────────   ───────────────   ────────
//! Initial recv        Handshaking     Init              shard.create()
//! HandshakeDone       Connected       Active            shard.activate()
//! pressure event      Connected       Backpressured     (none — observe)
//! pressure relief     Connected       Active            (none — observe)
//! prepare_migration   Connected       Migrating         shard.prepare_migration()
//! accept_migration    Connected       Active            shard.accept_migration()
//! complete_mig_out    Connected       Active            (free slot on src)
//! CONNECTION_CLOSE    Closing         Closing           shard.begin_close()
//! drain complete      Closed          Closed            shard.drain_closed()
//! ```

use connection_shard::{ConnectionId, ConnectionState};

/// Phase-0 skeleton. Phase 13 populates the binding table and the
/// translation logic.
#[derive(Default)]
pub struct ShardBinding {
    /// Number of QUIC connections currently mapped to shard slots.
    bound: usize,
}

impl ShardBinding {
    #[must_use]
    pub const fn new() -> Self {
        Self { bound: 0 }
    }

    /// TODO(phase-13): `fn bind(&mut self, conn_slot, shard: &mut ConnectionShard)`
    /// `fn reconcile(&mut self, quic_state, shard_state)`.
    pub fn _placeholder(&self) {}

    /// Whether the binding has any active mappings.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.bound == 0
    }
}

/// Phase-13 translate function (declarative mapping).
#[must_use]
pub fn shard_state_for(quic_state: crate::state::QuicConnState) -> ConnectionState {
    use crate::state::QuicConnState as Q;
    match quic_state {
        Q::Handshaking => ConnectionState::Init,
        Q::Connected => ConnectionState::Active,
        Q::Closing | Q::Draining => ConnectionState::Closing,
        Q::Closed => ConnectionState::Closed,
    }
}

/// Phase-13 cross-reference: when a QUIC connection is bound to a
/// shard slot, this is the handle pair.
#[derive(Clone, Copy, Debug)]
pub struct Binding {
    /// Engine-local connection slot.
    pub quic_slot: u32,
    /// Shard-side connection id (slot + generation).
    pub shard_id: ConnectionId,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::QuicConnState;

    #[test]
    fn placeholder() {
        let b = ShardBinding::new();
        assert!(b.is_empty());
        assert_eq!(
            shard_state_for(QuicConnState::Connected),
            ConnectionState::Active
        );
        // TODO(phase-13): full reconciliation, backpressure
        // propagation, migration handoff.
    }
}
