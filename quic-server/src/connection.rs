//! Connection registry — cross-layer per-connection state.
//!
//! Tracks the mapping from QUIC engine `conn_idx` to runtime-layer
//! `ConnectionId` and session-layer `SessionId`. Each layer manages
//! its own internal state (timer handles, stream sets, etc.); this
//! registry only stores the identifiers needed to bridge layers.

use std::collections::HashMap;
use std::net::SocketAddr;

use connection_shard::ConnectionId;
use session_manager::SessionId;

/// Cross-layer per-connection state.
#[derive(Clone, Copy)]
pub struct ConnState {
    /// runtime-layer connection identifier.
    pub conn_id: ConnectionId,
    /// session-layer application session id.
    pub session_id: SessionId,
    /// Peer socket address.
    pub peer_addr: SocketAddr,
}

/// Maps QUIC engine `conn_idx` → cross-layer connection state.
pub struct ConnectionRegistry {
    map: HashMap<usize, ConnState>,
}

impl ConnectionRegistry {
    /// Creates a new empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }

    /// Inserts a connection into the registry.
    pub fn insert(&mut self, conn_idx: usize, state: ConnState) {
        self.map.insert(conn_idx, state);
    }

    /// Removes and returns the connection state for `conn_idx`.
    pub fn remove(&mut self, conn_idx: usize) -> Option<ConnState> {
        self.map.remove(&conn_idx)
    }

    /// Returns a copy of the connection state for `conn_idx`.
    #[must_use]
    pub fn get(&self, conn_idx: usize) -> Option<ConnState> {
        self.map.get(&conn_idx).copied()
    }

    /// Returns the number of tracked connections.
    #[must_use]
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Returns `true` if no connections are tracked.
    #[must_use]
    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

impl Default for ConnectionRegistry {
    fn default() -> Self {
        Self::new()
    }
}
