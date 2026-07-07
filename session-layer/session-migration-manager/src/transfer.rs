//! [`StateTransfer`] — chunked, streaming state transfer from source
//! to target node.
//!
//! See `DESIGN.md` Part 4 (state transfer protocol).
//!
//! # Role
//!
//! `StateTransfer` handles the actual movement of serialized session
//! bytes from the source node to the target node. It supports:
//!
//! - **Chunked transfer** for large snapshots (avoid head-of-line
//!   blocking on QUIC streams).
//! - **Streaming transfer** for very large states (pipe snapshot
//!   chunks through without buffering the whole thing).
//! - **Checksum verification** after transfer.
//!
//! # Phase 0
//!
//! Skeleton only — every method returns a stub error.

use crate::error::MigrationError;
use crate::snapshot::MigrationSnapshotRef;
use session_registry::NodeId;

// ── StateTransfer (Phase 1 skeleton) ────────────────────────────

/// Manages chunked, streaming state transfer between nodes.
///
/// Phase 0 ships only a typed skeleton.
// TODO(phase-1): real transfer with QUIC stream + chunking.
#[derive(Debug, Default)]
pub struct StateTransfer {
    /// Maximum chunk size in bytes.
    #[allow(dead_code)]
    chunk_size: u64,
}

impl StateTransfer {
    /// Construct a new state transfer manager with the given chunk size.
    #[must_use]
    pub const fn new(chunk_size: u64) -> Self {
        Self { chunk_size }
    }

    /// Send the snapshot to the target node.
    ///
    /// Returns a transfer handle on success.
    ///
    /// # Errors
    /// Always returns [`MigrationError::TransferFailed`] in Phase 0.
    pub fn send(
        &self,
        _snapshot: &MigrationSnapshotRef,
        _target: NodeId,
    ) -> Result<u64, MigrationError> {
        // TODO(phase-1): open QUIC stream, chunk + send, verify checksum.
        Err(MigrationError::TransferFailed("phase-1 stub"))
    }

    /// Receive a snapshot from a transfer handle.
    ///
    /// # Errors
    /// Always returns [`MigrationError::TransferFailed`] in Phase 0.
    pub fn recv(&self, _handle: u64) -> Result<MigrationSnapshotRef, MigrationError> {
        // TODO(phase-1): read chunks, reassemble, verify checksum.
        Err(MigrationError::TransferFailed("phase-1 stub"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn construct_state_transfer() {
        let st = StateTransfer::new(65_536);
        let _ = format!("{st:?}");
    }

    #[test]
    fn send_returns_stub_error() {
        let st = StateTransfer::new(65_536);
        let snap = MigrationSnapshotRef::default();
        let res = st.send(&snap, NodeId::new(2));
        assert!(matches!(res, Err(MigrationError::TransferFailed(_))));
    }

    #[test]
    fn recv_returns_stub_error() {
        let st = StateTransfer::new(65_536);
        let res = st.recv(42);
        assert!(matches!(res, Err(MigrationError::TransferFailed(_))));
    }
}
