//! [`SnapshotMetadata`], [`MigrationSnapshotRef`] — session snapshot
//! descriptors for migration.
//!
//! See `DESIGN.md` Part 3 (session snapshot).
//!
//! # Role
//!
//! A snapshot captures the complete serializable state of a session
//! at a point in time: stream state, flow-control windows, sequence
//! numbers, pending messages, etc. The `SnapshotMetadata` records
//! the provenance and integrity of that snapshot, while
//! `MigrationSnapshotRef` adds a handle for retrieving the actual
//! bytes from the transfer layer.

use session_manager::SessionId;
use session_registry::{Generation, NodeId};

// ── SnapshotMetadata ────────────────────────────────────────────

/// Provenance and integrity metadata for a session snapshot.
///
/// This struct is intentionally `Copy` (48 bytes) so it can be
/// passed around without allocation. The actual snapshot bytes are
/// referenced by the [`MigrationSnapshotRef::handle`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SnapshotMetadata {
    /// The session this snapshot belongs to.
    pub session_id: SessionId,
    /// The node that captured the snapshot.
    pub source_node: NodeId,
    /// Nanoseconds since cluster epoch when the snapshot was taken.
    pub captured_ns: u64,
    /// Serialized snapshot size in bytes.
    pub size_bytes: u64,
    /// Number of active streams at capture time.
    pub stream_count: u16,
    /// Ownership generation at capture time.
    pub generation: Generation,
    /// Checksum (CRC64 or similar) of the serialized snapshot.
    pub checksum: u64,
}

impl SnapshotMetadata {
    /// Construct new metadata.
    #[must_use]
    pub const fn new(
        session_id: SessionId,
        source_node: NodeId,
        captured_ns: u64,
        size_bytes: u64,
        stream_count: u16,
        generation: Generation,
        checksum: u64,
    ) -> Self {
        Self {
            session_id,
            source_node,
            captured_ns,
            size_bytes,
            stream_count,
            generation,
            checksum,
        }
    }

    /// Whether the metadata describes a valid (non-empty) snapshot.
    ///
    /// A snapshot is valid if it has a non-nil session id, a non-nil
    /// source node, and non-zero size and checksum.
    #[must_use]
    pub const fn is_valid(self) -> bool {
        !self.session_id.is_nil()
            && !self.source_node.is_nil()
            && self.size_bytes > 0
            && self.checksum != 0
    }

    /// Convenience accessor for the session id.
    #[must_use]
    pub const fn session_id(self) -> SessionId {
        self.session_id
    }
}

// ── MigrationSnapshotRef ────────────────────────────────────────

/// A reference to a captured migration snapshot.
///
/// Combines the snapshot's [`SnapshotMetadata`] with a `handle` that
/// the transfer layer uses to look up the actual bytes. The handle
/// is opaque to the coordinator.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MigrationSnapshotRef {
    /// Metadata describing the snapshot.
    pub metadata: SnapshotMetadata,
    /// Opaque handle for retrieving snapshot bytes from the transfer
    /// layer. Zero means "no handle assigned".
    pub handle: u64,
}

impl MigrationSnapshotRef {
    /// Construct a new snapshot reference.
    #[must_use]
    pub const fn new(metadata: SnapshotMetadata, handle: u64) -> Self {
        Self { metadata, handle }
    }

    /// Whether the reference describes a valid snapshot with a
    /// non-zero handle.
    #[must_use]
    pub const fn is_valid(self) -> bool {
        self.metadata.is_valid() && self.handle != 0
    }

    /// Convenience accessor for the session id.
    #[must_use]
    pub const fn session_id(self) -> SessionId {
        self.metadata.session_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_metadata() -> SnapshotMetadata {
        SnapshotMetadata::new(
            SessionId::from_pair(1, 1),
            NodeId::new(1),
            5_000_000_000,
            4_096,
            3,
            Generation::new(2),
            0xABCD_1234,
        )
    }

    #[test]
    fn valid_metadata_passes_is_valid() {
        let m = sample_metadata();
        assert!(m.is_valid());
    }

    #[test]
    fn nil_session_fails_is_valid() {
        let mut m = sample_metadata();
        m.session_id = SessionId::nil();
        assert!(!m.is_valid());
    }

    #[test]
    fn nil_source_fails_is_valid() {
        let mut m = sample_metadata();
        m.source_node = NodeId::NIL;
        assert!(!m.is_valid());
    }

    #[test]
    fn zero_size_fails_is_valid() {
        let mut m = sample_metadata();
        m.size_bytes = 0;
        assert!(!m.is_valid());
    }

    #[test]
    fn zero_checksum_fails_is_valid() {
        let mut m = sample_metadata();
        m.checksum = 0;
        assert!(!m.is_valid());
    }

    #[test]
    fn snapshot_ref_is_valid_with_handle() {
        let m = sample_metadata();
        let r = MigrationSnapshotRef::new(m, 42);
        assert!(r.is_valid());
        assert_eq!(r.session_id(), SessionId::from_pair(1, 1));
    }

    #[test]
    fn snapshot_ref_invalid_without_handle() {
        let m = sample_metadata();
        let r = MigrationSnapshotRef::new(m, 0);
        assert!(!r.is_valid());
    }

    #[test]
    fn default_metadata_is_invalid() {
        let m = SnapshotMetadata::default();
        assert!(!m.is_valid());
    }
}
