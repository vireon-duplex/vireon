//! [`ConnIdTable`] — CID → [`crate::connection::QuicConnection`] lookup.
//!
//! Phase 1 — Connection Manager. See `DESIGN.md` §4.1.
//!
//! The hot-path structure for incoming packets: decode the DCID
//! from the short header, hash it, find the owning connection.
//! O(1) amortized, single-threaded per shard (no locks).
//!
//! Phase 1 implementation will use:
//! - `rustc-hash::FxHashMap` for the inner map (faster than
//!   `std::HashMap` for small keys, default in the reference impl)
//! - Inline 8-byte server CIDs (no heap allocation per lookup)
//! - A separate `retired` sub-table for the CID retirement grace
//!   window (RFC 9000 §5.1.2)
//!
//! For Phase 0 we ship only the type so other modules can reference
/// it. The actual lookup, insertion, retirement, and GC land in
/// Phase 1 along with the connection slab.
use crate::cid::QuicCid;

/// Placeholder slot index. In Phase 1 this becomes an index into the
/// engine's connection slab (analogous to
/// `connection_shard::ConnectionId`).
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct ConnSlot(pub u32);

/// Phase-0 type skeleton. Phase 1 will replace the inner field with
/// a real FxHashMap and add `lookup()`, `insert()`, `retire()`,
/// `drain_retired()`.
#[derive(Default)]
pub struct ConnIdTable {
    /// Number of entries currently in the table. Used for capacity
    /// reporting until the real map lands.
    len: usize,
}

impl ConnIdTable {
    #[must_use]
    pub const fn new() -> Self {
        Self { len: 0 }
    }

    /// Current entry count.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.len
    }

    /// Whether the table has no entries.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// TODO(phase-1): `lookup(&self, dcid: &[u8]) -> Option<ConnSlot>`
    /// with FxHashMap + inline-key optimization. Placeholder for now.
    #[must_use]
    pub fn _lookup_placeholder(&self, _dcid: &QuicCid) -> Option<ConnSlot> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn placeholder() {
        let t = ConnIdTable::new();
        assert!(t.is_empty());
        // TODO(phase-1): populate via insert(), benchmark lookup
        // latency vs FxHashMap baseline.
    }
}
