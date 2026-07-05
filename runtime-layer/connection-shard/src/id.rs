//! [`ConnectionId`] encoding (Layer 4).
//!
//! Each connection is identified by a 64-bit ID that packs a **slot
//! index** (position in the per-shard slab) and a **generation** counter
//! (incremented on each slot reuse to prevent ABA problems).
//!
//! ```text
//! ┌─────────────────┬──────────────┐
//! │   generation    │  slot_index  │
//! │    (16 bits)    │   (20 bits)  │
//! └─────────────────┴──────────────┘
//!  bit 36          20            0
//! ```
//!
//! The encoding is identical to the send-queue and task-queue patterns,
//! keeping the workspace consistent. Only the low 36 bits are used; the
//! high 28 bits are zero, leaving room for the IO Layer's `OpKind` tag
//! if this id ever needs to flow through io_uring `user_data`.

/// Bits used for the slot index field.
pub const SLOT_BITS: u32 = 20;
/// Maximum number of slots addressable by the encoding (1 048 576).
pub const MAX_SLOTS: usize = 1 << SLOT_BITS;
/// Bit shift for the generation field.
pub const GEN_SHIFT: u32 = SLOT_BITS;
/// Mask for the slot index field.
pub const SLOT_MASK: u64 = (1 << SLOT_BITS) - 1;
/// Mask for the generation field (16 bits).
pub const GEN_MASK: u64 = 0xFFFF;

/// A globally-unique connection identifier within a shard.
///
/// Wraps the raw encoded `u64` to prevent accidental misuse. Use
/// [`decode`](Self::decode) to extract the slot and generation, or
/// [`raw`](Self::raw) to obtain the underlying bits for logging.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ConnectionId(u64);

impl ConnectionId {
    /// Creates an id from encoded raw bits.
    #[inline]
    #[must_use]
    pub const fn from_raw(raw: u64) -> Self {
        Self(raw)
    }

    /// Returns the raw encoded bits.
    #[inline]
    #[must_use]
    pub const fn raw(self) -> u64 {
        self.0
    }

    /// Whether this is the nil id (slot 0, generation 0).
    #[inline]
    #[must_use]
    pub const fn is_nil(self) -> bool {
        self.0 == 0
    }

    /// Decodes the slot index from this id.
    #[inline]
    #[must_use]
    pub const fn slot(self) -> u32 {
        decode_slot(self.0)
    }

    /// Decodes the generation from this id.
    #[inline]
    #[must_use]
    pub const fn generation(self) -> u16 {
        decode_generation(self.0)
    }
}

/// The nil connection id (slot 0, generation 0). Used as a sentinel.
pub const NIL_ID: ConnectionId = ConnectionId::from_raw(0);

/// Encodes `(slot_index, generation)` into a single `u64`.
///
/// `slot_index` must be less than [`MAX_SLOTS`]; `generation` wraps at
/// 65 536 (16 bits).
#[inline]
#[must_use]
pub const fn encode_id(slot_index: u32, generation: u16) -> u64 {
    ((generation as u64 & GEN_MASK) << GEN_SHIFT) | ((slot_index as u64) & SLOT_MASK)
}

/// Extracts the slot index from an encoded id.
#[inline]
#[must_use]
pub const fn decode_slot(id: u64) -> u32 {
    (id & SLOT_MASK) as u32
}

/// Extracts the generation from an encoded id.
#[inline]
#[must_use]
pub const fn decode_generation(id: u64) -> u16 {
    ((id >> GEN_SHIFT) & GEN_MASK) as u16
}

/// Constructs a [`ConnectionId`] from `(slot_index, generation)`.
#[inline]
#[must_use]
pub const fn make_id(slot_index: u32, generation: u16) -> ConnectionId {
    ConnectionId::from_raw(encode_id(slot_index, generation))
}

impl std::fmt::Display for ConnectionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "conn#{}/{}", self.slot(), self.generation())
    }
}

impl Default for ConnectionId {
    #[inline]
    fn default() -> Self {
        NIL_ID
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_zero() {
        let id = encode_id(0, 0);
        assert_eq!(decode_slot(id), 0);
        assert_eq!(decode_generation(id), 0);
    }

    #[test]
    fn round_trip_max_slot() {
        let max = (MAX_SLOTS - 1) as u32;
        let id = encode_id(max, 0);
        assert_eq!(decode_slot(id), max);
        assert_eq!(decode_generation(id), 0);
    }

    #[test]
    fn round_trip_max_generation() {
        let id = encode_id(0, 0xFFFF);
        assert_eq!(decode_slot(id), 0);
        assert_eq!(decode_generation(id), 0xFFFF);
    }

    #[test]
    fn round_trip_combined() {
        let id = encode_id(137, 42);
        assert_eq!(decode_slot(id), 137);
        assert_eq!(decode_generation(id), 42);
    }

    #[test]
    fn fits_in_60_bits() {
        // The IO Layer's ID_MASK is (1 << 60) - 1. Verify our encoding
        // never exceeds this (in case it ever flows through user_data).
        let id = encode_id((MAX_SLOTS - 1) as u32, 0xFFFF);
        assert!(id < (1u64 << 60));
    }

    #[test]
    fn slot_and_generation_do_not_overlap() {
        for slot in [0u32, 1, 100, (MAX_SLOTS - 1) as u32] {
            for generation in [0u16, 1, 100, 0xFFFF] {
                let id = encode_id(slot, generation);
                assert_eq!(decode_slot(id), slot, "slot mismatch");
                assert_eq!(decode_generation(id), generation, "gen mismatch");
            }
        }
    }

    #[test]
    fn connection_id_methods() {
        let id = make_id(256, 7);
        assert_eq!(id.slot(), 256);
        assert_eq!(id.generation(), 7);
        assert!(!id.is_nil());
        assert!(!NIL_ID.is_nil() || NIL_ID.raw() == 0);
    }

    #[test]
    fn nil_id_is_zero() {
        assert_eq!(NIL_ID.raw(), 0);
        assert!(NIL_ID.is_nil());
    }

    #[test]
    fn display_format() {
        let id = make_id(42, 3);
        assert_eq!(format!("{id}"), "conn#42/3");
    }
}
