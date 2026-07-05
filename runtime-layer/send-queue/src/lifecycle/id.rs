//! ID encoding for send-queue packets (Layer 8 — helpers).
//!
//! Each submitted packet needs a unique `user_data` identifier so that
//! CQEs can be correlated back to the originating slot. The identifier
//! packs a **slot index** (position within the ring's backing array) and
//! a **generation** counter (incremented on each slot reuse) into a
//! single `u64`.
//!
//! ```text
//! id (low 60 bits — fits within the IO Layer's ID_MASK)
//! ┌─────────────────┬──────────────┐
//! │   generation    │  slot_index  │
//! │    (16 bits)    │   (20 bits)  │
//! └─────────────────┴──────────────┘
//!  bit 36          20            0
//! ```
//!
//! The IO Layer prepends `OpKind` into bits [63:60] via its own `encode`
//! function, so the send-queue only owns the low 60 bits.

/// Bits used for the slot index.
pub const SLOT_BITS: u32 = 20;
/// Maximum number of slots addressable by the ID encoding (1 048 576).
pub const MAX_SLOTS: usize = 1 << SLOT_BITS;
/// Bit shift for the generation field.
pub const GEN_SHIFT: u32 = SLOT_BITS;
/// Mask for the slot index field.
pub const SLOT_MASK: u64 = (1 << SLOT_BITS) - 1;
/// Mask for the generation field (16 bits).
pub const GEN_MASK: u64 = 0xFFFF;

/// Encodes `(slot_index, generation)` into a single `u64` id.
///
/// This value is passed to the IO Layer as the `id` parameter; the IO
/// Layer then prepends `OpKind` to produce the final `user_data`.
#[inline]
#[must_use]
pub const fn encode_id(slot_index: u16, generation: u16) -> u64 {
    ((generation as u64 & GEN_MASK) << GEN_SHIFT) | (slot_index as u64 & SLOT_MASK)
}

/// Extracts the slot index from an encoded id.
#[inline]
#[must_use]
pub const fn decode_slot(id: u64) -> u16 {
    (id & SLOT_MASK) as u16
}

/// Extracts the generation from an encoded id.
#[inline]
#[must_use]
pub const fn decode_generation(id: u64) -> u16 {
    ((id >> GEN_SHIFT) & GEN_MASK) as u16
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
        // slot_index is u16, so the practical max is u16::MAX.
        let max = u16::MAX; // 65 535
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
        // never exceeds this.
        let id = encode_id(u16::MAX, 0xFFFF);
        assert!(id < (1u64 << 60));
    }

    #[test]
    fn slot_and_generation_do_not_overlap() {
        for slot in [0u16, 1, 100, u16::MAX] {
            for generation in [0u16, 1, 100, 0xFFFF] {
                let id = encode_id(slot, generation);
                assert_eq!(decode_slot(id), slot, "slot mismatch");
                assert_eq!(decode_generation(id), generation, "gen mismatch");
            }
        }
    }
}
