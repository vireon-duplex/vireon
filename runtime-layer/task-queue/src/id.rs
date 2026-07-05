//! ID encoding for Task Queue (Layer 3).
//!
//! Each tracked task is identified by a 64-bit [`TaskId`] that packs a
//! **slot index** (position within the slab) and a **generation**
//! counter (incremented on each slot reuse). This mirrors the Send
//! Queue's packet-id encoding for cross-module consistency.
//!
//! ```text
//! TaskId (low 60 bits — fits within the IO Layer's ID_MASK if ever
//! correlated with IO ops)
//! ┌─────────────────┬──────────────┐
//! │   generation    │  slot_index  │
//! │    (16 bits)    │   (20 bits)  │
//! └─────────────────┴──────────────┘
//!  bit 36          20            0
//! ```

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

/// Opaque task identifier — packed `(slot_index, generation)`.
///
/// `Copy`, `Eq`, cheap to pass by value. The `raw()` accessor returns
/// the underlying `u64` for storage in queues and registries.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct TaskId(u64);

impl TaskId {
    /// Creates a `TaskId` from a raw encoded `u64`.
    #[must_use]
    pub const fn from_raw(raw: u64) -> Self {
        Self(raw)
    }

    /// Returns the raw encoded `u64`.
    #[must_use]
    pub const fn raw(self) -> u64 {
        self.0
    }

    /// Returns `true` if the id is the nil sentinel (slot 0, gen 0).
    #[must_use]
    pub const fn is_nil(self) -> bool {
        self.0 == 0
    }
}

/// The nil `TaskId` — slot 0 generation 0. Useful as a sentinel.
pub const NIL_ID: TaskId = TaskId::from_raw(0);

/// Encodes `(slot_index, generation)` into a raw `u64`.
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

/// Builds a [`TaskId`] from `(slot_index, generation)`.
#[inline]
#[must_use]
pub const fn make_id(slot_index: u32, generation: u16) -> TaskId {
    TaskId::from_raw(encode_id(slot_index, generation))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_zero() {
        let id = make_id(0, 0);
        assert_eq!(decode_slot(id.raw()), 0);
        assert_eq!(decode_generation(id.raw()), 0);
        assert!(id.is_nil());
    }

    #[test]
    fn round_trip_max_slot() {
        let max = (1u32 << SLOT_BITS) - 1;
        let id = make_id(max, 0);
        assert_eq!(decode_slot(id.raw()), max);
        assert_eq!(decode_generation(id.raw()), 0);
    }

    #[test]
    fn round_trip_max_generation() {
        let id = make_id(0, 0xFFFF);
        assert_eq!(decode_slot(id.raw()), 0);
        assert_eq!(decode_generation(id.raw()), 0xFFFF);
    }

    #[test]
    fn round_trip_combined() {
        let id = make_id(137, 42);
        assert_eq!(decode_slot(id.raw()), 137);
        assert_eq!(decode_generation(id.raw()), 42);
    }

    #[test]
    fn fits_in_60_bits() {
        let max_slot = (1u32 << SLOT_BITS) - 1;
        let id = make_id(max_slot, 0xFFFF);
        assert!(id.raw() < (1u64 << 60));
    }

    #[test]
    fn nil_id() {
        assert!(NIL_ID.is_nil());
        assert!(!make_id(1, 0).is_nil());
    }
}
