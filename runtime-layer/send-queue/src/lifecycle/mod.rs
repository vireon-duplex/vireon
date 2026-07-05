//! In-flight tracking table (Layer 8).
//!
//! [`InFlightTable`] maps packet IDs to their lifecycle state so the
//! two-CQE `SEND_ZC` protocol is correctly tracked. The table is
//! direct-mapped by slot index — no hashing, O(1) per operation.

pub mod id;

pub use id::{MAX_SLOTS, decode_generation, decode_slot, encode_id};

use crate::queue::SlotState;

/// Per-slot in-flight metadata.
#[derive(Clone, Copy, Debug, Default)]
struct InFlightEntry {
    /// Generation when this entry was created. `0` with `state == Empty`
    /// means the slot is free.
    generation: u16,
    /// Current lifecycle state.
    state: SlotState,
    /// Timestamp (monotonic nanos) when the packet was submitted to the
    /// IO Layer. Used for RTT measurement.
    submitted_at_ns: u64,
    /// Number of retransmission attempts so far.
    retry_count: u8,
}

/// Direct-mapped table tracking packets through the `SEND_ZC` lifecycle.
///
/// Indexed by slot index (extracted from the encoded id). Each slot has
/// at most one in-flight packet, so direct mapping is safe — no aliasing.
pub struct InFlightTable {
    entries: Vec<InFlightEntry>,
}

impl InFlightTable {
    /// Creates a table sized for `capacity` slots.
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        let cap = capacity.min(MAX_SLOTS);
        Self {
            entries: vec![InFlightEntry::default(); cap],
        }
    }

    /// Records that packet `id` was submitted to the IO Layer at `now_ns`.
    ///
    /// Overwrites any previous entry for this slot (which should have been
    /// retired already — the caller is responsible for lifecycle discipline).
    pub fn track(&mut self, id: u64, now_ns: u64) {
        let slot = decode_slot(id) as usize;
        debug_assert!(slot < self.entries.len(), "slot {slot} out of range");
        if slot < self.entries.len() {
            self.entries[slot] = InFlightEntry {
                generation: decode_generation(id),
                state: SlotState::InFlight,
                submitted_at_ns: now_ns,
                retry_count: 0,
            };
        }
    }

    /// Checks whether `id` is currently tracked (matches generation and is
    /// not `Empty`).
    #[must_use]
    pub fn is_tracked(&self, id: u64) -> bool {
        self.entry_for(id)
            .map(|e| e.state != SlotState::Empty)
            .unwrap_or(false)
    }

    /// Returns the current state for `id`, or `None` if stale/untracked.
    #[must_use]
    pub fn state(&self, id: u64) -> Option<SlotState> {
        self.entry_for(id)
            .filter(|e| e.state != SlotState::Empty)
            .map(|e| e.state)
    }

    /// Transitions the packet to `new_state`.
    ///
    /// Returns `true` if the transition was applied (id is live), `false`
    /// if the id is stale or untracked.
    pub fn transition(&mut self, id: u64, new_state: SlotState) -> bool {
        let slot = decode_slot(id) as usize;
        let generation = decode_generation(id);
        if slot >= self.entries.len() {
            return false;
        }
        let entry = &mut self.entries[slot];
        if entry.generation != generation || entry.state == SlotState::Empty {
            return false;
        }
        entry.state = new_state;
        true
    }

    /// Marks `id` as retried, bumping `retry_count`.
    ///
    /// Returns the new retry count, or `None` if stale/untracked.
    pub fn bump_retry(&mut self, id: u64) -> Option<u8> {
        let slot = decode_slot(id) as usize;
        let generation = decode_generation(id);
        if slot >= self.entries.len() {
            return None;
        }
        let entry = &mut self.entries[slot];
        if entry.generation != generation || entry.state == SlotState::Empty {
            return None;
        }
        entry.retry_count = entry.retry_count.saturating_add(1);
        entry.state = SlotState::Retrying;
        Some(entry.retry_count)
    }

    /// Returns the retry count for `id`, or `None` if stale/untracked.
    #[must_use]
    pub fn retry_count(&self, id: u64) -> Option<u8> {
        self.entry_for(id)
            .filter(|e| e.state != SlotState::Empty)
            .map(|e| e.retry_count)
    }

    /// Returns the submission timestamp for `id`.
    #[must_use]
    pub fn submitted_at(&self, id: u64) -> Option<u64> {
        self.entry_for(id)
            .filter(|e| e.state != SlotState::Empty)
            .map(|e| e.submitted_at_ns)
    }

    /// Retires the entry for `id` (slot is fully reclaimed).
    ///
    /// Returns `true` if the entry was live and is now retired.
    pub fn retire(&mut self, id: u64) -> bool {
        let slot = decode_slot(id) as usize;
        let generation = decode_generation(id);
        if slot >= self.entries.len() {
            return false;
        }
        let entry = &mut self.entries[slot];
        if entry.generation != generation || entry.state == SlotState::Empty {
            return false;
        }
        *entry = InFlightEntry::default();
        true
    }

    /// Number of currently tracked (non-Empty) entries.
    #[must_use]
    pub fn tracked_count(&self) -> usize {
        self.entries
            .iter()
            .filter(|e| e.state != SlotState::Empty)
            .count()
    }

    /// Looks up the entry for `id`, verifying generation.
    fn entry_for(&self, id: u64) -> Option<&InFlightEntry> {
        let slot = decode_slot(id) as usize;
        let generation = decode_generation(id);
        let entry = self.entries.get(slot)?;
        if entry.generation != generation {
            return None;
        }
        Some(entry)
    }
}

impl std::fmt::Debug for InFlightTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InFlightTable")
            .field("capacity", &self.entries.len())
            .field("tracked", &self.tracked_count())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn track_and_retire() {
        let mut table = InFlightTable::new(64);
        let id = encode_id(5, 1);
        table.track(id, 1_000);
        assert!(table.is_tracked(id));
        assert_eq!(table.state(id), Some(SlotState::InFlight));
        assert!(table.retire(id));
        assert!(!table.is_tracked(id));
    }

    #[test]
    fn stale_generation_rejected() {
        let mut table = InFlightTable::new(64);
        let id1 = encode_id(3, 1);
        table.track(id1, 100);
        // Retire and re-track with new generation
        table.retire(id1);
        let id2 = encode_id(3, 2);
        table.track(id2, 200);
        // Old id should not be tracked
        assert!(!table.is_tracked(id1));
        assert!(table.is_tracked(id2));
    }

    #[test]
    fn transition_works() {
        let mut table = InFlightTable::new(64);
        let id = encode_id(10, 1);
        table.track(id, 0);
        assert!(table.transition(id, SlotState::Accepted));
        assert_eq!(table.state(id), Some(SlotState::Accepted));
    }

    #[test]
    fn transition_rejects_stale() {
        let mut table = InFlightTable::new(64);
        let id = encode_id(10, 5);
        assert!(!table.transition(id, SlotState::Accepted));
    }

    #[test]
    fn bump_retry_increments() {
        let mut table = InFlightTable::new(64);
        let id = encode_id(7, 1);
        table.track(id, 0);
        assert_eq!(table.bump_retry(id), Some(1));
        assert_eq!(table.bump_retry(id), Some(2));
        assert_eq!(table.retry_count(id), Some(2));
        assert_eq!(table.state(id), Some(SlotState::Retrying));
    }

    #[test]
    fn submitted_at_round_trips() {
        let mut table = InFlightTable::new(64);
        let id = encode_id(2, 3);
        table.track(id, 42_000);
        assert_eq!(table.submitted_at(id), Some(42_000));
    }

    #[test]
    fn tracked_count_reflects_state() {
        let mut table = InFlightTable::new(64);
        assert_eq!(table.tracked_count(), 0);
        let id1 = encode_id(0, 1);
        let id2 = encode_id(1, 1);
        table.track(id1, 0);
        table.track(id2, 0);
        assert_eq!(table.tracked_count(), 2);
        table.retire(id1);
        assert_eq!(table.tracked_count(), 1);
    }

    #[test]
    fn out_of_range_slot_returns_false() {
        let mut table = InFlightTable::new(4);
        let id = encode_id(100, 1); // slot 100 > capacity
        assert!(!table.is_tracked(id));
        assert!(!table.transition(id, SlotState::Accepted));
        assert!(!table.retire(id));
        assert_eq!(table.retry_count(id), None);
    }
}
