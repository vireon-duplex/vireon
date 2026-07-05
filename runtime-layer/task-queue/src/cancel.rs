//! CancellationRegistry — bitmap of cancelled slot indices (Layer 13).
//!
//! Cancellation is **lazy**: [`TaskHandle::cancel`](crate::handle::TaskHandle::cancel)
//! sets the shared atomic bool on the handle; the executor checks
//! `is_cancelled(slot_idx)` before running each task. Tasks that are
//! flagged cancelled are skipped and recycled.
//!
//! This registry complements the per-task `cancel_flag` with a
//! bitmap-based path for O(1) bulk checks during batch execution.
//! Each slot's `cancel_flag` is the authoritative source; the bitmap
//! is a cache of those flags, refreshed on each `tick`.

use crate::pool::TaskPool;

/// Bitmap of cancelled slot indices. One bit per slot.
///
/// For a capacity of 4096 this is 512 bytes — fits comfortably in L1.
pub struct CancellationRegistry {
    bits: Vec<u64>,
    capacity: usize,
}

impl CancellationRegistry {
    /// Creates a registry sized for `capacity` slots.
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        let words = capacity.div_ceil(64).max(1);
        Self {
            bits: vec![0u64; words],
            capacity,
        }
    }

    /// Returns the configured slot capacity.
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    #[inline]
    fn word_bit(slot_idx: u32) -> (usize, u64) {
        let word = (slot_idx as usize) / 64;
        let bit = 1u64 << ((slot_idx as usize) % 64);
        (word, bit)
    }

    /// Marks `slot_idx` as cancelled.
    pub fn cancel(&mut self, slot_idx: u32) {
        if (slot_idx as usize) >= self.capacity {
            return;
        }
        let (word, bit) = Self::word_bit(slot_idx);
        self.bits[word] |= bit;
    }

    /// Returns `true` if `slot_idx` has been marked cancelled.
    #[must_use]
    pub fn is_cancelled(&self, slot_idx: u32) -> bool {
        if (slot_idx as usize) >= self.capacity {
            return false;
        }
        let (word, bit) = Self::word_bit(slot_idx);
        (self.bits[word] & bit) != 0
    }

    /// Clears the cancellation flag for `slot_idx` (called when the
    /// slot is recycled).
    pub fn clear(&mut self, slot_idx: u32) {
        if (slot_idx as usize) >= self.capacity {
            return;
        }
        let (word, bit) = Self::word_bit(slot_idx);
        self.bits[word] &= !bit;
    }

    /// Refreshes the bitmap from the pool's per-slot cancel flags.
    /// Called at the start of each tick.
    pub fn refresh_from(&mut self, pool: &TaskPool) {
        // Walk free-list hint lazily: we check each slot's flag.
        // In production this would be a tighter scan, but the pool
        // owns the per-slot `Arc<AtomicBool>`. For Phase 1 we rely on
        // the per-task handle cancel + executor's pre-check via the
        // same flag. The bitmap is a secondary cache for batched
        // queries during shutdown.
        for slot_idx in 0..self.capacity {
            if let Some(slot) = pool.get(slot_idx as u32) {
                let cancelled = slot.cancel_flag.load(std::sync::atomic::Ordering::Acquire);
                let (word, bit) = Self::word_bit(slot_idx as u32);
                if cancelled {
                    self.bits[word] |= bit;
                } else {
                    self.bits[word] &= !bit;
                }
            }
        }
    }

    /// Clears all bits.
    pub fn clear_all(&mut self) {
        for w in &mut self.bits {
            *w = 0;
        }
    }
}

impl std::fmt::Debug for CancellationRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CancellationRegistry")
            .field("capacity", &self.capacity)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_test_clear() {
        let mut reg = CancellationRegistry::new(256);
        assert!(!reg.is_cancelled(0));
        assert!(!reg.is_cancelled(100));
        reg.cancel(0);
        reg.cancel(100);
        assert!(reg.is_cancelled(0));
        assert!(reg.is_cancelled(100));
        assert!(!reg.is_cancelled(1));
        reg.clear(0);
        assert!(!reg.is_cancelled(0));
        assert!(reg.is_cancelled(100));
    }

    #[test]
    fn out_of_range_is_safe() {
        let reg = CancellationRegistry::new(64);
        assert!(!reg.is_cancelled(9999));
    }

    #[test]
    fn cross_word_boundary() {
        let mut reg = CancellationRegistry::new(200);
        reg.cancel(63); // word 0, last bit
        reg.cancel(64); // word 1, first bit
        reg.cancel(128);
        assert!(reg.is_cancelled(63));
        assert!(reg.is_cancelled(64));
        assert!(reg.is_cancelled(128));
        assert!(!reg.is_cancelled(127));
    }

    #[test]
    fn clear_all_resets() {
        let mut reg = CancellationRegistry::new(128);
        reg.cancel(1);
        reg.cancel(64);
        reg.clear_all();
        assert!(!reg.is_cancelled(1));
        assert!(!reg.is_cancelled(64));
    }
}
