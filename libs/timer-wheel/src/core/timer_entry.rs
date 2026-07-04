//! Compact timer record (Layer 4).
//!
//! [`TimerEntry`] is exactly **24 bytes** so that ~1 million outstanding
//! timers occupy ~24 MB. Cancellation is tracked externally (a per-shard
//! `HashSet<TaskId>` drained at fire time), so the entry itself stays a plain
//! record with no state bits.

/// Compact, `Copy` record for one outstanding timer.
///
/// # Layout
///
/// `#[repr(C)]` pins the layout so the size assertion in the tests is
/// meaningful across compilers:
///
/// | offset | field | type |
/// |--------|-------|------|
/// | 0 | `deadline` | `u64` (absolute expiry tick) |
/// | 8 | `task_id` | `u64` |
/// | 16 | `prev` | `u32` (0 = null link) |
/// | 20 | `next` | `u32` (0 = null link) |
#[repr(C)]
#[derive(Clone, Copy)]
pub struct TimerEntry {
    /// Absolute expiry tick. Bounded by the wheel horizon
    /// (`bits_per_level * levels <= 62`), so always well below `u64::MAX`.
    pub deadline: u64,
    /// Application-supplied identifier returned via `drain_expired`.
    pub task_id: u64,
    /// Index of the previous entry in the bucket's linked list (0 = null).
    pub prev: u32,
    /// Index of the next entry in the bucket's linked list (0 = null).
    pub next: u32,
}

impl TimerEntry {
    /// Creates a fresh, unlinked entry.
    #[inline]
    #[must_use]
    pub const fn new(task_id: u64, deadline: u64) -> Self {
        Self {
            deadline,
            task_id,
            prev: 0,
            next: 0,
        }
    }

    /// True when this entry has no bucket links (not currently linked).
    #[inline]
    #[must_use]
    pub const fn is_unlinked(self) -> bool {
        self.prev == 0 && self.next == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::mem::size_of;

    #[test]
    fn entry_is_exactly_24_bytes() {
        assert_eq!(size_of::<TimerEntry>(), 24);
    }

    #[test]
    fn new_entry_is_unlinked() {
        let e = TimerEntry::new(7, 1234);
        assert_eq!(e.task_id, 7);
        assert_eq!(e.deadline, 1234);
        assert!(e.is_unlinked());
    }

    #[test]
    fn realistic_horizon_deadline_fits() {
        // Even a 2^60-tick horizon is comfortably below u64::MAX.
        let e = TimerEntry::new(0, (1u64 << 60) - 1);
        assert_eq!(e.deadline, (1u64 << 60) - 1);
    }
}
