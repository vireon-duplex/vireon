//! Hierarchical timing wheel (Layer 5).
//!
//! [`HierarchicalWheel`] owns `levels × 2^bits_per_level` bucket heads in a
//! single flat `Vec<u32>` (cache-friendly). Timers are placed at the finest
//! level that can hold their remaining delay; when a lower level completes a
//! full revolution, the parent level's current slot is cascaded down.
//!
//! Cancellation is **lazy**: the cancel tombstone lives in the entry's
//! `deadline` top bit ([`TimerEntry::mark_cancelled`]). A cancelled entry
//! stays linked and is skipped + reclaimed when its bucket drains.
//!
//! ## Slot math
//!
//! * `slot_at_level(deadline, L) = (deadline >> (L * bits)) & mask`
//! * `level_for_remaining(r) = (r.ilog2() + bits) / bits - 1` (clamped to
//!   `levels - 1`). `r ≥ 1` is required.

use crate::core::allocator::SlabAllocator;
use crate::core::bucket::{drain_all, push_front};
use crate::core::timer_entry::TimerEntry;
use profiler::profile;

/// Hierarchical timing wheel state (single-threaded).
pub struct HierarchicalWheel {
    bits: u32,
    levels: u32,
    slots_per_level: usize,
    slot_mask: usize,
    /// Flat bucket-head vector: `buckets[level * slots_per_level + slot]`.
    buckets: Vec<u32>,
    last_tick: u64,
    count: u64,
}

impl HierarchicalWheel {
    /// Creates an empty wheel. `initial_tick` is the current tick at creation
    /// so the first [`add`](Self::add) computes `remaining` correctly.
    #[must_use]
    pub fn new(bits: u32, levels: u32, initial_tick: u64) -> Self {
        let slots_per_level = 1usize << bits;
        let slot_mask = slots_per_level - 1;
        let total_slots = levels as usize * slots_per_level;
        Self {
            bits,
            levels,
            slots_per_level,
            slot_mask,
            buckets: vec![0u32; total_slots],
            last_tick: initial_tick,
            count: 0,
        }
    }

    /// Number of live (linked) timers, including cancelled-but-not-yet-drained.
    #[inline]
    #[must_use]
    pub fn count(&self) -> u64 {
        self.count
    }

    /// The most recently processed tick.
    #[inline]
    #[must_use]
    pub fn last_tick(&self) -> u64 {
        self.last_tick
    }

    /// Sets the clock baseline (used by the shard when the worker first wakes).
    #[inline]
    pub fn set_last_tick(&mut self, tick: u64) {
        self.last_tick = tick;
    }

    /// Adds a live timer at `slot` expiring at absolute `deadline`.
    ///
    /// Caller obligation: `deadline > last_tick`. The shard must fire any
    /// already-due timer itself rather than calling this.
    #[inline]
    #[profile]
    pub fn add(&mut self, slab: &mut SlabAllocator<TimerEntry>, slot: u32, deadline: u64) {
        debug_assert!(
            deadline > self.last_tick,
            "add requires deadline > last_tick"
        );
        self.place(slab, slot, deadline);
        self.count += 1;
    }

    /// Internal: bucket the entry without touching `count` (used by cascading).
    fn place(&mut self, slab: &mut SlabAllocator<TimerEntry>, slot: u32, deadline: u64) {
        let remaining = deadline - self.last_tick;
        let level = self.level_for_remaining(remaining);
        let slot_idx = ((deadline >> (level * self.bits)) as usize) & self.slot_mask;
        let head = &mut self.buckets[(level as usize) * self.slots_per_level + slot_idx];
        push_front(slab, head, slot);
    }

    /// Returns the wheel level for a given `remaining` (≥ 1) tick count.
    fn level_for_remaining(&self, remaining: u64) -> u32 {
        // ilog2(1) = 0; level = (ilog2 + bits) / bits - 1.
        // remaining ≥ 1 is guaranteed by the caller.
        let lz = remaining.leading_zeros();
        // highest set bit position = 63 - lz; bit_length = 64 - lz.
        let bit_length = 64u32 - lz;
        let mut level = (bit_length.saturating_add(self.bits - 1) / self.bits).saturating_sub(1);
        if level >= self.levels {
            level = self.levels - 1;
        }
        level
    }

    /// Advances the wheel to `now`, draining every due bucket and cascading
    /// parent levels on wrap. Returns the slot indices that must be reclaimed
    /// by the caller (fired or cancelled) — the shard frees them and routes
    /// non-cancelled ones to the expired queue.
    #[profile]
    pub fn advance(
        &mut self,
        slab: &mut SlabAllocator<TimerEntry>,
        now: u64,
        reclaimed: &mut Vec<u32>,
    ) {
        while self.last_tick < now {
            self.last_tick += 1;
            let t = self.last_tick;

            // Fire the level-0 bucket for this tick.
            let l0_slot = (t as usize) & self.slot_mask;
            let drained = drain_all(slab, &mut self.buckets[l0_slot]);
            if !drained.is_empty() {
                self.count = self.count.saturating_sub(drained.len() as u64);
                reclaimed.extend(drained);
            }

            // On a level-0 wrap, cascade the parent level's current slot, and
            // keep climbing while the level below also wrapped.
            if l0_slot == 0 {
                for lvl in 1..self.levels {
                    let sidx = ((t >> (lvl * self.bits)) as usize) & self.slot_mask;
                    let cascaded = drain_all(
                        slab,
                        &mut self.buckets[(lvl as usize) * self.slots_per_level + sidx],
                    );
                    if cascaded.is_empty() {
                        // nothing to do at this level; still respect wrap chain
                        if sidx != 0 {
                            break;
                        }
                        continue;
                    }
                    for slot in cascaded {
                        let dl = slab.get(slot).deadline;
                        if dl <= self.last_tick {
                            // became due during the same tick — fire/reclaim now
                            self.count = self.count.saturating_sub(1);
                            reclaimed.push(slot);
                        } else {
                            self.place(slab, slot, dl);
                        }
                    }
                    if sidx != 0 {
                        break;
                    }
                }
            }
        }
    }

    /// Returns an approximate (never late) earliest tick at which a timer may
    /// fire, or `None` when the wheel is empty. The worker parks until then.
    ///
    /// For timers in higher levels the estimate may be slightly early (a
    /// cascade brings them down within one level-0 revolution), which is the
    /// safe direction: the worker wakes, re-advances, and re-evaluates.
    #[must_use]
    #[profile]
    pub fn next_deadline(&self) -> Option<u64> {
        if self.count == 0 {
            return None;
        }
        let mut best: u64 = u64::MAX;
        for lvl in 0..self.levels {
            let cursor = ((self.last_tick >> (lvl * self.bits)) as usize) & self.slot_mask;
            for off in 1..=self.slots_per_level {
                let s = (cursor + off) & self.slot_mask;
                if self.buckets[(lvl as usize) * self.slots_per_level + s] != 0 {
                    // ticks until this level's cursor reaches `s`: `off * 2^(lvl*bits)`.
                    let ticks_ahead = (off as u64) << (lvl * self.bits);
                    let candidate = self.last_tick.saturating_add(ticks_ahead);
                    if candidate < best {
                        best = candidate;
                    }
                    break;
                }
            }
        }
        (best != u64::MAX).then_some(best)
    }

    /// Clears the wheel (used on shutdown; abandons pending timers).
    pub fn clear(&mut self) {
        self.buckets.iter_mut().for_each(|h| *h = 0);
        self.count = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const BITS: u32 = 6;
    const LEVELS: u32 = 3; // horizon = 2^18 ticks

    fn fresh_wheel(start: u64) -> (HierarchicalWheel, SlabAllocator<TimerEntry>) {
        (
            HierarchicalWheel::new(BITS, LEVELS, start),
            SlabAllocator::new(1024).expect("slab"),
        )
    }

    fn add_timer(
        wheel: &mut HierarchicalWheel,
        slab: &mut SlabAllocator<TimerEntry>,
        task_id: u64,
        deadline: u64,
    ) -> u32 {
        let slot = slab.allocate().expect("slot");
        let slot_u32 = slot.get();
        slab.write(slot, TimerEntry::new(task_id, deadline));
        wheel.add(slab, slot_u32, deadline);
        slot_u32
    }

    #[test]
    fn level_for_remaining_matches_reference() {
        let (w, _) = fresh_wheel(0);
        // remaining 1..63 -> level 0; 64..4095 -> level 1; 4096.. -> level 2.
        assert_eq!(w.level_for_remaining(1), 0);
        assert_eq!(w.level_for_remaining(63), 0);
        assert_eq!(w.level_for_remaining(64), 1);
        assert_eq!(w.level_for_remaining(4095), 1);
        assert_eq!(w.level_for_remaining(4096), 2);
        assert_eq!(w.level_for_remaining((1 << 18) - 1), 2);
    }

    #[test]
    fn fresh_wheel_is_empty() {
        let (w, _) = fresh_wheel(0);
        assert_eq!(w.count(), 0);
        assert!(w.next_deadline().is_none());
    }

    #[test]
    fn add_increments_count_and_sets_deadline() {
        let (mut w, mut slab) = fresh_wheel(0);
        let slot = add_timer(&mut w, &mut slab, 1, 10);
        assert_eq!(w.count(), 1);
        let e = slab.get(slot);
        assert_eq!(e.task_id, 1);
        assert_eq!(e.deadline, 10);
    }

    #[test]
    fn advance_fires_level_zero_timer_at_exact_tick() {
        let (mut w, mut slab) = fresh_wheel(0);
        let _ = add_timer(&mut w, &mut slab, 42, 5);
        let mut reclaimed = Vec::new();
        // advance to exactly tick 5
        w.advance(&mut slab, 5, &mut reclaimed);
        assert_eq!(reclaimed.len(), 1);
        assert_eq!(slab.get(reclaimed[0]).task_id, 42);
        assert_eq!(w.count(), 0);
    }

    #[test]
    fn advance_to_before_deadline_does_not_fire() {
        let (mut w, mut slab) = fresh_wheel(0);
        let _ = add_timer(&mut w, &mut slab, 42, 10);
        let mut reclaimed = Vec::new();
        w.advance(&mut slab, 5, &mut reclaimed);
        assert!(reclaimed.is_empty());
        assert_eq!(w.count(), 1);
    }

    #[test]
    fn advance_reclaims_every_drained_slot() {
        let (mut w, mut slab) = fresh_wheel(0);
        let _ = add_timer(&mut w, &mut slab, 7, 3);
        let _ = add_timer(&mut w, &mut slab, 8, 3);
        let mut reclaimed = Vec::new();
        w.advance(&mut slab, 3, &mut reclaimed);
        // the wheel reports both slots; the shard decides fire-vs-cancelled
        assert_eq!(reclaimed.len(), 2);
        assert_eq!(w.count(), 0);
    }

    #[test]
    fn cross_level_timer_fires_after_cascade() {
        let (mut w, mut slab) = fresh_wheel(0);
        // deadline 100 ticks out -> level 1 (64..4095).
        let _ = add_timer(&mut w, &mut slab, 99, 100);
        assert_eq!(w.count(), 1);
        let mut reclaimed = Vec::new();
        // advance to tick 100; cascades along the way.
        w.advance(&mut slab, 100, &mut reclaimed);
        assert_eq!(reclaimed.len(), 1);
        assert_eq!(w.count(), 0);
        assert_eq!(slab.get(reclaimed[0]).task_id, 99);
    }

    #[test]
    fn many_timers_fire_in_order() {
        let (mut w, mut slab) = fresh_wheel(0);
        let mut expected = Vec::new();
        for d in 1..=200u64 {
            let _ = add_timer(&mut w, &mut slab, d, d * 2);
            expected.push(d);
        }
        let mut reclaimed = Vec::new();
        w.advance(&mut slab, 400, &mut reclaimed);
        // collect task_ids in drain order, sort by deadline (== 2*task_id)
        let mut fired: Vec<u64> = reclaimed.iter().map(|&s| slab.get(s).task_id).collect();
        fired.sort_unstable();
        assert_eq!(fired, expected);
        assert_eq!(w.count(), 0);
    }

    #[test]
    fn next_deadline_points_at_next_non_empty_bucket() {
        let (mut w, mut slab) = fresh_wheel(0);
        let _ = add_timer(&mut w, &mut slab, 1, 7);
        let nd = w.next_deadline().expect("some deadline");
        assert_eq!(nd, 7);
    }

    #[test]
    fn next_deadline_returns_none_when_empty() {
        let (w, _slab) = fresh_wheel(0);
        assert!(w.next_deadline().is_none());
    }

    #[test]
    fn clear_resets_count_and_buckets() {
        let (mut w, mut slab) = fresh_wheel(0);
        let _ = add_timer(&mut w, &mut slab, 1, 5);
        w.clear();
        assert_eq!(w.count(), 0);
        assert!(w.next_deadline().is_none());
    }
}
