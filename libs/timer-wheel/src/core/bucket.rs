//! Intrusive doubly-linked-list bucket operations (Layer 5).
//!
//! A "bucket" is just a `u32` head index stored inside the wheel's bucket
//! vector. These free functions manipulate the [`TimerEntry`] link fields
//! (`prev`/`next`, 0 = null) stored in the slab. All operations are O(1).

use crate::core::allocator::SlabAllocator;
use crate::core::timer_entry::TimerEntry;

/// Inserts `slot` at the front of the bucket whose head lives at `*head`.
///
/// After the call, `*head == slot`, the previous head becomes `slot`'s
/// successor, and `slot`'s `prev` link is the null sentinel.
#[inline]
pub fn push_front(slab: &mut SlabAllocator<TimerEntry>, head: &mut u32, slot: u32) {
    let old_head = *head;
    {
        let e = slab.get_mut(slot);
        e.prev = 0;
        e.next = old_head;
    }
    if old_head != 0 {
        slab.get_mut(old_head).prev = slot;
    }
    *head = slot;
}

/// Unlinks `slot` from whatever bucket it is currently in (head at `*head`).
///
/// Updates the neighbour links and, if `slot` was the head, advances `*head`
/// to its successor. Clears `slot`'s own links afterwards.
#[inline]
pub fn unlink(slab: &mut SlabAllocator<TimerEntry>, head: &mut u32, slot: u32) {
    let (prev, next) = {
        let e = slab.get(slot);
        (e.prev, e.next)
    };
    if prev != 0 {
        slab.get_mut(prev).next = next;
    } else {
        // slot was the head — promote its successor.
        *head = next;
    }
    if next != 0 {
        slab.get_mut(next).prev = prev;
    }
    let e = slab.get_mut(slot);
    e.prev = 0;
    e.next = 0;
}

/// Drains every entry from the bucket at `*head`, returning the slot indices
/// in front-to-back order. Resets `*head` to the null sentinel.
///
/// Does not touch each entry's own `next` link (the slots are about to be
/// fired or freed), which is safe because a freed slot is re-written before
/// its next use.
pub fn drain_all(slab: &mut SlabAllocator<TimerEntry>, head: &mut u32) -> Vec<u32> {
    let mut out = Vec::new();
    let mut cur = *head;
    *head = 0;
    while cur != 0 {
        // Walk via `next`. `cur`'s prev must be 0 (head of the remaining chain).
        debug_assert_eq!(slab.get(cur).prev, 0, "drain walked a non-head node");
        out.push(cur);
        let next = slab.get(cur).next;
        // clear this node's prev so the next iteration's debug_assert holds
        if next != 0 {
            slab.get_mut(next).prev = 0;
        }
        cur = next;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::timer_entry::TimerEntry;

    fn fresh_slab(n: usize) -> SlabAllocator<TimerEntry> {
        SlabAllocator::new(n).expect("slab")
    }

    fn make_entry(slab: &mut SlabAllocator<TimerEntry>, task_id: u64) -> u32 {
        let slot = slab.allocate().expect("slot");
        let slot_u32 = slot.get();
        slab.write(slot, TimerEntry::new(task_id, task_id));
        slot_u32
    }

    #[test]
    fn push_front_single_entry_makes_it_head() {
        let mut slab = fresh_slab(4);
        let mut head = 0u32;
        let s = make_entry(&mut slab, 10);
        push_front(&mut slab, &mut head, s);
        assert_eq!(head, s);
        let e = slab.get(s);
        assert_eq!(e.prev, 0);
        assert_eq!(e.next, 0);
    }

    #[test]
    fn push_front_chains_in_lifo_order() {
        let mut slab = fresh_slab(4);
        let mut head = 0u32;
        let a = make_entry(&mut slab, 1);
        let b = make_entry(&mut slab, 2);
        let c = make_entry(&mut slab, 3);
        push_front(&mut slab, &mut head, a);
        push_front(&mut slab, &mut head, b);
        push_front(&mut slab, &mut head, c);
        // head -> c -> b -> a
        assert_eq!(head, c);
        assert_eq!(slab.get(c).next, b);
        assert_eq!(slab.get(b).prev, c);
        assert_eq!(slab.get(b).next, a);
        assert_eq!(slab.get(a).prev, b);
        assert_eq!(slab.get(a).next, 0);
    }

    #[test]
    fn unlink_middle_repairs_links() {
        let mut slab = fresh_slab(4);
        let mut head = 0u32;
        let a = make_entry(&mut slab, 1);
        let b = make_entry(&mut slab, 2);
        let c = make_entry(&mut slab, 3);
        push_front(&mut slab, &mut head, a);
        push_front(&mut slab, &mut head, b);
        push_front(&mut slab, &mut head, c);
        // remove b
        unlink(&mut slab, &mut head, b);
        assert_eq!(head, c);
        assert_eq!(slab.get(c).next, a);
        assert_eq!(slab.get(a).prev, c);
        // b's links are cleared
        let eb = slab.get(b);
        assert_eq!(eb.prev, 0);
        assert_eq!(eb.next, 0);
    }

    #[test]
    fn unlink_head_promotes_successor() {
        let mut slab = fresh_slab(4);
        let mut head = 0u32;
        let a = make_entry(&mut slab, 1);
        let b = make_entry(&mut slab, 2);
        push_front(&mut slab, &mut head, a);
        push_front(&mut slab, &mut head, b);
        // head -> b -> a ; unlink b
        unlink(&mut slab, &mut head, b);
        assert_eq!(head, a);
        assert_eq!(slab.get(a).prev, 0);
    }

    #[test]
    fn unlink_only_entry_empties_bucket() {
        let mut slab = fresh_slab(4);
        let mut head = 0u32;
        let a = make_entry(&mut slab, 1);
        push_front(&mut slab, &mut head, a);
        unlink(&mut slab, &mut head, a);
        assert_eq!(head, 0);
    }

    #[test]
    fn drain_all_returns_front_to_back_and_clears_head() {
        let mut slab = fresh_slab(4);
        let mut head = 0u32;
        let a = make_entry(&mut slab, 1);
        let b = make_entry(&mut slab, 2);
        let c = make_entry(&mut slab, 3);
        push_front(&mut slab, &mut head, a);
        push_front(&mut slab, &mut head, b);
        push_front(&mut slab, &mut head, c);
        let drained = drain_all(&mut slab, &mut head);
        // head -> c -> b -> a
        assert_eq!(drained, vec![c, b, a]);
        assert_eq!(head, 0);
    }

    #[test]
    fn drain_all_on_empty_bucket_is_noop() {
        let mut slab = fresh_slab(4);
        let mut head = 0u32;
        assert!(drain_all(&mut slab, &mut head).is_empty());
        assert_eq!(head, 0);
    }
}
