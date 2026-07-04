//! Drain: thread-local -> global transfer (Layer 7).
//!
//! When a thread-local bucket overflows, [`drain_to_shard`] pushes the excess
//! into the matching global shard. Anything that does not fit stays in
//! `bucket` for the caller to return to the arena.

use crate::cache::SlotPtr;
use crate::cache::shard::Shard;

/// Pushes slots from `bucket` into `shard` until the shard is full or the
/// bucket is empty.
///
/// Returns the count remaining in `bucket` (the overflow the caller must
/// return to the arena).
#[inline]
pub fn drain_to_shard(bucket: &mut Vec<SlotPtr>, shard: &Shard) -> usize {
    shard.push_batch(bucket);
    bucket.len()
}

/// Drains the whole bucket into `shard`, returning whatever did not fit.
pub fn drain_all(bucket: &mut Vec<SlotPtr>, shard: &Shard) -> Vec<SlotPtr> {
    shard.push_batch(bucket);
    std::mem::take(bucket)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ptr::NonNull;

    fn ptr(addr: usize) -> SlotPtr {
        SlotPtr::new(NonNull::new(addr as *mut u8).unwrap())
    }

    #[test]
    fn drain_pushes_into_shard_until_full() {
        let shard = Shard::new(2);
        let mut bucket = vec![ptr(1), ptr(2), ptr(3), ptr(4)];
        let remaining = drain_to_shard(&mut bucket, &shard);
        assert_eq!(shard.len(), 2);
        assert_eq!(remaining, 2);
    }

    #[test]
    fn drain_all_returns_leftover() {
        let shard = Shard::new(2);
        let mut bucket = vec![ptr(1), ptr(2), ptr(3)];
        let leftover = drain_all(&mut bucket, &shard);
        assert_eq!(shard.len(), 2);
        assert_eq!(leftover.len(), 1);
        assert!(bucket.is_empty());
    }

    #[test]
    fn drain_into_empty_bucket_is_noop() {
        let shard = Shard::new(4);
        let mut bucket = Vec::new();
        let remaining = drain_to_shard(&mut bucket, &shard);
        assert_eq!(remaining, 0);
        assert_eq!(shard.len(), 0);
    }
}
