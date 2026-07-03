//! Refill: global -> thread-local transfer (Layer 7).
//!
//! When a thread-local bucket runs dry, [`refill_from_shard`] pulls a batch
//! from the matching global shard into the bucket. The pool then pops one slot
//! off the freshly refilled bucket.

use crate::cache::SlotPtr;
use crate::cache::shard::Shard;

/// Pulls up to `batch` slots from `shard`, appending them to `bucket`.
///
/// Returns the number of slots moved. A return of `0` means the global shard
/// was empty; the caller should then allocate a fresh slot from the arena.
#[inline]
pub fn refill_from_shard(bucket: &mut Vec<SlotPtr>, shard: &Shard, batch: usize) -> usize {
    shard.pop_batch(batch, bucket)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ptr::NonNull;

    fn ptr(addr: usize) -> SlotPtr {
        SlotPtr::new(NonNull::new(addr as *mut u8).unwrap())
    }

    #[test]
    fn refill_moves_batch_into_bucket() {
        let shard = Shard::new(16);
        shard.push(ptr(1));
        shard.push(ptr(2));
        shard.push(ptr(3));
        let mut bucket = Vec::new();
        let moved = refill_from_shard(&mut bucket, &shard, 2);
        assert_eq!(moved, 2);
        assert_eq!(bucket.len(), 2);
        assert_eq!(shard.len(), 1);
    }

    #[test]
    fn refill_from_empty_shard_moves_zero() {
        let shard = Shard::new(16);
        let mut bucket = Vec::new();
        let moved = refill_from_shard(&mut bucket, &shard, 4);
        assert_eq!(moved, 0);
        assert!(bucket.is_empty());
    }
}
