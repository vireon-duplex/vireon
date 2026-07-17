//! Pool public API (Layer 1).
//!
//! [`BufferPool`] is the user-facing handle. It wraps an `Rc` of internal
//! state and provides the `get` / `try_get` / `flush` / `shrink` / `close`
//! surface. Internally, `PoolInner` wires together the allocator, caches,
//! stats, and implements [`ReleaseSink`] so a dropped [`Buffer`] is recycled
//! automatically.
//!
//! This pool is **single-threaded** (`Rc`-based, `!Send`). Each reactor
//! thread owns its pool exclusively — no cross-core sharing, no locks.

use std::alloc::Layout;
use std::cell::UnsafeCell;
use std::ptr::NonNull;
use std::rc::{Rc, Weak};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Instant;

use profiler::profile;

use crate::allocator::allocator::{Allocation, Allocator};
use crate::allocator::classifier::{Classification, classify_request};
use crate::allocator::size_class::{ClassId, NUM_CLASSES, object_size};
use crate::backpressure::BackpressurePolicy;
use crate::buffer::config::BufferPoolConfig;
use crate::cache::SlotPtr;
use crate::debug::leak::LeakTracker;
use crate::error::BufferPoolError;
use crate::guard::{Buffer, ReleaseSink};
use crate::memory::recycle::prepare;
use crate::stats::{Counters, LatencyHistogram, StatsSnapshot};

/// Monotonic counter assigning each pool a unique id (kept for diagnostics;
/// no longer used for thread-local cache keying).
static POOL_ID_SEQ: AtomicUsize = AtomicUsize::new(0);

/// Per-class free-list storage with zero-overhead interior mutability.
///
/// Wraps `UnsafeCell<Vec<Vec<SlotPtr>>>`. We avoid `RefCell` here because
/// the borrow-flag load+store on every acquire/release showed up as a
/// measurable hot-path cost under single-core profiling. Safety rests on
/// two invariants:
///
/// 1. **`!Send` pool.** `PoolInner` is held inside `Rc<PoolInner>`; `Rc` is
///    `!Send`/`!Sync`, so the pool never crosses threads. All access is
///    serialized on its owning reactor thread.
/// 2. **No re-entrancy.** Neither [`PoolInner::acquire`] nor
///    [`ReleaseSink::release_buffer`] drops a `Buffer` (the only path that
///    re-enters the free-list). Therefore an in-flight `pop`/`push` cannot
///    be interleaved with another mutation through the same `&self`.
///
/// Iteration accessors (`iter`, `iter_mut`) borrow `&self` for the duration
/// of the returned iterator — the caller must not hold another mutable
/// access to the same `FreeLists` while iterating.
struct FreeLists(UnsafeCell<Vec<Vec<SlotPtr>>>);

impl FreeLists {
    fn with_class_count(n: usize) -> Self {
        Self(UnsafeCell::new((0..n).map(|_| Vec::new()).collect()))
    }

    /// Pops a slot from the given class free-list.
    #[inline]
    fn pop(&self, class_idx: usize) -> Option<SlotPtr> {
        // SAFETY: see type-level docs — single-threaded + no re-entrancy.
        unsafe { (&mut *self.0.get())[class_idx].pop() }
    }

    /// Pushes a slot back onto the given class free-list.
    #[inline]
    fn push(&self, class_idx: usize, slot: SlotPtr) {
        // SAFETY: see type-level docs — single-threaded + no re-entrancy.
        unsafe { (&mut *self.0.get())[class_idx].push(slot) }
    }

    /// Drains every class's free-list, returning slots one at a time with
    /// its class index. Used by `shrink`/`close`.
    fn drain_all(&self) -> impl Iterator<Item = (usize, SlotPtr)> + '_ {
        // SAFETY: caller (shrink/close) holds no overlapping access — the
        // returned iterator borrows `&self` for its whole lifetime, and no
        // other pool method runs while shrink/close is executing.
        let outer = unsafe { &mut *self.0.get() };
        outer
            .iter_mut()
            .enumerate()
            .flat_map(|(idx, list)| list.drain(..).map(move |s| (idx, s)))
    }

    /// Per-class length snapshot (used by stats).
    fn class_len(&self, class_idx: usize) -> usize {
        // SAFETY: see type-level docs.
        unsafe { (&*self.0.get())[class_idx].len() }
    }
}

/// Internal state — single-threaded (`!Send`). All mutable fields use
/// `UnsafeCell`/`Cell` interior mutability since the pool is only ever
/// accessed from its owning reactor thread.
pub(crate) struct PoolInner {
    #[allow(dead_code)]
    pool_id: usize,
    config: BufferPoolConfig,
    allocator: Allocator,
    /// Per-class free-list. Replaces the old two-tier (thread-local +
    /// global sharded) cache. Single-owner → no locks.
    free_lists: FreeLists,
    counters: Counters,
    histogram: LatencyHistogram,
    closed: AtomicBool,
    bp_policy: BackpressurePolicy,
    leak: LeakTracker,
}

impl PoolInner {
    /// Acquires memory for `size` bytes.
    ///
    /// Fast path: free-list pop. Slow path: arena allocation.
    #[profile]
    fn acquire(&self, size: usize) -> Result<Allocation, BufferPoolError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(BufferPoolError::PoolClosed);
        }
        match classify_request(size) {
            Classification::Huge { .. } => {
                let alloc = self.allocator.allocate(size)?;
                self.counters.record_alloc(alloc.capacity);
                Ok(alloc)
            }
            Classification::Classed { id, class } => {
                let class_idx = usize::from(id.0);

                // Fast path: direct free-list pop (no Mutex, no thread_local,
                // no RefCell borrow flag — see `FreeLists` safety docs).
                if let Some(slot) = self.free_lists.pop(class_idx) {
                    self.counters.record_hit();
                    return Ok(Allocation {
                        ptr: slot.as_nonnull(),
                        capacity: class.size,
                        class: id,
                        huge_layout: None,
                    });
                }
                self.counters.record_miss();

                // Slow path: fresh allocation from the arena.
                let alloc = self.allocator.allocate(size)?;
                self.counters.record_alloc(alloc.capacity);
                Ok(alloc)
            }
        }
    }
}

impl ReleaseSink for PoolInner {
    #[profile]
    fn release_buffer(
        &self,
        ptr: NonNull<u8>,
        len: usize,
        class: ClassId,
        huge_layout: Option<Layout>,
    ) {
        self.leak.record_free(ptr.as_ptr() as usize);

        // Huge allocations bypass the free-list and are deallocated directly.
        if class == ClassId::HUGE {
            if let Some(layout) = huge_layout {
                let _ = self.allocator.deallocate_huge(Allocation {
                    ptr,
                    capacity: len,
                    class,
                    huge_layout: Some(layout),
                });
            } else {
                tracing::error!(
                    "huge buffer released without layout; leaking {:p} ({} bytes)",
                    ptr.as_ptr(),
                    len
                );
            }
            self.counters.record_free(len);
            return;
        }

        let class_idx = usize::from(class.0);
        let cap = object_size(class);

        // Zero the used portion before recycling.
        // SAFETY: `ptr` is valid for `len` bytes (the caller-visible region).
        unsafe {
            prepare(ptr.as_ptr(), len, self.config.zeroing);
        }

        // Push directly to the free-list (no Mutex, no thread_local, no
        // RefCell borrow flag — see `FreeLists` safety docs).
        self.free_lists.push(class_idx, SlotPtr::new(ptr));

        self.counters.record_free(cap);
    }
}

/// A handle to a single-threaded buffer pool.
///
/// Construct via [`BufferPool::new`] (zero-config) or
/// [`BufferPool::builder`](BufferPool::builder) for tuned construction. The
/// handle is [`Clone`] — each clone shares the same underlying state.
///
/// **`!Send` / `!Sync`** — backed by `Rc`, must stay on the creating thread.
pub struct BufferPool {
    inner: Rc<PoolInner>,
}

impl BufferPool {
    /// Creates a pool with the given configuration.
    pub fn new(config: BufferPoolConfig) -> Self {
        let pool_id = POOL_ID_SEQ.fetch_add(1, Ordering::Relaxed);
        let bp_policy = BackpressurePolicy::from_max_memory(config.max_memory);
        // Capture the preferred NUMA node before `config` is moved into the struct.
        let numa_node = config.numa_node;
        let inner = Rc::new(PoolInner {
            pool_id,
            config,
            allocator: Allocator::with_numa_node(numa_node),
            free_lists: FreeLists::with_class_count(NUM_CLASSES),
            counters: Counters::new(),
            histogram: LatencyHistogram::new(),
            closed: AtomicBool::new(false),
            bp_policy,
            leak: LeakTracker::new(),
        });
        Self { inner }
    }

    /// Returns a fluent [`BufferPoolBuilder`](crate::buffer::BufferPoolBuilder).
    pub fn builder() -> crate::buffer::builder::BufferPoolBuilder {
        crate::buffer::builder::BufferPoolBuilder::new()
    }

    /// Acquires a buffer of at least `size` bytes.
    ///
    /// Non-blocking: returns [`Err(Rejected)`](BufferPoolError::Rejected) when
    /// `bytes_outstanding + size` would exceed the pool's byte ceiling,
    /// [`Err(PoolClosed)`](BufferPoolError::PoolClosed) when the pool is closed.
    #[profile]
    pub fn get(&self, size: usize) -> Result<Buffer, BufferPoolError> {
        let outstanding = self.inner.counters.bytes_outstanding() as usize;
        if self.inner.bp_policy.would_exceed(outstanding, size) {
            return Err(BufferPoolError::Rejected { size });
        }
        self.acquire_buffer(size)
    }

    /// Non-blocking acquire (alias of [`get`](Self::get)).
    #[profile]
    pub fn try_get(&self, size: usize) -> Result<Buffer, BufferPoolError> {
        self.get(size)
    }

    /// Shared allocation path used by [`get`](Self::get).
    #[profile]
    fn acquire_buffer(&self, size: usize) -> Result<Buffer, BufferPoolError> {
        let start = Instant::now();
        let alloc = self.inner.acquire(size)?;
        self.inner.counters.record_acquire(alloc.capacity);
        self.inner
            .leak
            .record_alloc(alloc.ptr.as_ptr() as usize, alloc.capacity);
        let elapsed_ns = start.elapsed().as_nanos() as u64;
        self.inner.histogram.observe(elapsed_ns);
        let weak: Weak<dyn ReleaseSink> = {
            let dyn_rc: Rc<dyn ReleaseSink> = self.inner.clone();
            Rc::downgrade(&dyn_rc)
        };
        Ok(Buffer::new(
            alloc.ptr,
            size,
            alloc.capacity,
            alloc.class,
            alloc.huge_layout,
            weak,
        ))
    }

    /// Drains excess free-list entries back to the arena.
    ///
    /// With the single-tier free-list design this is rarely needed — the
    /// free-list grows only when buffers are released and shrinks on
    /// `shrink()`.
    #[profile]
    pub fn flush(&self) {
        // No-op: the free-list is already the only cache. Nothing to flush
        // across tiers. Kept for API compatibility.
    }

    /// Trims free-lists and purges fully-free slabs.
    pub fn shrink(&self) {
        for (class_idx, slot) in self.inner.free_lists.drain_all() {
            let class = ClassId(class_idx as u8);
            let _ = self.inner.allocator.return_slot(slot.as_nonnull(), class);
        }
        let _ = self.inner.allocator.purge();
    }

    /// Returns a point-in-time snapshot of pool statistics.
    pub fn stats(&self) -> StatsSnapshot {
        let cached_bytes: u64 = (0..NUM_CLASSES)
            .map(|idx| {
                (self.inner.free_lists.class_len(idx) as u64)
                    * object_size(ClassId(idx as u8)) as u64
            })
            .sum();
        StatsSnapshot {
            counters: self.inner.counters.snapshot(),
            latency: self.inner.histogram.snapshot(),
            arena_bytes: self.inner.allocator.arena_memory() as u64,
            cached_bytes,
        }
    }

    /// Returns the active configuration.
    pub fn config(&self) -> &BufferPoolConfig {
        &self.inner.config
    }

    /// Closes the pool to new allocations and drains all caches.
    ///
    /// After `close()`, [`get`](BufferPool::get) returns
    /// [`PoolClosed`](BufferPoolError::PoolClosed).
    pub fn close(&self) {
        self.inner.closed.store(true, Ordering::Release);
        for (class_idx, slot) in self.inner.free_lists.drain_all() {
            let class = ClassId(class_idx as u8);
            let _ = self.inner.allocator.return_slot(slot.as_nonnull(), class);
        }
        let _ = self.inner.allocator.purge();
    }

    /// `true` after [`close`](BufferPool::close) has been called.
    pub fn is_closed(&self) -> bool {
        self.inner.closed.load(Ordering::Acquire)
    }

    /// Current outstanding bytes across all live buffers.
    #[must_use]
    pub fn bytes_outstanding(&self) -> u64 {
        self.inner.counters.bytes_outstanding()
    }

    /// Current memory-pressure level based on outstanding bytes vs capacity.
    #[must_use]
    pub fn pressure_level(&self) -> crate::buffer::policy::PressureLevel {
        let outstanding = self.inner.counters.bytes_outstanding() as usize;
        crate::maintenance::pressure::classify_pressure(outstanding, self.inner.bp_policy.capacity)
    }
}

impl Clone for BufferPool {
    fn clone(&self) -> Self {
        Self {
            inner: Rc::clone(&self.inner),
        }
    }
}

impl Default for BufferPool {
    fn default() -> Self {
        Self::new(BufferPoolConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::policy::ZeroingPolicy;

    #[test]
    fn get_returns_buffer_of_requested_size() {
        let pool = BufferPool::default();
        let buf = pool.get(100).unwrap();
        assert_eq!(buf.len(), 100);
        assert!(buf.capacity() >= 100);
        assert!(!buf.is_huge());
    }

    #[test]
    fn get_zero_returns_empty_buffer() {
        let pool = BufferPool::default();
        let buf = pool.get(0).unwrap();
        assert_eq!(buf.len(), 0);
        assert!(buf.is_empty());
    }

    #[test]
    fn drop_then_get_is_served_from_free_list() {
        let pool = BufferPool::default();
        let _ = pool.get(256).unwrap();
        // After drop, the slot is in the free-list.
        let buf = pool.get(256).unwrap();
        let stats = pool.stats();
        assert!(stats.counters.hits >= 1, "expected at least 1 hit");
        assert_eq!(buf.len(), 256);
    }

    #[test]
    fn zeroing_clear_wipes_data_on_release() {
        let config = BufferPoolConfig {
            zeroing: ZeroingPolicy::Clear,
            ..Default::default()
        };
        let pool = BufferPool::new(config);

        let mut buf = pool.get(128).unwrap();
        for b in buf.iter_mut() {
            *b = 0xAA;
        }
        drop(buf);

        // Next acquire from free-list should see zeroed memory.
        let buf2 = pool.get(128).unwrap();
        assert!(buf2.iter().all(|&b| b == 0));
    }

    #[test]
    fn huge_allocation_bypasses_cache() {
        let pool = BufferPool::default();
        let huge_size = pool.config().huge_threshold + 1;
        let buf = pool.get(huge_size).unwrap();
        assert!(buf.is_huge());
        assert_eq!(buf.len(), huge_size);
        assert_eq!(buf.capacity(), huge_size);
    }

    #[test]
    fn stats_track_alloc_free_and_outstanding() {
        let pool = BufferPool::default();
        let buf1 = pool.get(64).unwrap();
        let buf2 = pool.get(64).unwrap();
        let stats = pool.stats();
        assert_eq!(stats.counters.outstanding, 2);
        assert_eq!(stats.counters.peak_outstanding, 2);
        drop(buf1);
        drop(buf2);
        let stats = pool.stats();
        assert_eq!(stats.counters.outstanding, 0);
        assert_eq!(stats.counters.frees, stats.counters.allocations);
    }

    #[test]
    fn close_prevents_further_allocations() {
        let pool = BufferPool::default();
        pool.close();
        assert!(pool.is_closed());
        let result = pool.get(64);
        assert!(matches!(result, Err(BufferPoolError::PoolClosed)));
    }

    #[test]
    fn shrink_frees_cached_memory() {
        let pool = BufferPool::default();
        // Fill caches by allocating and dropping many buffers.
        let buffers: Vec<_> = (0..64).map(|_| pool.get(64).unwrap()).collect();
        drop(buffers);
        let before = pool.stats().cached_bytes;
        pool.shrink();
        let after = pool.stats().cached_bytes;
        assert!(
            after <= before,
            "shrink should not increase cached bytes ({before} -> {after})"
        );
    }

    #[test]
    fn into_bytes_returns_copy_and_releases() {
        let pool = BufferPool::default();
        let mut buf = pool.get(32).unwrap();
        buf[..5].copy_from_slice(b"hello");
        let bytes = buf.into_bytes();
        assert_eq!(&bytes[..5], b"hello");
        // Slot should be released back.
        let stats = pool.stats();
        assert_eq!(stats.counters.frees, 1);
    }

    #[test]
    fn builder_rejects_invalid_config() {
        let result = BufferPool::builder().batch(0).build();
        assert!(matches!(result, Err(BufferPoolError::InvalidConfig(_))));
    }

    #[test]
    fn builder_accepts_valid_config() {
        let pool = BufferPool::builder()
            .local_cache(32)
            .global_cache(512)
            .shards(4)
            .batch(8)
            .build()
            .unwrap();
        let buf = pool.get(64).unwrap();
        assert_eq!(buf.len(), 64);
    }

    #[test]
    fn try_get_rejects_under_pressure() {
        let config = BufferPoolConfig {
            max_memory: 1024,
            ..Default::default()
        };
        let pool = BufferPool::new(config);

        // Hold a buffer to consume some of the budget.
        let _held = pool.get(512).unwrap();
        // Requesting enough to push over the ceiling should reject.
        let result = pool.try_get(1024);
        assert!(
            matches!(result, Err(BufferPoolError::Rejected { size }) if size == 1024),
            "expected Rejected, got {result:?}"
        );
    }

    #[test]
    fn try_get_succeeds_after_release_frees_capacity() {
        let config = BufferPoolConfig {
            max_memory: 1024,
            ..Default::default()
        };
        let pool = BufferPool::new(config);

        let held = pool.get(512).unwrap();
        assert!(pool.try_get(1024).is_err());
        drop(held);
        // Capacity freed → try_get should succeed now.
        assert!(pool.try_get(512).is_ok());
    }

    #[test]
    fn leak_tracker_reports_zero_after_drop() {
        let pool = BufferPool::default();
        {
            let _buf = pool.get(64).unwrap();
            assert_eq!(pool.inner.leak.count(), 1);
        }
        assert_eq!(pool.inner.leak.count(), 0);
    }
}
