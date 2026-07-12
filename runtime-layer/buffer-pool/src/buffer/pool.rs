//! Pool public API (Layer 1).
//!
//! [`BufferPool`] is the user-facing handle. It wraps an `Arc` of internal
//! state and provides the `get` / `try_get` / `flush` / `shrink` / `close`
//! surface. Internally, `PoolInner` wires together the allocator, caches,
//! stats, and implements [`ReleaseSink`] so a dropped [`Buffer`] is recycled
//! automatically.

use std::alloc::Layout;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use crate::allocator::allocator::{Allocation, Allocator};
use crate::allocator::classifier::{Classification, classify_request};
use crate::allocator::size_class::{ClassId, NUM_CLASSES, object_size};
use crate::backpressure::{BackpressurePolicy, Waiter};
use crate::buffer::config::BufferPoolConfig;
use crate::cache::SlotPtr;
use crate::cache::global::GlobalCache;
use crate::cache::thread_local::{flush_current_thread, try_with_local_cache};
use crate::debug::leak::LeakTracker;
use crate::error::BufferPoolError;
use crate::guard::{Buffer, ReleaseSink};
use crate::maintenance::MaintenanceWorker;
use crate::memory::recycle::prepare;
use crate::stats::{Counters, LatencyHistogram, StatsSnapshot};

/// Maximum time spent parked per iteration before re-checking capacity.
///
/// Kept short so that a lost wakeup (notify issued between the capacity check
/// and `park`) costs at most this much latency rather than the full
/// `wait_timeout`.
const PARK_TICK: Duration = Duration::from_millis(50);

/// Monotonic counter assigning each pool a unique id for thread-local cache
/// claiming.
static POOL_ID_SEQ: AtomicUsize = AtomicUsize::new(0);

/// Internal state shared by all handles to the same pool.
pub(crate) struct PoolInner {
    pool_id: usize,
    config: BufferPoolConfig,
    allocator: Allocator,
    global: GlobalCache,
    counters: Counters,
    histogram: LatencyHistogram,
    closed: AtomicBool,
    waiter: Waiter,
    bp_policy: BackpressurePolicy,
    leak: LeakTracker,
}

impl PoolInner {
    /// Acquires memory for `size` bytes.
    ///
    /// Fast path: thread-local → global → arena. Huge sizes bypass caches.
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

                // 1. Thread-local fast path.
                if let Some(slot) = self.pop_local(class_idx) {
                    self.counters.record_hit();
                    return Ok(Allocation {
                        ptr: slot,
                        capacity: class.size,
                        class: id,
                        huge_layout: None,
                    });
                }
                self.counters.record_miss();

                // 2. Global cache refill.
                if let Some(slot) = self.refill_from_global(class_idx) {
                    self.counters.record_reuse();
                    return Ok(Allocation {
                        ptr: slot,
                        capacity: class.size,
                        class: id,
                        huge_layout: None,
                    });
                }

                // 3. Fresh allocation from the arena.
                let alloc = self.allocator.allocate(size)?;
                self.counters.record_alloc(alloc.capacity);
                Ok(alloc)
            }
        }
    }

    /// Pops a slot from the calling thread's local cache.
    fn pop_local(&self, class_idx: usize) -> Option<NonNull<u8>> {
        try_with_local_cache(self.pool_id, NUM_CLASSES, self.config.local_cache, |c| {
            c.pop(class_idx)
        })
        .flatten()
        .map(SlotPtr::as_nonnull)
    }

    /// Pulls a batch from the global cache, caches extras in the thread-local,
    /// and returns one slot for the caller.
    fn refill_from_global(&self, class_idx: usize) -> Option<NonNull<u8>> {
        let batch = self.config.batch;
        let mut slots: Vec<SlotPtr> = Vec::with_capacity(batch);
        let moved = self.global.pop_batch(class_idx, batch, &mut slots);
        if moved == 0 {
            return None;
        }
        let head = slots.pop()?;

        // Stash the remainder in the thread-local cache.
        let overflow =
            try_with_local_cache(self.pool_id, NUM_CLASSES, self.config.local_cache, |c| {
                let mut overflow = Vec::new();
                for s in slots.drain(..) {
                    if !c.push(class_idx, s) {
                        overflow.push(s);
                    }
                }
                overflow
            });
        // If the thread is claimed by another pool, all extras are overflow.
        let mut overflow = overflow.unwrap_or_else(|| std::mem::take(&mut slots));

        // Return overflow to the global shard.
        self.global.push_batch(class_idx, &mut overflow);
        // Anything the shard could not accept goes back to the arena.
        let class = ClassId(class_idx as u8);
        for leftover in overflow {
            let _ = self.allocator.return_slot(leftover.as_nonnull(), class);
        }

        Some(head.as_nonnull())
    }
}

impl ReleaseSink for PoolInner {
    fn release_buffer(
        &self,
        ptr: NonNull<u8>,
        len: usize,
        class: ClassId,
        huge_layout: Option<Layout>,
    ) {
        self.leak.record_free(ptr.as_ptr() as usize);

        // Huge allocations bypass the cache and are deallocated directly.
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
            self.waiter.notify_one();
            return;
        }

        let class_idx = usize::from(class.0);
        let cap = object_size(class);

        // Zero the used portion before recycling.
        // SAFETY: `ptr` is valid for `len` bytes (the caller-visible region).
        unsafe {
            prepare(ptr.as_ptr(), len, self.config.zeroing);
        }

        // Try the thread-local cache first; drain to global on overflow.
        let accepted = try_with_local_cache(
            self.pool_id,
            NUM_CLASSES,
            self.config.local_cache,
            |c| -> bool {
                if c.push(class_idx, SlotPtr::new(ptr)) {
                    return true;
                }
                // Bucket full → drain to global, then push.
                let mut drained = c.drain_bucket(class_idx);
                self.global.push_batch(class_idx, &mut drained);
                let class_id = ClassId(class_idx as u8);
                for leftover in drained {
                    let _ = self.allocator.return_slot(leftover.as_nonnull(), class_id);
                }
                // The bucket is now empty; the push will succeed.
                let _ = c.push(class_idx, SlotPtr::new(ptr));
                true
            },
        );

        if accepted.is_none() {
            // Thread claimed by another pool → push directly to the global shard.
            if !self.global.push(class_idx, SlotPtr::new(ptr)) {
                let _ = self.allocator.return_slot(ptr, class);
            }
        }

        self.counters.record_free(cap);
        self.waiter.notify_one();
    }
}

/// A handle to a layered buffer pool.
///
/// Construct via [`BufferPool::new`] (zero-config) or
/// [`BufferPool::builder`](BufferPool::builder) for tuned construction. The
/// handle is [`Clone`] — each clone shares the same underlying state.
pub struct BufferPool {
    inner: Arc<PoolInner>,
}

impl BufferPool {
    /// Creates a pool with the given configuration.
    pub fn new(config: BufferPoolConfig) -> Self {
        let pool_id = POOL_ID_SEQ.fetch_add(1, Ordering::Relaxed);
        let global = GlobalCache::new(NUM_CLASSES, config.shards, config.global_cache);
        let bp_policy = BackpressurePolicy::from_max_memory(config.max_memory);
        let inner = Arc::new(PoolInner {
            pool_id,
            config,
            allocator: Allocator::new(),
            global,
            counters: Counters::new(),
            histogram: LatencyHistogram::new(),
            closed: AtomicBool::new(false),
            waiter: Waiter::new(),
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
    /// Blocks until capacity is available or the backpressure wait timeout
    /// elapses. Returns [`Err(Timeout)`](BufferPoolError::Timeout) on timeout
    /// and [`Err(PoolClosed)`](BufferPoolError::PoolClosed) if the pool was
    /// closed while waiting.
    pub fn get(&self, size: usize) -> Result<Buffer, BufferPoolError> {
        let deadline = Instant::now() + self.inner.bp_policy.wait_timeout;
        loop {
            let outstanding = self.inner.counters.bytes_outstanding() as usize;
            if !self.inner.bp_policy.would_exceed(outstanding, size) {
                break;
            }
            let now = Instant::now();
            if now >= deadline {
                return Err(BufferPoolError::Timeout {
                    size,
                    timeout_ms: self.inner.bp_policy.wait_timeout.as_millis() as u64,
                });
            }
            let remaining = deadline.saturating_duration_since(now);
            let park_for = remaining.min(PARK_TICK);
            let _ = self.inner.waiter.park(park_for);
        }
        self.acquire_buffer(size)
    }

    /// Non-blocking acquire.
    ///
    /// Returns [`Err(Rejected)`](BufferPoolError::Rejected) when
    /// `bytes_outstanding + size` would exceed the pool's byte ceiling,
    /// [`Err(PoolClosed)`](BufferPoolError::PoolClosed) when the pool is
    /// closed.
    pub fn try_get(&self, size: usize) -> Result<Buffer, BufferPoolError> {
        let outstanding = self.inner.counters.bytes_outstanding() as usize;
        if self.inner.bp_policy.would_exceed(outstanding, size) {
            return Err(BufferPoolError::Rejected { size });
        }
        self.acquire_buffer(size)
    }

    /// Shared allocation path used by both [`get`](Self::get) and
    /// [`try_get`](Self::try_get).
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
            let dyn_arc: Arc<dyn ReleaseSink> = self.inner.clone();
            Arc::downgrade(&dyn_arc)
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

    /// Flushes the calling thread's local cache to the global shard.
    ///
    /// Call this before a thread that used the pool exits, or when you want to
    /// return cached memory to the global pool.
    pub fn flush(&self) {
        let drained = flush_current_thread(self.inner.pool_id);
        for (class_idx, slots) in drained.into_iter().enumerate() {
            if slots.is_empty() {
                continue;
            }
            let mut batch = slots;
            self.inner.global.push_batch(class_idx, &mut batch);
            let class = ClassId(class_idx as u8);
            for leftover in batch {
                let _ = self
                    .inner
                    .allocator
                    .return_slot(leftover.as_nonnull(), class);
            }
        }
    }

    /// Trims the global cache and purges fully-free slabs.
    pub fn shrink(&self) {
        let half = self.inner.global.cap_per_shard() / 2;
        let drained = self.inner.global.trim(half);
        for (class_idx, slots) in drained.into_iter().enumerate() {
            let class = ClassId(class_idx as u8);
            for slot in slots {
                let _ = self.inner.allocator.return_slot(slot.as_nonnull(), class);
            }
        }
        let _ = self.inner.allocator.purge();
    }

    /// Returns a point-in-time snapshot of pool statistics.
    pub fn stats(&self) -> StatsSnapshot {
        StatsSnapshot {
            counters: self.inner.counters.snapshot(),
            latency: self.inner.histogram.snapshot(),
            arena_bytes: self.inner.allocator.arena_memory() as u64,
            cached_bytes: self
                .inner
                .global
                .memory_usage(|idx| object_size(ClassId(idx as u8)))
                as u64,
        }
    }

    /// Returns the active configuration.
    pub fn config(&self) -> &BufferPoolConfig {
        &self.inner.config
    }

    /// Closes the pool to new allocations and drains all caches.
    ///
    /// After `close()`, [`get`](BufferPool::get) and
    /// [`try_get`](BufferPool::try_get) return [`PoolClosed`](BufferPoolError::PoolClosed).
    ///
    /// **Not thread-safe with concurrent `get`/`release`** — call when no other
    /// thread is using the pool.
    pub fn close(&self) {
        self.inner.closed.store(true, Ordering::Release);
        let drained = self.inner.global.drain_all();
        for (class_idx, slots) in drained.into_iter().enumerate() {
            if slots.is_empty() {
                continue;
            }
            let class = ClassId(class_idx as u8);
            for slot in slots {
                let _ = self.inner.allocator.return_slot(slot.as_nonnull(), class);
            }
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

    /// Spawns a background maintenance thread that monitors pressure and
    /// shrinks caches.
    ///
    /// The worker runs until the returned [`MaintenanceWorker`] is dropped.
    pub fn start_maintenance(&self) -> std::io::Result<MaintenanceWorker> {
        MaintenanceWorker::start(self.clone())
    }
}

impl Clone for BufferPool {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
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
    fn drop_then_get_is_served_from_thread_local() {
        let pool = BufferPool::default();
        let _ = pool.get(256).unwrap();
        // After drop, the slot is in the thread-local cache.
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

        // Next acquire from cache should see zeroed memory.
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
        pool.flush();
        let before = pool.stats().cached_bytes;
        pool.shrink();
        let after = pool.stats().cached_bytes;
        assert!(
            after <= before,
            "shrink should not increase cached bytes ({before} -> {after})"
        );
    }

    #[test]
    fn concurrency_stress_no_panic() {
        let pool = BufferPool::default();
        let pool_ref = &pool;
        std::thread::scope(|s| {
            for _ in 0..4 {
                s.spawn(move || {
                    for _ in 0..500 {
                        let buf = pool_ref.try_get(256).unwrap();
                        drop(buf);
                    }
                    pool_ref.flush();
                });
            }
        });
        let stats = pool.stats();
        assert_eq!(
            stats.counters.outstanding, 0,
            "outstanding should be 0 after all buffers dropped"
        );
        let total = stats.counters.hits + stats.counters.misses + stats.counters.reuses;
        assert!(total >= 4 * 500, "expected at least {total} ops");
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
    fn get_wakes_when_capacity_freed() {
        let config = BufferPoolConfig {
            max_memory: 256,
            ..Default::default()
        };
        let pool = BufferPool::new(config);

        // Fill past the soft threshold (reject_at = 230 for 256 capacity).
        let held = pool.get(200).unwrap();

        let pool2 = pool.clone();
        let handle = std::thread::spawn(move || {
            // This parks because 200 outstanding + 64 requested = 264
            // > 230 (reject_at).
            pool2.get(64)
        });

        // Give the spawned thread time to enter the park loop.
        std::thread::sleep(Duration::from_millis(100));

        // Release the held buffer → frees capacity → parked thread wakes.
        drop(held);

        let result = handle.join().expect("thread join");
        assert!(
            result.is_ok(),
            "get should succeed after release: {result:?}"
        );
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
