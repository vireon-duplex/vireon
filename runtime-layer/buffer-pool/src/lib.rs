//! Production-grade buffer pool for `Vireon`.
//!
//! A layered memory subsystem modelled on jemalloc/mimalloc/tcmalloc: each
//! responsibility lives in its own module so it can be tested and tuned
//! independently. The public surface is the [`BufferPool`] handle; internally,
//! an acquire request flows through:
//!
//! ```text
//! BufferPool::get(size)
//!   -> classifier           (size -> ClassId / Huge)
//!   -> thread-local cache   (fast path, per-class bucket)
//!   -> global shard cache   (batch refill, sharded by thread id)
//!   -> arena / slab         (fresh allocation, power-of-2 size classes)
//!   -> huge allocator       (sizes above the class table)
//!
//!   backpressure gate       (rejects or parks when bytes_outstanding > cap)
//!   maintenance worker      (background shrink / tune / telemetry)
//! ```
//!
//! Release is the mirror image, driven by an RAII [`Buffer`] guard so a slot
//! can never be forgotten.
//!
//! # Quick start
//!
//! ```no_run
//! use buffer_pool::BufferPool;
//!
//! let pool = BufferPool::default();
//!
//! // Acquire a buffer — DerefMut to &mut [u8].
//! let mut buf = pool.get(1024)?;
//! buf[..5].copy_from_slice(b"hello");
//!
//! // into_bytes copies once into a refcounted Bytes.
//! let bytes = buf.into_bytes();
//! assert_eq!(&bytes[..5], b"hello");
//! # Ok::<(), buffer_pool::BufferPoolError>(())
//! ```
//!
//! # Layer map
//!
//! | Layer | Module | Responsibility |
//! |-------|--------|---------------|
//! | 1 | [`buffer`] | `BufferPool` handle, `PoolInner` |
//! | 2 | [`buffer`] | `BufferPoolBuilder` |
//! | 3 | [`allocator`] | Size-class table, classifier |
//! | 4 | [`allocator`] | Arena owning slabs |
//! | 5 | [`allocator`] | Slab (page-backed, bitmap) |
//! | 6 | [`allocator`] | Huge allocator (`alloc`/`dealloc`) |
//! | 7 | [`cache`] | Thread-local per-class buckets |
//! | 8 | [`cache`] | Global sharded cache |
//! | 9 | [`cache`] | Adaptive batch sizing |
//! | 10-11 | [`memory`] | Zeroing & recycle preparation |
//! | 12 | [`guard`] | `Buffer` RAII guard |
//! | 13-14 | [`stats`] | Atomic counters, latency histogram |
//! | 15 | [`telemetry`] | Metrics exporter (Prometheus / OTel) |
//! | 16-19 | [`maintenance`] | Pressure monitor, shrinker, tuner, worker |
//! | 20 | [`debug`] | Leak tracker with backtrace capture |
//! | 21 | [`debug`] | Validator, per-op tracing |
//! | 22 | [`numa`] | NUMA node detection & memory binding |
//! | 23 | [`backpressure`] | Byte-ceiling gate, parking waiter |
//! | 24 | [`buffer`] | Config & policy types |
//! | 25 | [`error`] | `BufferPoolError` enum |
//!
//! # Feature flags
//!
//! All features are **off by default** — a release build depends only on
//! `bytes`, `thiserror`, and `tracing`.
//!
//! - `prometheus` — registers counters/gauges on a Prometheus registry.
//! - `otel` — emits metrics via the OpenTelemetry SDK.
//! - `numa` — queries `getcpu` / `mbind` for NUMA-aware allocation (Linux).
//! - `leak` — captures allocation-site backtraces for the leak detector.
//!
//! # Design choices
//!
//! - **Raw-memory allocator.** Buffers are aligned slots owned by the pool;
//!   `Buffer` derefs to `&[u8]` / `&mut [u8]`. [`Buffer::into_bytes`] copies
//!   once when reference-counted ownership is required.
//! - **Sync fast path, no async runtime.** The pool sits below `tokio`;
//!   background maintenance runs on a dedicated `std::thread`.
//! - **std locks.** `Mutex` / `Condvar` from `std::sync` — the fast path is
//!   lock-free (thread-local); locks appear only on the refill/drain batch
//!   path.
//!
//! [`BufferPool`]: crate::buffer::BufferPool
//! [`Buffer`]: crate::guard::Buffer
//! [`Buffer::into_bytes`]: crate::guard::Buffer::into_bytes
//! [`buffer`]: crate::buffer
//! [`allocator`]: crate::allocator
//! [`cache`]: crate::cache
//! [`memory`]: crate::memory
//! [`guard`]: crate::guard
//! [`stats`]: crate::stats
//! [`telemetry`]: crate::telemetry
//! [`maintenance`]: crate::maintenance
//! [`debug`]: crate::debug
//! [`numa`]: crate::numa
//! [`backpressure`]: crate::backpressure
//! [`error`]: crate::error

#![allow(clippy::module_inception)]
#![cfg_attr(not(test), warn(clippy::unwrap_used, clippy::expect_used))]

pub mod allocator;
pub mod backpressure;
pub mod buffer;
pub mod cache;
pub mod debug;
pub mod error;
pub mod guard;
pub mod maintenance;
pub mod memory;
pub mod numa;
pub mod stats;
pub mod telemetry;

pub use backpressure::BackpressurePolicy;
pub use buffer::policy::{PressureLevel, ShrinkPolicy, ZeroingPolicy};
pub use buffer::{BufferPool, BufferPoolBuilder, BufferPoolConfig};
pub use error::BufferPoolError;
pub use guard::Buffer;
pub use numa::NodeId;
pub use stats::StatsSnapshot;
pub use telemetry::{MetricsExporter, NoopExporter};
