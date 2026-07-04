//! Production-grade hierarchical timing wheel for `Vireon`.
//!
//! A lock-free, thread-per-core timer subsystem modelled on Netty (MPSC +
//! hashed wheel), Kafka (hierarchical + on-demand cascading) and Seastar
//! (share-nothing sharding). The public surface is the [`TimerWheel`] handle;
//! internally, a `start` request flows through:
//!
//! ```text
//! TimerWheel::start(task_id, duration)
//!   -> sharder             (task_id -> ShardId, Fibonacci hash)
//!   -> outstanding gate    (synchronous capacity check)
//!   -> BoundedMpsc<TimerCmd> (lock-free Vyukov queue, per shard)
//!   -> worker tick         (drain commands -> slab allocate -> wheel.add)
//!   -> hierarchical wheel  (bitwise slot lookup, cascade on wrap)
//!   -> expired queue       (drain_expired, or inline TimerHandler)
//! ```
//!
//! # Quick start
//!
//! ```no_run
//! use std::time::Duration;
//! use timer_wheel::{TimerWheel, TimerWheelBuilder};
//!
//! let mut wheel = TimerWheelBuilder::new().shards(4).spawn()?;
//!
//! let _handle = wheel.start(42, Duration::from_millis(50))?;
//! // ... later:
//! let fired = wheel.drain_expired();
//! assert!(fired.contains(&42));
//!
//! wheel.shutdown();
//! # Ok::<(), timer_wheel::TimerError>(())
//! ```
//!
//! # Layer map
//!
//! | Layer | Module | Responsibility |
//! |-------|--------|---------------|
//! | 1 | [`error`] | `TimerError`, `ShardId` |
//! | 2 | [`config`] | `TimerWheelConfig`, constants |
//! | 3 | [`core::allocator`] | Slab allocator (O(1) free-list) |
//! | 4 | [`core::timer_entry`] | 24-byte `TimerEntry` |
//! | 5 | [`core::bucket`] / [`core::wheel`] | Intrusive buckets, hierarchical wheel |
//! | 6 | [`concurrent::mpsc`] / [`concurrent::sharder`] | Vyukov MPSC, shard router |
//! | 7 | [`scheduler::clock`] | Monotonic tick source |
//! | 8 | [`scheduler::shard`] / [`scheduler::worker`] | Share-nothing shard, event loop |
//! | 9 | [`api`] | `TimerWheel` facade, `TimerHandle` |
//!
//! # Design notes
//!
//! - **Cancellation** is lazy and tracked in a per-shard `HashSet<TaskId>`
//!   drained at fire time. A cancel issued before the corresponding `start`
//!   is processed by the worker is still honoured.
//! - **Sync fast path.** `start` only hashes, capacity-checks, and pushes to a
//!   lock-free queue; the slab and wheel are touched solely by the owning
//!   worker.
//! - **std locks: none.** The hot path is lock-free (Vyukov queue + Treiber
//!   reservation counter). Worker parking uses `std::thread::sleep`.
//!
//! [`core::allocator`]: crate::core::allocator
//! [`core::timer_entry`]: crate::core::timer_entry
//! [`core::bucket`]: crate::core::bucket
//! [`core::wheel`]: crate::core::wheel
//! [`concurrent::mpsc`]: crate::concurrent::mpsc
//! [`concurrent::sharder`]: crate::concurrent::sharder
//! [`scheduler::clock`]: crate::scheduler::clock
//! [`scheduler::shard`]: crate::scheduler::shard
//! [`scheduler::worker`]: crate::scheduler::worker

#![allow(clippy::module_inception)]

pub mod api;
pub mod concurrent;
pub mod config;
pub mod core;
pub mod error;
pub mod scheduler;

pub use api::{TimerHandle, TimerWheel, TimerWheelBuilder};
pub use config::TimerWheelConfig;
pub use error::{ShardId, TimerError};
pub use scheduler::clock::Clock;
pub use scheduler::shard::{TaskId, TimerHandler};
pub use scheduler::worker::Worker;
