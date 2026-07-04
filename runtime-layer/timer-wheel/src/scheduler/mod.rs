//! Scheduling subsystem (Layers 7-8).
//!
//! - [`clock`] — monotonic tick source.
//! - [`shard`] — per-core share-nothing unit (wheel + slab + queues).
//! - [`worker`] — event loop and thread lifecycle.

pub mod clock;
pub mod shard;
pub mod worker;

/// Re-exported so callers can build shards outside the facade if needed.
pub use clock::Clock;
pub use shard::Shard;
pub use worker::Worker;
