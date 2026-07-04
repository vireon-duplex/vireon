//! Concurrency primitives (Layer 6).
//!
//! - [`mpsc`] — bounded lock-free MPSC queue (Vyukov algorithm).
//! - [`sharder`] — routes a `task_id` to a [`ShardId`].

pub mod mpsc;
pub mod sharder;
