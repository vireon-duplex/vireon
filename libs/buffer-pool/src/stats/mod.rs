//! Statistics collection (Layers 13-14).
//!
//! - [`counters`] — atomic hit/miss/reuse/alloc/free counters.
//! - [`histogram`] — fixed-bucket latency distribution.
//! - [`snapshot`] — plain value types for cross-thread observation.

pub mod counters;
pub mod histogram;
pub mod snapshot;

pub use counters::Counters;
pub use histogram::LatencyHistogram;
pub use snapshot::{CountersSnapshot, HistogramSnapshot, Percentiles, StatsSnapshot};
