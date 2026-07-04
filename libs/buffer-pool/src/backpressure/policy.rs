//! Backpressure policy (Layer 23).
//!
//! [`BackpressurePolicy`] defines the byte thresholds that gate allocation.
//! When outstanding bytes exceed `capacity`,
//! [`BufferPool::try_get`](crate::BufferPool::try_get) rejects and
//! [`BufferPool::get`](crate::BufferPool::get) parks until a release frees
//! room.

use std::time::Duration;

use crate::buffer::config::DEFAULT_MAX_MEMORY;

/// Byte thresholds and timeout for the backpressure gate.
#[derive(Debug, Clone)]
pub struct BackpressurePolicy {
    /// Hard ceiling on outstanding bytes.
    pub capacity: usize,
    /// When `outstanding + requested` exceeds this, `try_get` rejects.
    pub reject_at: usize,
    /// How long `get` parks before timing out.
    pub wait_timeout: Duration,
}

impl BackpressurePolicy {
    /// Creates a policy from a `max_memory` budget.
    ///
    /// `reject_at` is set to 90 % of `capacity`.
    pub fn from_max_memory(max: usize) -> Self {
        Self {
            capacity: max,
            reject_at: (max / 10) * 9,
            wait_timeout: Duration::from_secs(5),
        }
    }

    /// `true` when `outstanding + requested` would exceed `capacity`.
    #[inline]
    pub fn would_exceed(&self, outstanding: usize, requested: usize) -> bool {
        outstanding.saturating_add(requested) > self.capacity
    }
}

impl Default for BackpressurePolicy {
    fn default() -> Self {
        Self::from_max_memory(DEFAULT_MAX_MEMORY)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_uses_standard_budget() {
        let p = BackpressurePolicy::default();
        assert_eq!(p.capacity, DEFAULT_MAX_MEMORY);
        assert_eq!(p.reject_at, (DEFAULT_MAX_MEMORY / 10) * 9);
    }

    #[test]
    fn from_max_memory_sets_90pct_reject() {
        let p = BackpressurePolicy::from_max_memory(100);
        assert_eq!(p.capacity, 100);
        assert_eq!(p.reject_at, 90);
    }

    #[test]
    fn would_exceed_checks_sum() {
        let p = BackpressurePolicy::from_max_memory(1000);
        assert!(!p.would_exceed(500, 400));
        assert!(p.would_exceed(500, 501));
        assert!(p.would_exceed(1000, 1));
    }

    #[test]
    fn would_exceed_saturates() {
        let p = BackpressurePolicy::from_max_memory(100);
        assert!(p.would_exceed(usize::MAX, 1));
    }
}
