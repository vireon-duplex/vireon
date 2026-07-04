//! Configuration for a [`BufferPool`](super::BufferPool) (Layer 24).
//!
//! [`BufferPoolConfig`] is a plain value type so it can be cloned, inspected,
//! and adjusted before construction. Sensible [`Default`]s make the common
//! case zero-config.

use crate::buffer::policy::{ShrinkPolicy, ZeroingPolicy};

/// Default threshold above which allocations go through the huge path.
pub const DEFAULT_HUGE_THRESHOLD: usize = 1 << 20; // 1 MiB
/// Default per-class thread-local cache capacity (number of buffers).
pub const DEFAULT_LOCAL_CACHE: usize = 64;
/// Default per-shard global cache capacity (number of buffers).
pub const DEFAULT_GLOBAL_CACHE: usize = 1024;
/// Default batch size for thread-local <-> global transfers.
pub const DEFAULT_BATCH: usize = 16;
/// Default number of global-cache shards per class.
pub const DEFAULT_SHARDS: usize = 8;
/// Default maximum bytes the pool may hold before backpressure engages.
pub const DEFAULT_MAX_MEMORY: usize = 256 << 20; // 256 MiB
/// Default maintenance-tick interval (milliseconds).
pub const DEFAULT_MAINTENANCE_INTERVAL_MS: u64 = 1000;

/// Full configuration for a buffer pool.
///
/// Construct via [`BufferPoolConfig::default`] and adjust fields, or use
/// [`BufferPoolBuilder`](crate::buffer::BufferPoolBuilder) for validated
/// construction.
#[derive(Debug, Clone)]
pub struct BufferPoolConfig {
    /// Per-class thread-local cache capacity (buffer count).
    pub local_cache: usize,
    /// Per-shard global cache capacity (buffer count).
    pub global_cache: usize,
    /// Number of global shards per size class.
    pub shards: usize,
    /// Default refill/drain batch size.
    pub batch: usize,
    /// Sizes above this go through the huge allocator directly.
    pub huge_threshold: usize,
    /// Hard byte ceiling for retained buffers.
    pub max_memory: usize,
    /// How buffers are zeroed on release.
    pub zeroing: ZeroingPolicy,
    /// When the background shrinker runs.
    pub shrink: ShrinkPolicy,
    /// Background maintenance tick interval (milliseconds).
    pub maintenance_interval_ms: u64,
}

impl Default for BufferPoolConfig {
    fn default() -> Self {
        Self {
            local_cache: DEFAULT_LOCAL_CACHE,
            global_cache: DEFAULT_GLOBAL_CACHE,
            shards: DEFAULT_SHARDS,
            batch: DEFAULT_BATCH,
            huge_threshold: DEFAULT_HUGE_THRESHOLD,
            max_memory: DEFAULT_MAX_MEMORY,
            zeroing: ZeroingPolicy::default(),
            shrink: ShrinkPolicy::default(),
            maintenance_interval_ms: DEFAULT_MAINTENANCE_INTERVAL_MS,
        }
    }
}

impl BufferPoolConfig {
    /// Validates the configuration, returning `Err` with a descriptive message
    /// when a constraint is violated.
    ///
    /// Rules:
    /// - `batch`, `local_cache`, `global_cache`, `shards` are all `>= 1`.
    /// - `batch <= local_cache` (a refill must fit in the local cache).
    /// - `huge_threshold >= 1024` (the class table must cover at least 1 KiB).
    /// - `max_memory >= huge_threshold` (the budget must admit at least one huge buffer).
    /// - `maintenance_interval_ms >= 10` (no busy-looping).
    pub fn validate(&self) -> Result<(), String> {
        if self.batch == 0 {
            return Err("batch must be >= 1".into());
        }
        if self.local_cache == 0 {
            return Err("local_cache must be >= 1".into());
        }
        if self.global_cache == 0 {
            return Err("global_cache must be >= 1".into());
        }
        if self.shards == 0 {
            return Err("shards must be >= 1".into());
        }
        if self.batch > self.local_cache {
            return Err(format!(
                "batch ({}) must be <= local_cache ({})",
                self.batch, self.local_cache
            ));
        }
        if self.huge_threshold < 1024 {
            return Err("huge_threshold must be >= 1024".into());
        }
        if self.max_memory < self.huge_threshold {
            return Err(format!(
                "max_memory ({}) must be >= huge_threshold ({})",
                self.max_memory, self.huge_threshold
            ));
        }
        if self.maintenance_interval_ms < 10 {
            return Err("maintenance_interval_ms must be >= 10".into());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_passes_validation() {
        BufferPoolConfig::default().validate().unwrap();
    }

    #[test]
    fn zero_batch_rejected() {
        let c = BufferPoolConfig {
            batch: 0,
            ..Default::default()
        };
        assert!(c.validate().is_err());
    }

    #[test]
    fn batch_exceeding_local_rejected() {
        let default = BufferPoolConfig::default();
        let c = BufferPoolConfig {
            batch: default.local_cache + 1,
            ..default
        };
        let err = c.validate().unwrap_err();
        assert!(err.contains("must be <= local_cache"));
    }

    #[test]
    fn tiny_huge_threshold_rejected() {
        let c = BufferPoolConfig {
            huge_threshold: 512,
            ..Default::default()
        };
        assert!(c.validate().is_err());
    }

    #[test]
    fn budget_below_huge_threshold_rejected() {
        let c = BufferPoolConfig {
            huge_threshold: 1 << 20,
            max_memory: (1 << 20) - 1,
            ..Default::default()
        };
        assert!(c.validate().is_err());
    }

    #[test]
    fn zero_shards_rejected() {
        let c = BufferPoolConfig {
            shards: 0,
            ..Default::default()
        };
        assert!(c.validate().is_err());
    }

    #[test]
    fn busy_loop_interval_rejected() {
        let c = BufferPoolConfig {
            maintenance_interval_ms: 1,
            ..Default::default()
        };
        assert!(c.validate().is_err());
    }
}
