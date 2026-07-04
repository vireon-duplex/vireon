//! Fluent builder for [`BufferPool`] (Layer 2).
//!
//! [`BufferPoolBuilder`] lets you tune every field of [`BufferPoolConfig`]
//! with chained calls, then validates before construction:
//!
//! ```
//! use buffer_pool::BufferPool;
//! use buffer_pool::ZeroingPolicy;
//!
//! let pool = BufferPool::builder()
//!     .local_cache(32)
//!     .batch(8)
//!     .zeroing(ZeroingPolicy::Secure)
//!     .build()
//! .unwrap();
//! ```

use crate::buffer::config::BufferPoolConfig;
use crate::buffer::policy::{ShrinkPolicy, ZeroingPolicy};
use crate::buffer::pool::BufferPool;
use crate::error::BufferPoolError;

/// Fluent builder for [`BufferPool`].
///
/// Starts from [`BufferPoolConfig::default`]; each setter overrides one field.
/// Call [`build`](BufferPoolBuilder::build) to validate and construct.
pub struct BufferPoolBuilder {
    config: BufferPoolConfig,
}

impl BufferPoolBuilder {
    /// Creates a builder seeded with [`BufferPoolConfig::default`].
    pub fn new() -> Self {
        Self {
            config: BufferPoolConfig::default(),
        }
    }

    /// Per-class thread-local cache capacity (buffer count).
    #[must_use]
    pub fn local_cache(mut self, n: usize) -> Self {
        self.config.local_cache = n;
        self
    }

    /// Per-shard global cache capacity (buffer count).
    #[must_use]
    pub fn global_cache(mut self, n: usize) -> Self {
        self.config.global_cache = n;
        self
    }

    /// Number of global shards per size class.
    #[must_use]
    pub fn shards(mut self, n: usize) -> Self {
        self.config.shards = n;
        self
    }

    /// Refill / drain batch size.
    #[must_use]
    pub fn batch(mut self, n: usize) -> Self {
        self.config.batch = n;
        self
    }

    /// Sizes above this go through the huge allocator directly.
    #[must_use]
    pub fn huge_threshold(mut self, bytes: usize) -> Self {
        self.config.huge_threshold = bytes;
        self
    }

    /// Hard byte ceiling for retained buffers.
    #[must_use]
    pub fn max_memory(mut self, bytes: usize) -> Self {
        self.config.max_memory = bytes;
        self
    }

    /// How buffers are zeroed on release.
    #[must_use]
    pub fn zeroing(mut self, policy: ZeroingPolicy) -> Self {
        self.config.zeroing = policy;
        self
    }

    /// When the background shrinker runs.
    #[must_use]
    pub fn shrink_policy(mut self, policy: ShrinkPolicy) -> Self {
        self.config.shrink = policy;
        self
    }

    /// Background maintenance tick interval (milliseconds).
    #[must_use]
    pub fn maintenance_interval_ms(mut self, ms: u64) -> Self {
        self.config.maintenance_interval_ms = ms;
        self
    }

    /// Validates the accumulated config and constructs the pool.
    ///
    /// Returns [`BufferPoolError::InvalidConfig`] when a constraint is
    /// violated. See [`BufferPoolConfig::validate`] for the rules.
    pub fn build(self) -> Result<BufferPool, BufferPoolError> {
        self.config
            .validate()
            .map_err(BufferPoolError::InvalidConfig)?;
        Ok(BufferPool::new(self.config))
    }
}

impl Default for BufferPoolBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_builder_produces_valid_pool() {
        let pool = BufferPoolBuilder::default().build().unwrap();
        let buf = pool.get(64).unwrap();
        assert_eq!(buf.len(), 64);
    }

    #[test]
    fn batch_zero_rejected() {
        assert!(BufferPoolBuilder::new().batch(0).build().is_err());
    }

    #[test]
    fn batch_exceeding_local_rejected() {
        assert!(
            BufferPoolBuilder::new()
                .local_cache(4)
                .batch(8)
                .build()
                .is_err()
        );
    }

    #[test]
    fn zero_shards_rejected() {
        assert!(BufferPoolBuilder::new().shards(0).build().is_err());
    }

    #[test]
    fn builder_fields_propagate_to_config() {
        let pool = BufferPoolBuilder::new()
            .local_cache(16)
            .global_cache(256)
            .shards(2)
            .batch(4)
            .huge_threshold(4096)
            .max_memory(16 * 1024 * 1024)
            .build()
            .unwrap();
        let cfg = pool.config();
        assert_eq!(cfg.local_cache, 16);
        assert_eq!(cfg.global_cache, 256);
        assert_eq!(cfg.shards, 2);
        assert_eq!(cfg.batch, 4);
        assert_eq!(cfg.huge_threshold, 4096);
    }
}
