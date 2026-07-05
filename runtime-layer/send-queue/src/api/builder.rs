//! [`SendQueueBuilder`] — fluent builder for [`SendQueue`].

use crate::api::facade::SendQueue;
use crate::backpressure::PressureObserver;
use crate::config::{
    DEFAULT_HIGH_WATERMARK_PCT, DEFAULT_HYSTERESIS_PCT, DEFAULT_LOW_WATERMARK_PCT, RetryConfig,
    SchedulePolicyKind, SendQueueConfig,
};
use crate::error::SendQueueError;

/// Fluent builder for [`SendQueue`].
///
/// All setters consume and return `self` (annotated `#[must_use]`).
/// Call [`build`](Self::build) to construct the queue.
#[derive(Default)]
pub struct SendQueueBuilder {
    config: SendQueueConfig,
    observers: Vec<Box<dyn PressureObserver>>,
}

impl SendQueueBuilder {
    /// Creates a builder populated with [`SendQueueConfig::default`].
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Ingress ring capacity (rounded up to a power of two). Default: 4096.
    ///
    /// Calling this recomputes the watermark defaults (low, high,
    /// hysteresis) as percentages of the new capacity. To override a
    /// watermark explicitly, call the corresponding setter **after**
    /// `.capacity()`.
    #[must_use]
    pub fn capacity(mut self, cap: usize) -> Self {
        self.config.capacity = cap;
        // Recompute watermark defaults for the new capacity so they
        // scale proportionally. If the caller overrides a watermark
        // after this call, their value wins (standard builder ordering).
        self.config.low_watermark = cap * DEFAULT_LOW_WATERMARK_PCT as usize / 100;
        self.config.high_watermark = cap * DEFAULT_HIGH_WATERMARK_PCT as usize / 100;
        self.config.hysteresis = cap * DEFAULT_HYSTERESIS_PCT as usize / 100;
        self
    }

    /// Scheduling policy. Default: [`Fifo`](SchedulePolicyKind::Fifo).
    #[must_use]
    pub fn policy(mut self, policy: SchedulePolicyKind) -> Self {
        self.config.policy = policy;
        self
    }

    /// Maximum packets per batch. Default: 64.
    #[must_use]
    pub fn batch_max_packets(mut self, n: usize) -> Self {
        self.config.batch_max_packets = n;
        self
    }

    /// Maximum bytes per batch. Default: 65 536.
    #[must_use]
    pub fn batch_max_bytes(mut self, n: usize) -> Self {
        self.config.batch_max_bytes = n;
        self
    }

    /// Maximum nanoseconds a packet waits in batch before flush. Default: 50 000.
    #[must_use]
    pub fn batch_max_age_ns(mut self, ns: u64) -> Self {
        self.config.batch_max_age_ns = ns;
        self
    }

    /// Enable or disable packet coalescing. Default: `false`.
    #[must_use]
    pub fn coalesce(mut self, enabled: bool) -> Self {
        self.config.coalesce_enabled = enabled;
        self
    }

    /// Low watermark for backpressure. Default: 75% of capacity.
    #[must_use]
    pub fn low_watermark(mut self, depth: usize) -> Self {
        self.config.low_watermark = depth;
        self
    }

    /// High watermark for backpressure. Default: 90% of capacity.
    #[must_use]
    pub fn high_watermark(mut self, depth: usize) -> Self {
        self.config.high_watermark = depth;
        self
    }

    /// Hysteresis band for pressure transitions. Default: 10% of capacity.
    #[must_use]
    pub fn hysteresis(mut self, depth: usize) -> Self {
        self.config.hysteresis = depth;
        self
    }

    /// Retry configuration. Default: initial RTO 1 ms, max 3 retries.
    #[must_use]
    pub fn retry(mut self, cfg: RetryConfig) -> Self {
        self.config.retry = cfg;
        self
    }

    /// Registers a pressure observer.
    #[must_use]
    pub fn observer(mut self, obs: Box<dyn PressureObserver>) -> Self {
        self.observers.push(obs);
        self
    }

    /// Validates the configuration and constructs the [`SendQueue`].
    pub fn build(self) -> Result<SendQueue, SendQueueError> {
        let mut sq = SendQueue::from_config(self.config)?;
        for obs in self.observers {
            sq.add_observer(obs);
        }
        Ok(sq)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_build_succeeds() {
        let sq = SendQueueBuilder::new().build();
        assert!(sq.is_ok());
    }

    #[test]
    fn capacity_is_power_of_two() {
        let sq = SendQueueBuilder::new()
            .capacity(1000)
            .build()
            .expect("build");
        assert!(sq.capacity().is_power_of_two());
        assert_eq!(sq.capacity(), 1024);
    }

    #[test]
    fn rejects_invalid_config() {
        let err = SendQueueBuilder::new().capacity(4).build().unwrap_err();
        assert!(matches!(err, SendQueueError::InvalidConfig(_)));
    }

    #[test]
    fn custom_retry_config() {
        let cfg = RetryConfig {
            max_retries: 5,
            ..RetryConfig::default()
        };
        let sq = SendQueueBuilder::new().retry(cfg).build().expect("build");
        assert_eq!(sq.capacity(), 4096);
    }

    #[test]
    fn watermark_override() {
        let sq = SendQueueBuilder::new()
            .capacity(256)
            .low_watermark(100)
            .high_watermark(200)
            .hysteresis(10)
            .build()
            .expect("build");
        assert_eq!(sq.capacity(), 256);
    }
}
