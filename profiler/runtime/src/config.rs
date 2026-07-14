//! Profiler configuration.
//!
//! [`ProfilerConfig`] is passed to [`crate::Profiler::init()`] to set
//! runtime parameters. After `init()`, changes take effect immediately
//! via the global atomic knobs in [`crate::collector`].

use std::path::PathBuf;

/// Configuration for the profiler.
#[derive(Clone, Debug)]
pub struct ProfilerConfig {
    /// Whether profiling is active at startup.
    pub enabled: bool,

    /// Sampling rate: 1 = every call, N = every Nth call.
    /// Higher values reduce overhead at the cost of accuracy.
    pub sampling: u32,

    /// Where to write exported reports (if `None`, caller handles output).
    pub export_path: Option<PathBuf>,

    /// Background flush interval in milliseconds.
    pub flush_interval_ms: u64,

    /// Histogram sub-buckets per octave (controls precision).
    /// Currently informational — the histogram is fixed at 16 sub/octave.
    pub histogram_precision: u8,

    /// Thread-local buffer capacity (calls before a flush).
    pub thread_local_capacity: usize,
}

impl Default for ProfilerConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            sampling: 1,
            export_path: None,
            flush_interval_ms: 1000,
            histogram_precision: 16,
            thread_local_capacity: 1024,
        }
    }
}

impl ProfilerConfig {
    /// Creates a builder for [`ProfilerConfig`].
    #[must_use]
    pub fn builder() -> ProfilerConfigBuilder {
        ProfilerConfigBuilder::default()
    }
}

/// Builder for [`ProfilerConfig`].
#[derive(Default)]
pub struct ProfilerConfigBuilder {
    inner: ProfilerConfig,
}

impl ProfilerConfigBuilder {
    /// Enables profiling at startup.
    #[must_use]
    pub fn enabled(mut self) -> Self {
        self.inner.enabled = true;
        self
    }

    /// Sets the sampling rate.
    #[must_use]
    pub fn sampling(mut self, rate: u32) -> Self {
        self.inner.sampling = rate.max(1);
        self
    }

    /// Sets the export path.
    #[must_use]
    pub fn export_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.inner.export_path = Some(path.into());
        self
    }

    /// Sets the background flush interval.
    #[must_use]
    pub fn flush_interval_ms(mut self, ms: u64) -> Self {
        self.inner.flush_interval_ms = ms;
        self
    }

    /// Sets the thread-local capacity (flush threshold).
    #[must_use]
    pub fn thread_local_capacity(mut self, cap: usize) -> Self {
        self.inner.thread_local_capacity = cap.max(1);
        self
    }

    /// Builds the config.
    #[must_use]
    pub fn build(self) -> ProfilerConfig {
        self.inner
    }
}

// ── Tests ─────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_values() {
        let cfg = ProfilerConfig::default();
        assert!(!cfg.enabled);
        assert_eq!(cfg.sampling, 1);
        assert_eq!(cfg.flush_interval_ms, 1000);
        assert_eq!(cfg.thread_local_capacity, 1024);
        assert!(cfg.export_path.is_none());
    }

    #[test]
    fn builder_custom_config() {
        let cfg = ProfilerConfig::builder()
            .enabled()
            .sampling(5)
            .flush_interval_ms(500)
            .thread_local_capacity(2048)
            .export_path("/tmp/profile.json")
            .build();
        assert!(cfg.enabled);
        assert_eq!(cfg.sampling, 5);
        assert_eq!(cfg.flush_interval_ms, 500);
        assert_eq!(cfg.thread_local_capacity, 2048);
        assert_eq!(
            cfg.export_path.as_ref().map(|p| p.to_str().unwrap()),
            Some("/tmp/profile.json")
        );
    }

    #[test]
    fn builder_clamps_sampling_to_min_1() {
        let cfg = ProfilerConfig::builder().sampling(0).build();
        assert_eq!(cfg.sampling, 1);
    }

    #[test]
    fn builder_clamps_capacity_to_min_1() {
        let cfg = ProfilerConfig::builder().thread_local_capacity(0).build();
        assert_eq!(cfg.thread_local_capacity, 1);
    }
}
