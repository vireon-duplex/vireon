//! [`ReactorConfig`] + [`ReactorPreset`] — construction-time
//! configuration for the reactor.
//!
//! See `DESIGN.md` §2 (architecture) for the configuration surface
//! and §16 (scalability) for the preset rationale.
//!
//! # Validation
//!
//! [`ReactorConfig::validate`] is called by
//! [`ReactorBuilder::build`](crate::builder::ReactorBuilder::build)
//! (Phase 1). All ranges below must be satisfied.

use crate::budget::BudgetConfig;
use crate::error::ReactorError;
use crate::idle::IdleStrategy;

// ── ring / CQ constants ──────────────────────────────────────────

/// Default ring depth (SQ + CQ).
pub const DEFAULT_RING_ENTRIES: u32 = 2048;
/// Minimum ring depth.
pub const MIN_RING_ENTRIES: u32 = 256;
/// Maximum ring depth.
pub const MAX_RING_ENTRIES: u32 = 32768;

/// Default provided-buffer ring depth (RX).
pub const DEFAULT_BUF_RING_ENTRIES: u32 = 4096;
/// Minimum provided-buffer ring depth.
pub const MIN_BUF_RING_ENTRIES: u32 = 64;
/// Maximum provided-buffer ring depth.
pub const MAX_BUF_RING_ENTRIES: u32 = 16384;

// ── CPU / NUMA defaults ──────────────────────────────────────────

/// Default CPU ID if none is specified.
pub const DEFAULT_CPU_ID: u32 = 0;
/// Default NUMA node if none is specified.
pub const DEFAULT_NUMA_NODE: u32 = 0;

/// Configuration for one reactor instance. Built directly or via
/// [`ReactorPreset`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReactorConfig {
    /// CPU core to pin the reactor thread to.
    pub cpu_id: u32,
    /// NUMA node hint for memory allocation.
    pub numa_node: u32,
    /// io_uring SQ/CQ depth.
    pub ring_entries: u32,
    /// Maximum CQEs drained per tick. Also bounded by
    /// `BudgetConfig::max_cqes`; the reactor uses `min()`.
    pub batch_max_cqes: u32,
    /// Provided-buffer ring depth (RX).
    pub buf_ring_entries: u32,
    /// Whether to enable `IORING_SETUP_SQPOLL` (kernel-side
    /// polling thread for SQE submission). Requires a dedicated
    /// core; off in `Cloud` and `LowLatency`.
    pub sqpoll_enabled: bool,
    /// Whether to enable `SEND_ZC` for zero-copy TX. Falls back to
    /// `SENDMMSG` when disabled.
    pub enable_zerocopy_send: bool,
    /// Idle strategy when the ring is empty.
    pub idle_strategy: IdleStrategy,
    /// Per-tick subsystem budget.
    pub budget: BudgetConfig,
}

impl Default for ReactorConfig {
    fn default() -> Self {
        Self {
            cpu_id: DEFAULT_CPU_ID,
            numa_node: DEFAULT_NUMA_NODE,
            ring_entries: DEFAULT_RING_ENTRIES,
            batch_max_cqes: crate::budget::DEFAULT_MAX_CQES,
            buf_ring_entries: DEFAULT_BUF_RING_ENTRIES,
            sqpoll_enabled: false,
            enable_zerocopy_send: false,
            idle_strategy: IdleStrategy::default(),
            budget: BudgetConfig::default(),
        }
    }
}

impl ReactorConfig {
    /// Validate every field against its allowed range.
    ///
    /// # Errors
    /// Returns [`ReactorError::InvalidConfig`] on any out-of-range
    /// field.
    pub fn validate(&self) -> Result<(), ReactorError> {
        if self.cpu_id > crate::pin::MAX_CPU_ID {
            return Err(ReactorError::InvalidConfig(format!(
                "cpu_id {} exceeds {}",
                self.cpu_id,
                crate::pin::MAX_CPU_ID
            )));
        }
        if !(MIN_RING_ENTRIES..=MAX_RING_ENTRIES).contains(&self.ring_entries) {
            return Err(ReactorError::InvalidConfig(format!(
                "ring_entries {} out of range [{}, {}]",
                self.ring_entries, MIN_RING_ENTRIES, MAX_RING_ENTRIES
            )));
        }
        if self.batch_max_cqes == 0 || self.batch_max_cqes > self.ring_entries {
            return Err(ReactorError::InvalidConfig(format!(
                "batch_max_cqes {} must be in 1..={}",
                self.batch_max_cqes, self.ring_entries
            )));
        }
        if !(MIN_BUF_RING_ENTRIES..=MAX_BUF_RING_ENTRIES).contains(&self.buf_ring_entries) {
            return Err(ReactorError::InvalidConfig(format!(
                "buf_ring_entries {} out of range [{}, {}]",
                self.buf_ring_entries, MIN_BUF_RING_ENTRIES, MAX_BUF_RING_ENTRIES
            )));
        }
        self.idle_strategy.validate()?;
        self.budget.validate()?;
        Ok(())
    }
}

/// Tuned configuration presets. Each one returns a fully populated
/// [`ReactorConfig`] via [`ReactorPreset::to_config`].
///
/// See `DESIGN.md` §16 for the workload assumptions behind each
/// preset.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReactorPreset {
    /// Cloud VM with limited cores and unknown NUMA topology.
    /// Conservative ring sizes, hybrid idle, no SQPOLL.
    Cloud,
    /// Bare-metal with dedicated cores. Large rings, SQPOLL on,
    /// zero-copy send on.
    BareMetal,
    /// 100 GbE NIC, multi-queue. Very large rings + buf-ring,
    /// block idle (the NIC is never idle for long).
    HundredGbe,
    /// Latency-sensitive workload (financial trading, gaming).
    /// Smaller rings, pure spin, no SQPOLL overhead.
    LowLatency,
    /// Maximum throughput (bulk transfer, CDNs). Very large
    /// rings, SQPOLL + zero-copy, block idle.
    MaxThroughput,
}

impl ReactorPreset {
    /// Materialise the preset into a [`ReactorConfig`].
    #[must_use]
    #[allow(clippy::too_many_lines)] // tabular preset data
    pub fn to_config(self, cpu_id: u32) -> ReactorConfig {
        use crate::budget::{BudgetConfig, DEFAULT_DEADLINE_NS};
        use crate::idle::IdleStrategy;

        match self {
            Self::Cloud => ReactorConfig {
                cpu_id,
                numa_node: 0,
                ring_entries: 1024,
                batch_max_cqes: 128,
                buf_ring_entries: 1024,
                sqpoll_enabled: false,
                enable_zerocopy_send: false,
                idle_strategy: IdleStrategy::default(),
                budget: BudgetConfig {
                    max_cqes: 128,
                    max_tasks: 32,
                    max_send_flush: 64,
                    deadline_ns: DEFAULT_DEADLINE_NS,
                },
            },
            Self::BareMetal => ReactorConfig {
                cpu_id,
                numa_node: 0,
                ring_entries: 4096,
                batch_max_cqes: 256,
                buf_ring_entries: 4096,
                sqpoll_enabled: true,
                enable_zerocopy_send: true,
                idle_strategy: IdleStrategy::default(),
                budget: BudgetConfig {
                    max_cqes: 256,
                    max_tasks: 64,
                    max_send_flush: 128,
                    deadline_ns: DEFAULT_DEADLINE_NS,
                },
            },
            Self::HundredGbe => ReactorConfig {
                cpu_id,
                numa_node: 0,
                ring_entries: 8192,
                batch_max_cqes: 512,
                buf_ring_entries: 8192,
                sqpoll_enabled: true,
                enable_zerocopy_send: true,
                idle_strategy: IdleStrategy::Block,
                budget: BudgetConfig {
                    max_cqes: 512,
                    max_tasks: 128,
                    max_send_flush: 256,
                    deadline_ns: 100_000, // 100 μs
                },
            },
            Self::LowLatency => ReactorConfig {
                cpu_id,
                numa_node: 0,
                ring_entries: 2048,
                batch_max_cqes: 64,
                buf_ring_entries: 2048,
                sqpoll_enabled: false,
                enable_zerocopy_send: false,
                idle_strategy: IdleStrategy::Spin,
                budget: BudgetConfig {
                    max_cqes: 64,
                    max_tasks: 16,
                    max_send_flush: 32,
                    deadline_ns: 50_000, // 50 μs
                },
            },
            Self::MaxThroughput => ReactorConfig {
                cpu_id,
                numa_node: 0,
                ring_entries: 8192,
                batch_max_cqes: 512,
                buf_ring_entries: 8192,
                sqpoll_enabled: true,
                enable_zerocopy_send: true,
                idle_strategy: IdleStrategy::Block,
                budget: BudgetConfig {
                    max_cqes: 512,
                    max_tasks: 128,
                    max_send_flush: 512,
                    deadline_ns: 500_000, // 500 μs
                },
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_validates() -> Result<(), ReactorError> {
        ReactorConfig::default().validate()?;
        Ok(())
    }

    #[test]
    fn validate_rejects_tiny_ring() {
        let cfg = ReactorConfig {
            ring_entries: MIN_RING_ENTRIES / 2,
            ..ReactorConfig::default()
        };
        assert!(matches!(
            cfg.validate(),
            Err(ReactorError::InvalidConfig(_))
        ));
    }

    #[test]
    fn validate_rejects_batch_larger_than_ring() {
        let cfg = ReactorConfig {
            ring_entries: 256,
            batch_max_cqes: 257,
            ..ReactorConfig::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_huge_cpu() {
        let cfg = ReactorConfig {
            cpu_id: crate::pin::MAX_CPU_ID + 1,
            ..ReactorConfig::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn all_presets_validate_on_cpu_zero() -> Result<(), ReactorError> {
        for p in [
            ReactorPreset::Cloud,
            ReactorPreset::BareMetal,
            ReactorPreset::HundredGbe,
            ReactorPreset::LowLatency,
            ReactorPreset::MaxThroughput,
        ] {
            p.to_config(0).validate()?;
        }
        Ok(())
    }

    #[test]
    fn hundred_gbe_uses_block_idle() {
        let cfg = ReactorPreset::HundredGbe.to_config(0);
        assert_eq!(cfg.idle_strategy, IdleStrategy::Block);
        assert!(cfg.sqpoll_enabled);
    }

    #[test]
    fn low_latency_uses_spin_idle() {
        let cfg = ReactorPreset::LowLatency.to_config(0);
        assert_eq!(cfg.idle_strategy, IdleStrategy::Spin);
        assert!(!cfg.sqpoll_enabled);
    }
}
