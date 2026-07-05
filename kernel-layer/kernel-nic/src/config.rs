//! Strongly-typed configuration for the kernel-nic layer.
//!
//! [`NicConfig`] is the user-facing bag of tunables. It is consumed by
//! [`NicManagerBuilder`](crate::NicManagerBuilder) and cloned into the
//! running [`NicManager`](crate::NicManager) for later consultation
//! during recovery and reconfiguration.

use std::time::Duration;

use crate::error::KernelNicError;

// ── Tunable defaults ────────────────────────────────────────────────

/// Default RX queue count target when the NIC exposes more queues than
/// cores on its NUMA node.
pub const DEFAULT_TARGET_RX_QUEUES_PER_CORE: u32 = 1;

/// Default socket receive buffer (bytes).
pub const DEFAULT_RCVBUF: u32 = 16 * 1024 * 1024;

/// Default socket send buffer (bytes).
pub const DEFAULT_SNDBUF: u32 = 16 * 1024 * 1024;

/// Default socket pool depth per spec per core.
pub const DEFAULT_SOCKET_POOL_DEPTH: usize = 2;

/// Default grace period for shutting down a socket group.
pub const DEFAULT_SHUTDOWN_GRACE: Duration = Duration::from_secs(5);

/// Maximum CPUs supported by the layer. Bounds `CpuSet` so per-core
/// structures stay fixed-size and off the heap allocator on the hot
/// path.
pub const MAX_CPUS: usize = 1024;

/// Maximum queues per NIC tracked by stats counters.
pub const MAX_QUEUES: usize = 256;

// ── Presets ─────────────────────────────────────────────────────────

/// Pre-baked configuration profiles. See [`Preset::apply`] for the
/// exact field values.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Preset {
    /// Cloud VM with limited NIC features (virtio, ena, vmxnet3).
    Cloud,
    /// Bare metal with full kernel access.
    BareMetal,
    /// 100 GbE NIC, throughput-optimised.
    HundredGbe,
    /// 25 GbE NIC, balanced.
    TwentyFiveGbe,
    /// Sub-100µs p99 latency target.
    LowLatency,
    /// Maximum throughput, latency-insensitive.
    MaxThroughput,
}

impl Preset {
    /// Returns a [`NicConfig`] populated with sensible values for the
    /// preset. Callers may further customise the result before passing
    /// it to [`NicManagerBuilder::config`](crate::NicManagerBuilder::config).
    #[must_use]
    pub fn apply(self) -> NicConfig {
        let mut c = NicConfig::default();
        match self {
            Self::Cloud => {
                c.target_rx_queues_per_core = 1;
                c.rcvbuf = 4 * 1024 * 1024;
                c.sndbuf = 4 * 1024 * 1024;
                c.socket_pool_depth = 0;
                c.enable_udp_gro = true;
                c.enable_udp_segment = true;
                c.skip_irq_config = true;
                c.skip_sysctl_apply = true;
                c.busy_poll_us = 0;
            }
            Self::BareMetal => {
                c.target_rx_queues_per_core = 1;
                c.rcvbuf = DEFAULT_RCVBUF;
                c.sndbuf = DEFAULT_SNDBUF;
                c.socket_pool_depth = DEFAULT_SOCKET_POOL_DEPTH;
                c.enable_udp_gro = true;
                c.enable_udp_segment = true;
                c.busy_poll_us = 50;
            }
            Self::HundredGbe | Self::MaxThroughput => {
                c.target_rx_queues_per_core = 1;
                c.rcvbuf = 64 * 1024 * 1024;
                c.sndbuf = 64 * 1024 * 1024;
                c.socket_pool_depth = 8;
                c.enable_udp_gro = true;
                c.enable_udp_segment = true;
                c.busy_poll_us = if matches!(self, Self::MaxThroughput) {
                    0
                } else {
                    200
                };
            }
            Self::TwentyFiveGbe => {
                c.target_rx_queues_per_core = 1;
                c.rcvbuf = 32 * 1024 * 1024;
                c.sndbuf = 32 * 1024 * 1024;
                c.socket_pool_depth = 4;
                c.enable_udp_gro = true;
                c.enable_udp_segment = true;
                c.busy_poll_us = 50;
            }
            Self::LowLatency => {
                c.target_rx_queues_per_core = 1;
                c.rcvbuf = 8 * 1024 * 1024;
                c.sndbuf = 8 * 1024 * 1024;
                c.socket_pool_depth = 4;
                c.enable_udp_gro = true;
                c.enable_udp_segment = true;
                c.busy_poll_us = 50;
                c.prefer_busy_poll = true;
            }
        }
        c
    }
}

// ── NicConfig ───────────────────────────────────────────────────────

/// User-facing configuration. Constructed via [`Self::default`] or
/// [`Preset::apply`].
#[derive(Clone, Debug)]
pub struct NicConfig {
    /// Reactor id for diagnostics (mirrors the io-layer reactor id).
    pub reactor_id: u32,

    /// How many RX queues to aim for per core on the NIC's NUMA node.
    pub target_rx_queues_per_core: u32,

    /// Socket `SO_RCVBUF` (bytes). 0 = leave kernel default.
    pub rcvbuf: u32,

    /// Socket `SO_SNDBUF` (bytes). 0 = leave kernel default.
    pub sndbuf: u32,

    /// `SO_BUSY_POLL` micro-seconds. 0 = do not set.
    pub busy_poll_us: u32,

    /// Set `SO_PREFER_BUSY_POLL=1` when supported.
    pub prefer_busy_poll: bool,

    /// Enable UDP GRO on sockets when supported.
    pub enable_udp_gro: bool,

    /// Enable UDP GSO (UDP_SEGMENT) on sockets when supported.
    pub enable_udp_segment: bool,

    /// Per-core socket pool depth (recycled bound sockets per spec).
    pub socket_pool_depth: usize,

    /// When `true`, do not write IRQ affinity / RSS / XPS. Used by
    /// cloud presets where we lack `CAP_NET_ADMIN`.
    pub skip_irq_config: bool,

    /// When `true`, do not apply sysctls. Implies `read_only` for the
    /// `/proc/sys/net` side of configuration.
    pub skip_sysctl_apply: bool,

    /// Restore IRQ / RSS / XPS / sysctl state on shutdown.
    pub restore_state_on_shutdown: bool,

    /// Fail-fast during [`NicManagerBuilder::build`](crate::NicManagerBuilder::build)
    /// if any non-fatal discovery step reports a warning.
    pub strict_init: bool,

    /// Grace period for socket group shutdown.
    pub shutdown_grace: Duration,
}

impl Default for NicConfig {
    fn default() -> Self {
        Self {
            reactor_id: 0,
            target_rx_queues_per_core: DEFAULT_TARGET_RX_QUEUES_PER_CORE,
            rcvbuf: DEFAULT_RCVBUF,
            sndbuf: DEFAULT_SNDBUF,
            busy_poll_us: 0,
            prefer_busy_poll: false,
            enable_udp_gro: true,
            enable_udp_segment: true,
            socket_pool_depth: DEFAULT_SOCKET_POOL_DEPTH,
            skip_irq_config: false,
            skip_sysctl_apply: false,
            restore_state_on_shutdown: true,
            strict_init: false,
            shutdown_grace: DEFAULT_SHUTDOWN_GRACE,
        }
    }
}

impl NicConfig {
    /// Sanity-checks the config. Returns `Err` on the first issue found.
    pub fn validate(&self) -> Result<(), KernelNicError> {
        if self.socket_pool_depth > 64 {
            return Err(KernelNicError::InvalidConfig(format!(
                "socket_pool_depth must be <= 64 (is {})",
                self.socket_pool_depth
            )));
        }
        if self.busy_poll_us > 1_000_000 {
            return Err(KernelNicError::InvalidConfig(format!(
                "busy_poll_us must be <= 1_000_000 (is {})",
                self.busy_poll_us
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_passes_validation() {
        NicConfig::default().validate().expect("default config");
    }

    #[test]
    fn pool_depth_overflow_rejected() {
        let c = NicConfig {
            socket_pool_depth: 65,
            ..NicConfig::default()
        };
        assert!(c.validate().is_err());
    }

    #[test]
    fn busy_poll_overflow_rejected() {
        let c = NicConfig {
            busy_poll_us: 2_000_000,
            ..NicConfig::default()
        };
        assert!(c.validate().is_err());
    }

    #[test]
    fn presets_produce_distinct_configs() {
        let cloud = Preset::Cloud.apply();
        let bm = Preset::BareMetal.apply();
        let h100 = Preset::HundredGbe.apply();
        let lat = Preset::LowLatency.apply();

        assert!(cloud.skip_irq_config);
        assert!(cloud.skip_sysctl_apply);
        assert!(!bm.skip_irq_config);
        assert_eq!(h100.rcvbuf, 64 * 1024 * 1024);
        assert!(lat.prefer_busy_poll);
    }

    #[test]
    fn all_presets_validate() {
        for p in [
            Preset::Cloud,
            Preset::BareMetal,
            Preset::HundredGbe,
            Preset::TwentyFiveGbe,
            Preset::LowLatency,
            Preset::MaxThroughput,
        ] {
            let result = p.apply().validate();
            assert!(result.is_ok(), "preset {p:?} failed: {:?}", result.err());
        }
    }
}
