//! Busy-poll policy + application helpers.
//!
//! [`BusyPollPolicy`] drives `SO_BUSY_POLL` / `SO_PREFER_BUSY_POLL` on
//! per-socket configuration. The actual `setsockopt(2)` calls live in
//! [`socket::builder`](crate::socket::builder) — this module only owns
//! the policy decision.

/// Busy-poll mode.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BusyPollMode {
    /// No busy polling at all.
    Off,
    /// Static `SO_BUSY_POLL=Nµs`.
    Static { us: u32 },
    /// Latency mode: small budget, prefer busy poll enabled.
    Latency { us: u32 },
    /// Throughput mode: larger budget, prefer busy poll enabled.
    Throughput { us: u32 },
    /// Adaptive: kernel-driven via NAPI id; we only set the floor.
    Adaptive { min_us: u32 },
}

impl BusyPollMode {
    /// Returns the `us` value to write to `SO_BUSY_POLL`, or `0` if
    /// busy polling is off.
    #[must_use]
    pub const fn us(self) -> u32 {
        match self {
            Self::Off => 0,
            Self::Static { us }
            | Self::Latency { us }
            | Self::Throughput { us }
            | Self::Adaptive { min_us: us } => us,
        }
    }

    /// Returns `true` when `SO_PREFER_BUSY_POLL=1` should be applied.
    #[must_use]
    pub const fn prefer_busy_poll(self) -> bool {
        matches!(
            self,
            Self::Latency { .. } | Self::Throughput { .. } | Self::Adaptive { .. }
        )
    }
}

/// Full busy-poll policy. Carries both the mode and a saved
/// capability flag (`KernelFeatures::SO_PREFER_BUSY_POLL`) snapshot
/// so the per-socket code path is branch-free.
#[derive(Clone, Copy, Debug)]
pub struct BusyPollPolicy {
    pub mode: BusyPollMode,
    pub supports_prefer: bool,
}

impl Default for BusyPollPolicy {
    fn default() -> Self {
        Self {
            mode: BusyPollMode::Off,
            supports_prefer: false,
        }
    }
}

impl BusyPollPolicy {
    /// Construct from mode + capability.
    #[must_use]
    pub const fn new(mode: BusyPollMode, supports_prefer: bool) -> Self {
        Self {
            mode,
            supports_prefer,
        }
    }

    /// `true` if the policy requires any `setsockopt` calls.
    #[must_use]
    pub const fn active(self) -> bool {
        self.mode.us() > 0
    }

    /// `true` if the policy should set `SO_PREFER_BUSY_POLL=1`.
    #[must_use]
    pub const fn should_prefer(self) -> bool {
        self.supports_prefer && self.mode.prefer_busy_poll()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn off_has_zero_us() {
        assert_eq!(BusyPollMode::Off.us(), 0);
        assert!(!BusyPollMode::Off.prefer_busy_poll());
    }

    #[test]
    fn latency_prefer_enabled() {
        let m = BusyPollMode::Latency { us: 50 };
        assert_eq!(m.us(), 50);
        assert!(m.prefer_busy_poll());
    }

    #[test]
    fn throughput_prefer_enabled() {
        let m = BusyPollMode::Throughput { us: 200 };
        assert_eq!(m.us(), 200);
        assert!(m.prefer_busy_poll());
    }

    #[test]
    fn static_mode_not_preferred() {
        let m = BusyPollMode::Static { us: 50 };
        assert_eq!(m.us(), 50);
        assert!(!m.prefer_busy_poll());
    }

    #[test]
    fn policy_active_matches_mode() {
        let p = BusyPollPolicy::new(BusyPollMode::Off, true);
        assert!(!p.active());
        let p = BusyPollPolicy::new(BusyPollMode::Latency { us: 50 }, true);
        assert!(p.active());
        assert!(p.should_prefer());
    }

    #[test]
    fn policy_respects_capability() {
        let p = BusyPollPolicy::new(BusyPollMode::Latency { us: 50 }, false);
        assert!(p.active());
        assert!(!p.should_prefer());
    }
}
