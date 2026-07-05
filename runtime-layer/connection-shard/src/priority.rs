//! [`ConnectionPriority`] — scheduling priority class (Layer 6).
//!
//! Priority is assigned at connection creation and influences scheduling
//! decisions in the task-queue and send-queue. The reactor consults
//! priority when deciding which connections to serve first under load.

/// Connection priority class.
///
/// Lower numeric value = lower priority. `Normal` is the default for
/// most connections. `RealTime` should be reserved for latency-critical
/// control streams; `Bulk` for background transfers.
#[repr(u8)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ConnectionPriority {
    /// Background / best-effort traffic. Last to be served.
    Bulk = 0,
    /// Below-normal priority.
    Low = 1,
    /// Standard priority — the common case.
    #[default]
    Normal = 2,
    /// Elevated priority for important streams.
    High = 3,
    /// Latency-critical control traffic. Served before all others.
    RealTime = 4,
}

impl ConnectionPriority {
    /// Numeric weight for weighted-fair scheduling (higher = more CPU).
    #[inline]
    #[must_use]
    pub const fn weight(self) -> u32 {
        match self {
            Self::Bulk => 1,
            Self::Low => 2,
            Self::Normal => 4,
            Self::High => 8,
            Self::RealTime => 16,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_normal() {
        assert_eq!(ConnectionPriority::default(), ConnectionPriority::Normal);
    }

    #[test]
    fn ordering() {
        assert!(ConnectionPriority::Bulk < ConnectionPriority::Low);
        assert!(ConnectionPriority::Low < ConnectionPriority::Normal);
        assert!(ConnectionPriority::Normal < ConnectionPriority::High);
        assert!(ConnectionPriority::High < ConnectionPriority::RealTime);
    }

    #[test]
    fn weights_are_monotonic() {
        let priorities = [
            ConnectionPriority::Bulk,
            ConnectionPriority::Low,
            ConnectionPriority::Normal,
            ConnectionPriority::High,
            ConnectionPriority::RealTime,
        ];
        for w in priorities.windows(2) {
            assert!(w[0].weight() < w[1].weight(), "{:?} weight", w);
        }
    }
}
