//! [`SchedulingPolicy`] — the selectable algorithms the scheduler
//! supports.
//!
//! See `DESIGN.md` Part 2 (scheduling model comparison) and Part 4
//! (RFC 9218).

/// The active scheduling algorithm. Picked at construction time via
/// [`crate::config::SchedulerConfig`] or a [`crate::config::Preset`].
///
/// `#[non_exhaustive]` allows new policies in future phases.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum SchedulingPolicy {
    /// First-In-First-Out. Simplest; no fairness guarantee.
    Fifo,
    /// Round-Robin across ready streams regardless of priority.
    RoundRobin,
    /// Weighted Round-Robin — each stream gets a share proportional
    /// to its weight (derived from RFC 9218 urgency).
    WeightedRoundRobin,
    /// Deficit Round-Robin — like WRR but with byte-count deficits
    /// for finer fairness.
    DeficitRoundRobin,
    /// Weighted Fair Queuing — generalized processor sharing.
    WeightedFair,
    /// Strict Priority — urgency 0 streams always win; lower urgency
    /// can starve. Use with aging.
    StrictPriority,
    /// Hybrid (default) — strict priority across urgency bands, RR
    /// within each band, fallback WFQ when one band dominates.
    #[default]
    Hybrid,
}

impl SchedulingPolicy {
    /// Whether the policy can preempt a running stream (i.e. a
    /// higher-priority arrival interrupts the current selection).
    /// Strict-priority and hybrid can preempt; the round-robin
    /// variants cannot.
    #[must_use]
    pub const fn is_preemptive(self) -> bool {
        matches!(self, Self::StrictPriority | Self::Hybrid)
    }

    /// Whether the policy provides a fairness guarantee (bounded
    /// starvation). Strict priority does not.
    #[must_use]
    pub const fn is_fair(self) -> bool {
        !matches!(self, Self::Fifo | Self::StrictPriority)
    }

    /// Human-readable name for logging / metrics labels.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Fifo => "fifo",
            Self::RoundRobin => "round_robin",
            Self::WeightedRoundRobin => "weighted_round_robin",
            Self::DeficitRoundRobin => "deficit_round_robin",
            Self::WeightedFair => "weighted_fair",
            Self::StrictPriority => "strict_priority",
            Self::Hybrid => "hybrid",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_hybrid() {
        assert_eq!(SchedulingPolicy::default(), SchedulingPolicy::Hybrid);
    }

    #[test]
    fn preemptive_policies() {
        assert!(SchedulingPolicy::StrictPriority.is_preemptive());
        assert!(SchedulingPolicy::Hybrid.is_preemptive());
        assert!(!SchedulingPolicy::Fifo.is_preemptive());
        assert!(!SchedulingPolicy::RoundRobin.is_preemptive());
    }

    #[test]
    fn fair_policies() {
        assert!(SchedulingPolicy::RoundRobin.is_fair());
        assert!(SchedulingPolicy::WeightedFair.is_fair());
        assert!(!SchedulingPolicy::Fifo.is_fair());
        assert!(!SchedulingPolicy::StrictPriority.is_fair());
    }

    #[test]
    fn as_str_covers_all_variants() {
        for p in [
            SchedulingPolicy::Fifo,
            SchedulingPolicy::RoundRobin,
            SchedulingPolicy::WeightedRoundRobin,
            SchedulingPolicy::DeficitRoundRobin,
            SchedulingPolicy::WeightedFair,
            SchedulingPolicy::StrictPriority,
            SchedulingPolicy::Hybrid,
        ] {
            assert!(!p.as_str().is_empty());
        }
    }
}
