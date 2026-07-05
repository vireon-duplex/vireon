//! [`BudgetConfig`] — the configuration source for
//! [`TickBudget`](crate::tick::TickBudget).
//!
//! `BudgetConfig` is what the operator (or a
//! [`ReactorPreset`](crate::config::ReactorPreset)) sets at
//! build time. It is converted to a [`TickBudget`](crate::tick::TickBudget)
//! once at reactor start and may be shrunk under backpressure.
//!
//! See `DESIGN.md` §3 (event loop ordering) and §15 (performance
//! budgeting).

use crate::tick::TickBudget;

/// Default per-tick CQE drain cap.
pub const DEFAULT_MAX_CQES: u32 = 256;
/// Default per-tick task execution cap.
pub const DEFAULT_MAX_TASKS: u32 = 64;
/// Default per-tick send-queue flush cap.
pub const DEFAULT_MAX_SEND_FLUSH: u32 = 128;
/// Default tick deadline (200 μs in nanoseconds).
pub const DEFAULT_DEADLINE_NS: u64 = 200_000;

/// Minimum sane per-tick CQE drain cap.
pub const MIN_MAX_CQES: u32 = 16;
/// Maximum per-tick CQE drain cap (must fit in the SQ).
pub const MAX_MAX_CQES: u32 = 4096;

/// Minimum sane per-tick task cap.
pub const MIN_MAX_TASKS: u32 = 1;
/// Maximum per-tick task cap.
pub const MAX_MAX_TASKS: u32 = 1024;

/// Minimum sane per-tick send-queue flush cap.
pub const MIN_MAX_SEND_FLUSH: u32 = 1;
/// Maximum per-tick send-queue flush cap.
pub const MAX_MAX_SEND_FLUSH: u32 = 4096;

/// Minimum tick deadline (10 μs). Below this, the loop busy-spins.
pub const MIN_DEADLINE_NS: u64 = 10_000;
/// Maximum tick deadline (10 ms). Above this, latency suffers.
pub const MAX_DEADLINE_NS: u64 = 10_000_000;

/// Configurable per-tick budget. Stored on
/// [`ReactorConfig`](crate::config::ReactorConfig); converted to a
/// [`TickBudget`] at reactor start.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BudgetConfig {
    /// Max CQEs drained per tick.
    pub max_cqes: u32,
    /// Max tasks executed per tick.
    pub max_tasks: u32,
    /// Max packets flushed per tick.
    pub max_send_flush: u32,
    /// Soft tick wall-clock deadline (ns).
    pub deadline_ns: u64,
}

impl Default for BudgetConfig {
    fn default() -> Self {
        Self {
            max_cqes: DEFAULT_MAX_CQES,
            max_tasks: DEFAULT_MAX_TASKS,
            max_send_flush: DEFAULT_MAX_SEND_FLUSH,
            deadline_ns: DEFAULT_DEADLINE_NS,
        }
    }
}

impl BudgetConfig {
    /// Validate the budget caps against their allowed ranges.
    ///
    /// # Errors
    /// Returns [`ReactorError::InvalidConfig`](crate::error::ReactorError::InvalidConfig)
    /// if any field is out of range.
    pub fn validate(&self) -> Result<(), crate::error::ReactorError> {
        if !(MIN_MAX_CQES..=MAX_MAX_CQES).contains(&self.max_cqes) {
            return Err(crate::error::ReactorError::InvalidConfig(format!(
                "max_cqes {} out of range [{}, {}]",
                self.max_cqes, MIN_MAX_CQES, MAX_MAX_CQES
            )));
        }
        if !(MIN_MAX_TASKS..=MAX_MAX_TASKS).contains(&self.max_tasks) {
            return Err(crate::error::ReactorError::InvalidConfig(format!(
                "max_tasks {} out of range [{}, {}]",
                self.max_tasks, MIN_MAX_TASKS, MAX_MAX_TASKS
            )));
        }
        if !(MIN_MAX_SEND_FLUSH..=MAX_MAX_SEND_FLUSH).contains(&self.max_send_flush) {
            return Err(crate::error::ReactorError::InvalidConfig(format!(
                "max_send_flush {} out of range [{}, {}]",
                self.max_send_flush, MIN_MAX_SEND_FLUSH, MAX_MAX_SEND_FLUSH
            )));
        }
        if !(MIN_DEADLINE_NS..=MAX_DEADLINE_NS).contains(&self.deadline_ns) {
            return Err(crate::error::ReactorError::InvalidConfig(format!(
                "deadline_ns {} out of range [{}, {}]",
                self.deadline_ns, MIN_DEADLINE_NS, MAX_DEADLINE_NS
            )));
        }
        Ok(())
    }

    /// Materialise a runtime [`TickBudget`].
    #[must_use]
    pub const fn to_tick_budget(&self) -> TickBudget {
        TickBudget::new(
            self.max_cqes,
            self.max_tasks,
            self.max_send_flush,
            self.deadline_ns,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ReactorError;

    #[test]
    fn defaults_match_constants() {
        let d = BudgetConfig::default();
        assert_eq!(d.max_cqes, DEFAULT_MAX_CQES);
        assert_eq!(d.max_tasks, DEFAULT_MAX_TASKS);
        assert_eq!(d.max_send_flush, DEFAULT_MAX_SEND_FLUSH);
        assert_eq!(d.deadline_ns, DEFAULT_DEADLINE_NS);
    }

    #[test]
    fn validate_accepts_default() -> Result<(), ReactorError> {
        BudgetConfig::default().validate()?;
        Ok(())
    }

    #[test]
    fn validate_rejects_too_small_cqes() {
        let cfg = BudgetConfig {
            max_cqes: MIN_MAX_CQES / 2,
            ..BudgetConfig::default()
        };
        assert!(matches!(
            cfg.validate(),
            Err(ReactorError::InvalidConfig(_))
        ));
    }

    #[test]
    fn validate_rejects_huge_deadline() {
        let cfg = BudgetConfig {
            deadline_ns: MAX_DEADLINE_NS * 2,
            ..BudgetConfig::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn to_tick_budget_roundtrip() {
        let cfg = BudgetConfig::default();
        let tb = cfg.to_tick_budget();
        assert_eq!(tb.max_cqes, cfg.max_cqes);
        assert_eq!(tb.max_tasks, cfg.max_tasks);
        assert_eq!(tb.max_send_flush, cfg.max_send_flush);
        assert_eq!(tb.deadline_ns, cfg.deadline_ns);
    }
}
