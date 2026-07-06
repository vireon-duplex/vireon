//! [`StreamPriority`] + [`PriorityUpdate`] — RFC 9218 extensible
//! prioritization scheme.
//!
//! RFC 9218 defines two per-stream parameters:
//!
//! - **urgency**: `u8` in `0..=7` (lower = higher priority). Default `3`.
//! - **incremental**: `bool` (whether the stream benefits from
//!   partial delivery vs. waiting for full chunks).
//!
//! The Stream Manager owns these values; the Stream Scheduler
//! reads them when selecting the next stream to send.
//!
//! See `DESIGN.md` Part 2 (stream object design) and the
//! companion scheduler design (RFC 9218).

/// Minimum urgency value (highest priority). RFC 9218 §3.
pub const URGENCY_MIN: u8 = 0;
/// Maximum urgency value (lowest priority). RFC 9218 §3.
pub const URGENCY_MAX: u8 = 7;
/// Default urgency for new streams. RFC 9218 §3.
pub const URGENCY_DEFAULT: u8 = 3;

/// RFC 9218 per-stream priority.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct StreamPriority {
    /// 0 = highest, 7 = lowest. Default 3.
    pub urgency: u8,
    /// Whether the stream benefits from partial / round-robin
    /// delivery. Default `false`.
    pub incremental: bool,
}

impl Default for StreamPriority {
    fn default() -> Self {
        Self {
            urgency: URGENCY_DEFAULT,
            incremental: false,
        }
    }
}

impl StreamPriority {
    /// Construct a priority with the given urgency; incremental
    /// defaults to `false`.
    #[must_use]
    pub const fn with_urgency(urgency: u8) -> Self {
        Self {
            urgency,
            incremental: false,
        }
    }

    /// Construct an incremental priority with the given urgency.
    #[must_use]
    pub const fn incremental(urgency: u8) -> Self {
        Self {
            urgency,
            incremental: true,
        }
    }

    /// Builder-style urgency setter.
    #[must_use]
    pub const fn urgency(mut self, urgency: u8) -> Self {
        self.urgency = urgency;
        self
    }

    /// Builder-style incremental setter.
    #[must_use]
    pub const fn incremental_flag(mut self, incremental: bool) -> Self {
        self.incremental = incremental;
        self
    }

    /// Validate the priority fields.
    ///
    /// # Errors
    /// Returns [`StreamManagerError::InvalidConfig`](crate::error::StreamManagerError::InvalidConfig)
    /// if `urgency` is outside `0..=7`.
    pub fn validate(&self) -> Result<(), crate::error::StreamManagerError> {
        if self.urgency > URGENCY_MAX {
            return Err(crate::error::StreamManagerError::InvalidConfig(format!(
                "urgency {} exceeds max {}",
                self.urgency, URGENCY_MAX
            )));
        }
        Ok(())
    }

    /// Whether this priority is higher (lower numeric urgency) than
    /// `other`. Ties broken by `incremental` (non-incremental wins).
    #[must_use]
    pub const fn is_higher_than(self, other: Self) -> bool {
        if self.urgency != other.urgency {
            self.urgency < other.urgency
        } else {
            !self.incremental && other.incremental
        }
    }
}

/// An update to a stream's priority. The Stream Scheduler reads
/// these from the application / driver and applies them between
/// ticks.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PriorityUpdate {
    /// Replace the entire priority.
    Set(StreamPriority),
    /// Change urgency by a signed delta; clamped to `0..=7`.
    AdjustUrgency(i8),
    /// Increase urgency by `delta` (clamped to 0).
    IncrementUrgency(u8),
    /// Decrease urgency by `delta` (clamped to 7).
    DecrementUrgency(u8),
    /// Toggle the `incremental` flag.
    ToggleIncremental,
    /// Reset to default (`urgency=3, incremental=false`).
    Reset,
}

impl PriorityUpdate {
    /// Apply this update to a priority, returning the new value.
    #[must_use]
    pub const fn apply(self, mut p: StreamPriority) -> StreamPriority {
        match self {
            Self::Set(new) => new,
            Self::AdjustUrgency(delta) => {
                let new_val = if delta >= 0 {
                    p.urgency.saturating_add(delta as u8)
                } else {
                    p.urgency.saturating_sub((-delta) as u8)
                };
                p.urgency = if new_val > URGENCY_MAX {
                    URGENCY_MAX
                } else {
                    new_val
                };
                p
            }
            Self::IncrementUrgency(delta) => {
                let new_val = p.urgency.saturating_sub(delta);
                p.urgency = new_val;
                p
            }
            Self::DecrementUrgency(delta) => {
                let new_val = p.urgency.saturating_add(delta);
                p.urgency = if new_val > URGENCY_MAX {
                    URGENCY_MAX
                } else {
                    new_val
                };
                p
            }
            Self::ToggleIncremental => {
                p.incremental = !p.incremental;
                p
            }
            Self::Reset => StreamPriority {
                urgency: URGENCY_DEFAULT,
                incremental: false,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_urgency_3_non_incremental() {
        let p = StreamPriority::default();
        assert_eq!(p.urgency, 3);
        assert!(!p.incremental);
    }

    #[test]
    fn validate_accepts_default() {
        StreamPriority::default().validate().expect("default ok");
    }

    #[test]
    fn validate_rejects_huge_urgency() {
        let p = StreamPriority::with_urgency(8);
        assert!(p.validate().is_err());
    }

    #[test]
    fn urgency_boundary_values() {
        StreamPriority::with_urgency(0).validate().expect("min ok");
        StreamPriority::with_urgency(7).validate().expect("max ok");
    }

    #[test]
    fn higher_than_compares_correctly() {
        let urgent = StreamPriority::with_urgency(0);
        let normal = StreamPriority::with_urgency(3);
        assert!(urgent.is_higher_than(normal));
        assert!(!normal.is_higher_than(urgent));
    }

    #[test]
    fn incremental_breaks_ties() {
        let ni = StreamPriority::with_urgency(3);
        let inc = StreamPriority::incremental(3);
        assert!(ni.is_higher_than(inc));
        assert!(!inc.is_higher_than(ni));
    }

    #[test]
    fn update_set_replaces() {
        let p = StreamPriority::default();
        let new = StreamPriority::with_urgency(0);
        assert_eq!(PriorityUpdate::Set(new).apply(p), new);
    }

    #[test]
    fn update_increment_clamps_at_zero() {
        let p = StreamPriority::with_urgency(2);
        let after = PriorityUpdate::IncrementUrgency(10).apply(p);
        assert_eq!(after.urgency, 0);
    }

    #[test]
    fn update_decrement_clamps_at_seven() {
        let p = StreamPriority::with_urgency(5);
        let after = PriorityUpdate::DecrementUrgency(10).apply(p);
        assert_eq!(after.urgency, 7);
    }

    #[test]
    fn update_adjust_signed() {
        let p = StreamPriority::with_urgency(3);
        assert_eq!(PriorityUpdate::AdjustUrgency(-2).apply(p).urgency, 1);
        assert_eq!(PriorityUpdate::AdjustUrgency(2).apply(p).urgency, 5);
    }

    #[test]
    fn update_toggle_incremental() {
        let p = StreamPriority::default();
        let after = PriorityUpdate::ToggleIncremental.apply(p);
        assert!(after.incremental);
    }

    #[test]
    fn update_reset() {
        let p = StreamPriority::incremental(0);
        assert_eq!(PriorityUpdate::Reset.apply(p), StreamPriority::default());
    }
}
