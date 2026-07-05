//! Backpressure gate (Layer 6).
//!
//! [`BackpressureGate`] provides graded admission control via dual
//! watermarks (low / high) and hysteresis. Producers receive a
//! synchronous [`PressureLevel`] signal so they can shed load, retry,
//! or park.

use std::sync::atomic::{AtomicU8, Ordering};

use crate::error::SendQueueError;

/// Graded pressure level reported by the gate.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum PressureLevel {
    /// Below low watermark — no action needed.
    #[default]
    Normal = 0,
    /// Between low and high — begin shedding non-critical traffic.
    Elevated = 1,
    /// At or above high watermark — reject new submissions.
    Saturated = 2,
}

impl PressureLevel {
    /// Converts the level to its numeric representation.
    #[inline]
    #[must_use]
    pub fn as_u8(self) -> u8 {
        self as u8
    }

    /// Converts a numeric value back to a level (clamped).
    #[inline]
    #[must_use]
    pub fn from_u8(v: u8) -> Self {
        match v {
            0 => Self::Normal,
            1 => Self::Elevated,
            _ => Self::Saturated,
        }
    }
}

/// Notified when the queue's pressure level transitions.
///
/// Implementations must be non-blocking; they are called synchronously
/// on the producer thread.
pub trait PressureObserver: Send + Sync {
    /// Called whenever the pressure level changes.
    fn on_pressure(&self, level: PressureLevel, depth: usize);
}

/// Admission gate with dual watermarks and hysteresis.
///
/// ```text
/// 0          low_watermark        high_watermark         capacity
/// ├──────────────┬────────────────────┬──────────────────┤
/// │   NORMAL     │     ELEVATED        │     SATURATED    │
/// │ (accept all) │  (accept, signal)   │  (reject: Err)   │
/// ```
///
/// Hysteresis prevents flapping: the `Elevated → Normal` transition
/// only fires when depth drops below `low_watermark - hysteresis`.
pub struct BackpressureGate {
    low: usize,
    high: usize,
    hysteresis: usize,
    capacity: usize,
    /// Current level encoded as `PressureLevel`'s discriminant.
    current: AtomicU8,
}

impl BackpressureGate {
    /// Creates a gate with the given watermarks and hysteresis band.
    #[must_use]
    pub fn new(low: usize, high: usize, hysteresis: usize, capacity: usize) -> Self {
        Self {
            low,
            high,
            hysteresis,
            capacity,
            current: AtomicU8::new(PressureLevel::Normal.as_u8()),
        }
    }

    /// Checks whether a push at the given `depth` should be admitted.
    ///
    /// Returns `Err(QueueFull)` when depth would reach `high` or beyond.
    /// This is a pre-push check — call with the depth **before** the new
    /// packet is counted.
    pub fn admit(&self, depth: usize) -> Result<(), SendQueueError> {
        if depth >= self.high {
            return Err(SendQueueError::QueueFull {
                capacity: self.capacity,
            });
        }
        Ok(())
    }

    /// Updates the gate's pressure level after a depth change and returns
    /// the (possibly new) level.
    ///
    /// This is the post-push / post-pop notification. The method detects
    /// level transitions and returns the new level so callers can emit
    /// observer notifications.
    pub fn post_change(&self, depth: usize) -> PressureLevel {
        let new_level = self.compute_level(depth);
        let old_raw = self.current.swap(new_level.as_u8(), Ordering::AcqRel);
        if old_raw != new_level.as_u8() {
            return new_level;
        }
        // No transition — return the same level (idempotent).
        new_level
    }

    /// Returns the current pressure level (relaxed load).
    #[must_use]
    pub fn level(&self) -> PressureLevel {
        PressureLevel::from_u8(self.current.load(Ordering::Relaxed))
    }

    /// Computes the level for `depth`, applying hysteresis.
    fn compute_level(&self, depth: usize) -> PressureLevel {
        let prev = PressureLevel::from_u8(self.current.load(Ordering::Relaxed));
        match prev {
            PressureLevel::Normal => {
                if depth >= self.high {
                    PressureLevel::Saturated
                } else if depth >= self.low {
                    PressureLevel::Elevated
                } else {
                    PressureLevel::Normal
                }
            }
            PressureLevel::Elevated => {
                if depth >= self.high {
                    PressureLevel::Saturated
                } else if depth < self.low.saturating_sub(self.hysteresis) {
                    PressureLevel::Normal
                } else {
                    PressureLevel::Elevated
                }
            }
            PressureLevel::Saturated => {
                if depth < self.low.saturating_sub(self.hysteresis) {
                    PressureLevel::Normal
                } else if depth < self.high {
                    PressureLevel::Elevated
                } else {
                    PressureLevel::Saturated
                }
            }
        }
    }
}

impl std::fmt::Debug for BackpressureGate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BackpressureGate")
            .field("low", &self.low)
            .field("high", &self.high)
            .field("hysteresis", &self.hysteresis)
            .field("current", &self.level())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn admit_rejects_at_high_watermark() {
        let gate = BackpressureGate::new(75, 90, 5, 100);
        assert!(gate.admit(89).is_ok());
        assert!(gate.admit(90).is_err());
    }

    #[test]
    fn normal_to_elevated_at_low_watermark() {
        let gate = BackpressureGate::new(75, 90, 5, 100);
        assert_eq!(gate.level(), PressureLevel::Normal);
        gate.post_change(74);
        assert_eq!(gate.level(), PressureLevel::Normal);
        gate.post_change(75);
        assert_eq!(gate.level(), PressureLevel::Elevated);
    }

    #[test]
    fn elevated_to_saturated_at_high_watermark() {
        let gate = BackpressureGate::new(75, 90, 5, 100);
        gate.post_change(75); // → Elevated
        gate.post_change(89); // still Elevated
        assert_eq!(gate.level(), PressureLevel::Elevated);
        gate.post_change(90); // → Saturated
        assert_eq!(gate.level(), PressureLevel::Saturated);
    }

    #[test]
    fn hysteresis_prevents_flapping() {
        let gate = BackpressureGate::new(75, 90, 5, 100);
        gate.post_change(75); // → Elevated
        assert_eq!(gate.level(), PressureLevel::Elevated);
        // Without hysteresis, 74 would return to Normal.
        gate.post_change(74);
        assert_eq!(gate.level(), PressureLevel::Elevated); // still Elevated
        // Below low - hysteresis (= 70), we drop to Normal.
        gate.post_change(69);
        assert_eq!(gate.level(), PressureLevel::Normal);
    }

    #[test]
    fn saturated_to_elevated_on_drain() {
        let gate = BackpressureGate::new(75, 90, 5, 100);
        gate.post_change(90); // → Saturated
        gate.post_change(85); // below high, but above low → Elevated
        assert_eq!(gate.level(), PressureLevel::Elevated);
    }

    #[test]
    fn pressure_level_round_trip() {
        assert_eq!(PressureLevel::from_u8(0), PressureLevel::Normal);
        assert_eq!(PressureLevel::from_u8(1), PressureLevel::Elevated);
        assert_eq!(PressureLevel::from_u8(2), PressureLevel::Saturated);
        assert_eq!(PressureLevel::from_u8(99), PressureLevel::Saturated);
    }
}
