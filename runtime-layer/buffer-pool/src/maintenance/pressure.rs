//! Memory-pressure detection (Layer 16).
//!
//! [`resident_memory`] reads the current RSS; [`classify_pressure`] maps a
//! usage ratio to a [`PressureLevel`] that the shrinker and backpressure gate
//! consume.

use crate::buffer::policy::{PressureLevel, ShrinkPolicy};

/// Returns the current resident set size in bytes, if available.
///
/// On Linux this reads `VmRSS` from `/proc/self/status`. On other platforms
/// the value is `None` and the caller should fall back to arena/cache bytes.
pub fn resident_memory() -> Option<usize> {
    #[cfg(target_os = "linux")]
    {
        read_proc_status_rss()
    }
    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}

/// Classifies `used / max` into a [`PressureLevel`].
///
/// | Ratio | Level |
/// |-------|-------|
/// | < 50 % | [`Low`] |
/// | 50–74 % | [`Moderate`] |
/// | 75–89 % | [`High`] |
/// | ≥ 90 % | [`Critical`] |
///
/// [`Low`]: PressureLevel::Low
/// [`Moderate`]: PressureLevel::Moderate
/// [`High`]: PressureLevel::High
/// [`Critical`]: PressureLevel::Critical
pub fn classify_pressure(used: usize, max: usize) -> PressureLevel {
    if max == 0 {
        return PressureLevel::Critical;
    }
    let pct = (used * 100) / max;
    match pct {
        0..=49 => PressureLevel::Low,
        50..=74 => PressureLevel::Moderate,
        75..=89 => PressureLevel::High,
        _ => PressureLevel::Critical,
    }
}

/// Whether the shrinker should run given the current pressure and policy.
pub fn should_shrink(level: PressureLevel, policy: ShrinkPolicy) -> bool {
    match policy {
        ShrinkPolicy::Never => false,
        ShrinkPolicy::OnPressure => level >= PressureLevel::Moderate,
        ShrinkPolicy::Aggressive => true,
    }
}

#[cfg(target_os = "linux")]
fn read_proc_status_rss() -> Option<usize> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            let kb_str = rest.split_whitespace().next()?;
            let kb: usize = kb_str.parse().ok()?;
            return Some(kb * 1024);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_zero_budget_is_critical() {
        assert_eq!(classify_pressure(0, 0), PressureLevel::Critical);
    }

    #[test]
    fn classify_boundaries() {
        assert_eq!(classify_pressure(49, 100), PressureLevel::Low);
        assert_eq!(classify_pressure(50, 100), PressureLevel::Moderate);
        assert_eq!(classify_pressure(74, 100), PressureLevel::Moderate);
        assert_eq!(classify_pressure(75, 100), PressureLevel::High);
        assert_eq!(classify_pressure(89, 100), PressureLevel::High);
        assert_eq!(classify_pressure(90, 100), PressureLevel::Critical);
        assert_eq!(classify_pressure(100, 100), PressureLevel::Critical);
    }

    #[test]
    fn should_shrink_respects_policy() {
        assert!(!should_shrink(PressureLevel::High, ShrinkPolicy::Never));
        assert!(!should_shrink(PressureLevel::Low, ShrinkPolicy::OnPressure));
        assert!(should_shrink(
            PressureLevel::Moderate,
            ShrinkPolicy::OnPressure
        ));
        assert!(should_shrink(PressureLevel::Low, ShrinkPolicy::Aggressive));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn resident_memory_returns_nonzero() {
        // The test process certainly uses some RSS.
        let rss = resident_memory();
        assert!(rss.is_some(), "expected /proc/self/status to be readable");
        assert!(rss.unwrap() > 0);
    }
}
