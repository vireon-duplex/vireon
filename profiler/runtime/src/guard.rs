//! Timing guard — the bridge between the `#[profile]` macro and the
//! runtime collector.
//!
//! When the `enabled` feature is active, [`Guard`] records a
//! [`std::time::Instant`] on construction and the elapsed duration on
//! drop. When the feature is off, [`Guard`] compiles to an empty struct
//! with an empty `Drop` — zero cost.
//!
//! # Async note
//!
//! For `async fn`, the guard is placed at the body's entry point. It
//! lives in the state machine's saved frame, persisting across `.await`
//! points. This measures wall-clock time from first poll to completion.

#[cfg(feature = "enabled")]
use std::time::Instant;

// ── Active implementation (feature = "enabled") ──────────────────

#[cfg(feature = "enabled")]
mod active {
    use super::*;
    use crate::collector;
    use crate::context;

    /// RAII timing guard.
    ///
    /// Created by [`Guard::enter`] at function entry, dropped at exit.
    /// On drop, the elapsed time is recorded in the thread-local
    /// collector.
    pub struct Guard {
        name: &'static str,
        start: Instant,
        /// Whether this guard pushed to the call stack (for balanced pop).
        active: bool,
    }

    impl Guard {
        /// Creates a guard and starts timing.
        ///
        /// Called by the `#[profile]` macro expansion:
        /// ```ignore
        /// let _guard = ::profiler::Guard::enter("function_name");
        /// ```
        #[inline(always)]
        #[must_use]
        pub fn enter(name: &'static str) -> Self {
            let enabled = collector::is_enabled();
            if enabled {
                let parent = context::push(name);
                if let Some(p) = parent {
                    context::record_edge(p, name);
                }
            }
            Self {
                name,
                start: Instant::now(),
                active: enabled,
            }
        }
    }

    impl Drop for Guard {
        #[inline]
        fn drop(&mut self) {
            if !self.active {
                return;
            }
            // Skip recording during panic unwinding (we still pop the
            // call stack to keep it balanced).
            if !std::thread::panicking() {
                let elapsed = self.start.elapsed().as_nanos() as u64;
                collector::record(self.name, elapsed);
            }
            context::pop();
        }
    }

    impl std::fmt::Debug for Guard {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("Guard")
                .field("name", &self.name)
                .field("active", &self.active)
                .finish()
        }
    }
}

// ── Zero-cost no-op (feature not "enabled") ───────────────────────

#[cfg(not(feature = "enabled"))]
mod noop {
    /// Zero-cost timing guard (no-op).
    ///
    /// When the `enabled` feature is off, the guard is an empty struct
    /// with an empty `Drop`. The compiler eliminates it entirely.
    #[derive(Default)]
    pub struct Guard;

    impl Guard {
        /// No-op entry point.
        #[inline(always)]
        #[must_use]
        pub fn enter(_name: &'static str) -> Self {
            Guard
        }
    }

    impl Drop for Guard {
        #[inline(always)]
        fn drop(&mut self) {}
    }

    impl std::fmt::Debug for Guard {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("Guard").finish_non_exhaustive()
        }
    }
}

// ── Re-export ─────────────────────────────────────────────────────

#[cfg(feature = "enabled")]
pub use active::Guard;

#[cfg(not(feature = "enabled"))]
pub use noop::Guard;

// ── Tests ─────────────────────────────────────────────────────────

#[cfg(all(test, feature = "enabled"))]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;
    use crate::collector;
    use crate::registry;

    fn reset() {
        registry::reset_all();
        collector::set_enabled(true);
        collector::set_sampling(1);
    }

    #[test]
    fn guard_records_timing() {
        reset();
        {
            let _g = Guard::enter("guard_records_timing");
            std::thread::sleep(std::time::Duration::from_micros(10));
        }
        collector::flush_local();
        let snaps = registry::snapshot_all();
        let snap = snaps
            .iter()
            .find(|s| s.name == "guard_records_timing");
        assert!(snap.is_some(), "snapshot should exist after flush");
        let snap = snap.unwrap();
        assert!(snap.calls >= 1);
        assert!(snap.min_ns > 0, "should have recorded some time");
    }

    #[test]
    fn guard_disabled_skips_recording() {
        reset();
        collector::set_enabled(false);
        {
            let _g = Guard::enter("guard_disabled_test");
        }
        collector::flush_local();
        // With profiling disabled, no data should be recorded.
        let snaps = registry::snapshot_all();
        let snap = snaps.iter().find(|s| s.name == "guard_disabled_test");
        // It may have been registered but with 0 calls.
        if let Some(s) = snap {
            assert_eq!(s.calls, 0, "should not record when disabled");
        }
        collector::set_enabled(true);
    }

    #[test]
    fn guard_balanced_push_pop() {
        reset();
        crate::context::reset_thread();
        assert_eq!(crate::context::depth(), 0);
        {
            let _g1 = Guard::enter("outer_fn");
            assert_eq!(crate::context::depth(), 1);
            {
                let _g2 = Guard::enter("inner_fn");
                assert_eq!(crate::context::depth(), 2);
            }
            assert_eq!(crate::context::depth(), 1);
        }
        assert_eq!(crate::context::depth(), 0);
    }

    #[test]
    fn guard_survives_panic() {
        reset();
        crate::context::reset_thread();
        let result = std::panic::catch_unwind(|| {
            let _g = Guard::enter("panicking_fn");
            panic!("test panic");
        });
        assert!(result.is_err());
        // Call stack should be balanced after unwinding.
        assert_eq!(crate::context::depth(), 0);
    }
}
