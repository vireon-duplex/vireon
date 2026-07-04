//! Kernel-side timer wheel built on `IORING_OP_TIMEOUT`.
//!
//! Distinct from `runtime-layer/timer-wheel` (which is a userspace hierarchical
//! wheel): this module issues io_uring `Timeout` SQEs and tracks them so
//! the reactor can `cancel_timeout` an in-flight timer by `user_data`.
//!
//! # Lifecycle
//!
//! 1. [`TimerWheel::add_timeout`] builds a `Timeout` opcode with the
//!    caller's `user_data`, pushes it to the SQ, and records the
//!    `(user_data, deadline)` pair in the active map.
//! 2. When the kernel fires (or cancels) the timer, a CQE with that
//!    `user_data` lands in the reactor's CQ. The reactor removes it
//!    from the active map via [`TimerWheel::retire`].
//! 3. [`TimerWheel::cancel_timeout`] enqueues a `TimeoutRemove` SQE; the
//!    matching CQE arrives later with `res = -libc::ECANCELED`.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use io_uring::IoUring;
use io_uring::opcode::{AsyncCancel, Timeout, TimeoutRemove};
use io_uring::types::{TimeoutFlags, Timespec};

use crate::error::IoError;

/// Bookkeeping for one in-flight kernel timeout.
#[derive(Debug, Clone, Copy)]
#[allow(dead_code)]
struct ActiveTimer {
    /// Echoed back by the kernel in the CQE.
    user_data: u64,
    /// When the timer was scheduled to fire (best-effort wall-clock).
    deadline: Instant,
}

/// Kernel-side timer wheel.
pub struct TimerWheel {
    active: HashMap<u64, ActiveTimer>,
}

impl TimerWheel {
    /// Empty wheel.
    #[must_use]
    pub fn new() -> Self {
        Self {
            active: HashMap::new(),
        }
    }

    /// Number of outstanding timers (not yet fired or cancelled).
    #[must_use]
    pub fn active_count(&self) -> usize {
        self.active.len()
    }

    /// Schedules a one-shot timeout.
    ///
    /// `target_id` becomes the CQE's `user_data`, so the reactor can route
    /// the fire event back to the originating connection/stream.
    pub fn add_timeout(
        &mut self,
        ring: &mut IoUring,
        target_id: u64,
        duration: Duration,
    ) -> Result<(), IoError> {
        let ts = Box::leak(Box::new(
            Timespec::new()
                .sec(duration.as_secs())
                .nsec(duration.subsec_nanos()),
        ));
        let entry = Timeout::new(ts).build().user_data(target_id);
        push_entry(ring, entry)?;
        self.active.insert(
            target_id,
            ActiveTimer {
                user_data: target_id,
                deadline: Instant::now() + duration,
            },
        );
        Ok(())
    }

    /// Schedules a periodic tick that fires every `period`.
    ///
    /// Uses `IORING_TIMEOUT_MULTISHOT` (kernel ≥ 6.2). Each expiry posts
    /// a CQE with `IORING_CQE_F_MORE` set until cancelled.
    pub fn add_periodic_tick(
        &mut self,
        ring: &mut IoUring,
        user_data: u64,
        period: Duration,
    ) -> Result<(), IoError> {
        let ts = Box::leak(Box::new(
            Timespec::new()
                .sec(period.as_secs())
                .nsec(period.subsec_nanos()),
        ));
        let entry = Timeout::new(ts)
            .flags(TimeoutFlags::MULTISHOT)
            .build()
            .user_data(user_data);
        push_entry(ring, entry)?;
        self.active.insert(
            user_data,
            ActiveTimer {
                user_data,
                deadline: Instant::now() + period,
            },
        );
        Ok(())
    }

    /// Best-effort cancellation of an outstanding timer.
    ///
    /// Builds a `TimeoutRemove` SQE keyed on `user_data`. The reactor will
    /// observe a CQE with `res = -libc::ECANCELED` shortly afterwards.
    pub fn cancel_timeout(&mut self, ring: &mut IoUring, target_id: u64) -> Result<(), IoError> {
        // Prefer `TimeoutRemove` (lighter than full AsyncCancel).
        let entry = TimeoutRemove::new(target_id).build();
        match push_entry(ring, entry) {
            Ok(()) => Ok(()),
            Err(IoError::Submit(_)) => {
                // Fall back to `AsyncCancel` if the kernel rejects the
                // remove opcode (it can on older kernels).
                let alt = AsyncCancel::new(target_id).build();
                push_entry(ring, alt)
            }
            other => other,
        }
    }

    /// Removes `user_data` from the active map. Called by the reactor
    /// when it observes the matching fire/cancel CQE. Returns the
    /// retired timer's `user_data` if it was present.
    pub fn retire(&mut self, user_data: u64) -> Option<u64> {
        self.active.remove(&user_data).map(|t| t.user_data)
    }

    /// Returns `true` if `user_data` is currently outstanding.
    #[must_use]
    pub fn is_active(&self, user_data: u64) -> bool {
        self.active.contains_key(&user_data)
    }
}

impl Default for TimerWheel {
    fn default() -> Self {
        Self::new()
    }
}

/// Pushes an SQE and submits the ring, mapping the queue-full case to
/// [`IoError::Backpressure`].
fn push_entry(ring: &mut IoUring, entry: io_uring::squeue::Entry) -> Result<(), IoError> {
    let mut sq = ring.submission();
    // SAFETY: the entry is a well-formed `squeue::Entry` produced by an
    // opcode builder; the kernel will not outlive the borrow held by `sq`.
    if unsafe { sq.push(&entry) }.is_err() {
        return Err(IoError::Backpressure { errno: libc::EBUSY });
    }
    drop(sq);
    ring.submit().map_err(IoError::Submit)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn fresh_ring() -> IoUring {
        IoUring::builder()
            .setup_single_issuer()
            .setup_defer_taskrun()
            .setup_coop_taskrun()
            .build(16)
            .expect("ring")
    }

    #[test]
    fn add_and_retire_lifecycle() {
        let mut ring = fresh_ring();
        let mut tw = TimerWheel::new();
        assert_eq!(tw.active_count(), 0);

        tw.add_timeout(&mut ring, 100, Duration::from_millis(50))
            .expect("add");
        assert_eq!(tw.active_count(), 1);
        assert!(tw.is_active(100));

        // Simulate the reactor retiring it.
        let retired = tw.retire(100);
        assert_eq!(retired, Some(100));
        assert_eq!(tw.active_count(), 0);
        assert!(tw.retire(100).is_none());
    }

    #[test]
    fn add_periodic_tick_records_active() {
        let mut ring = fresh_ring();
        let mut tw = TimerWheel::new();
        tw.add_periodic_tick(&mut ring, 7, Duration::from_millis(10))
            .expect("periodic");
        assert!(tw.is_active(7));
    }

    #[test]
    fn cancel_does_not_panic() {
        let mut ring = fresh_ring();
        let mut tw = TimerWheel::new();
        tw.add_timeout(&mut ring, 42, Duration::from_secs(60))
            .expect("add");
        // We can't synchronously assert the kernel cancelled (that arrives
        // as a CQE), but we verify the SQE path doesn't error.
        tw.cancel_timeout(&mut ring, 42).expect("cancel path");
    }
}
