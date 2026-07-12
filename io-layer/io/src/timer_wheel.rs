//! Kernel-side timer wheel built on `IORING_OP_TIMEOUT`.
//!
//! Distinct from `runtime-layer/timer-wheel` (which is a userspace hierarchical
//! wheel): this module issues io_uring `Timeout` SQEs and tracks them so
//! the reactor can `cancel_timeout` an in-flight timer by `user_data`.
//!
//! # user_data encoding
//!
//! Every timer SQE carries `user_data = encode(OpKind::Timeout, id)`.
//! Cancel SQEs carry `encode(OpKind::Cancel, id)`. The reactor decodes
//! the kind + ID from the CQE's `user_data` via [`ops`](crate::ops).
//!
//! # Lifecycle
//!
//! 1. [`TimerWheel::add_timeout`] builds a `Timeout` opcode with the
//!    caller's `id` encoded into `user_data`, pushes it to the SQ, and
//!    records the `(id, deadline)` pair in the active map.
//! 2. When the kernel fires (or cancels) the timer, a CQE with that
//!    `user_data` lands in the reactor's CQ. The reactor removes it
//!    from the active map via [`TimerWheel::retire`].
//! 3. [`TimerWheel::cancel_timeout`] enqueues a `TimeoutRemove` SQE; the
//!    matching CQE arrives later with `res = -libc::ECANCELED`.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use io_uring::IoUring;
use io_uring::types::Timespec;

use crate::error::IoError;
use crate::ops::timeout::{build_async_cancel, build_cancel, build_periodic_tick, build_timeout};
use crate::ops::{OpKind, decode_id, decode_kind, encode, push_entry};

/// Bookkeeping for one in-flight kernel timeout.
#[derive(Debug)]
#[allow(dead_code)]
struct ActiveTimer {
    /// Caller-supplied ID (decoded from `user_data`).
    user_data: u64,
    /// When the timer was scheduled to fire (best-effort wall-clock).
    deadline: Instant,
    /// Owned Timespec — kept alive until the kernel fires or cancels
    /// the timer, preventing the leak that `Box::leak` caused.
    _timespec: Option<Box<Timespec>>,
}

/// Kernel-side timer wheel.
pub struct TimerWheel {
    /// Keyed by the caller-supplied ID (NOT the encoded `user_data`).
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
    /// `id` is the caller-supplied ID encoded into the SQE's `user_data`
    /// so the reactor can route the fire event back to the originating
    /// connection/stream.
    pub fn add_timeout(
        &mut self,
        ring: &mut IoUring,
        id: u64,
        duration: Duration,
    ) -> Result<(), IoError> {
        let ts = Box::new(
            Timespec::new()
                .sec(duration.as_secs())
                .nsec(duration.subsec_nanos()),
        );
        let entry = build_timeout(&ts, id);
        push_entry(ring, entry)?;
        self.active.insert(
            id,
            ActiveTimer {
                user_data: id,
                deadline: Instant::now() + duration,
                _timespec: Some(ts),
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
        id: u64,
        period: Duration,
    ) -> Result<(), IoError> {
        let ts = Box::new(
            Timespec::new()
                .sec(period.as_secs())
                .nsec(period.subsec_nanos()),
        );
        let entry = build_periodic_tick(&ts, id);
        push_entry(ring, entry)?;
        self.active.insert(
            id,
            ActiveTimer {
                user_data: id,
                deadline: Instant::now() + period,
                _timespec: Some(ts),
            },
        );
        Ok(())
    }

    /// Best-effort cancellation of an outstanding timer.
    ///
    /// Builds a `TimeoutRemove` SQE keyed on the encoded `user_data` of
    /// the timer. The reactor will observe a CQE with
    /// `res = -libc::ECANCELED` shortly afterwards.
    pub fn cancel_timeout(&mut self, ring: &mut IoUring, id: u64) -> Result<(), IoError> {
        let target = encode(OpKind::Timeout, id);
        let entry = build_cancel(target, id);
        match push_entry(ring, entry) {
            Ok(()) => Ok(()),
            Err(IoError::Submit(_)) => {
                // Fall back to `AsyncCancel` if the kernel rejects the
                // remove opcode (it can on older kernels).
                let alt = build_async_cancel(target, id);
                push_entry(ring, alt)
            }
            other => other,
        }
    }

    /// Removes the timer identified by `user_data` (raw CQE value) from
    /// the active map. Called by the reactor when it observes the matching
    /// fire/cancel CQE. Returns the retired timer's ID if it was present.
    ///
    /// Decodes the caller ID from `user_data` internally.
    pub fn retire(&mut self, user_data: u64) -> Option<u64> {
        let id = decode_id(user_data);
        self.active.remove(&id).map(|t| t.user_data)
    }

    /// Returns `true` if the CQE's `user_data` corresponds to an active
    /// timer.
    ///
    /// Checks `OpKind::Timeout` first so non-timer CQEs (e.g. send
    /// completions) don't accidentally match by ID collision.
    #[must_use]
    pub fn is_active(&self, user_data: u64) -> bool {
        if decode_kind(user_data) != OpKind::Timeout {
            return false;
        }
        let id = decode_id(user_data);
        self.active.contains_key(&id)
    }
}

impl Default for TimerWheel {
    fn default() -> Self {
        Self::new()
    }
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

        // Simulate the reactor retiring it via the encoded user_data.
        let encoded = encode(OpKind::Timeout, 100);
        assert!(tw.is_active(encoded));
        let retired = tw.retire(encoded);
        assert_eq!(retired, Some(100));
        assert_eq!(tw.active_count(), 0);
        assert!(tw.retire(encoded).is_none());
    }

    #[test]
    fn add_periodic_tick_records_active() {
        let mut ring = fresh_ring();
        let mut tw = TimerWheel::new();
        tw.add_periodic_tick(&mut ring, 7, Duration::from_millis(10))
            .expect("periodic");
        let encoded = encode(OpKind::Timeout, 7);
        assert!(tw.is_active(encoded));
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

    #[test]
    fn is_active_rejects_non_timer_user_data() {
        let mut ring = fresh_ring();
        let mut tw = TimerWheel::new();
        tw.add_timeout(&mut ring, 5, Duration::from_secs(1))
            .expect("add");

        // A Send-kind user_data with the same ID should not match.
        let send_ud = encode(OpKind::Send, 5);
        assert!(!tw.is_active(send_ud));

        // A Recv-kind user_data with the same ID should not match.
        let recv_ud = encode(OpKind::Recv, 5);
        assert!(!tw.is_active(recv_ud));

        // The correct Timeout-kind user_data matches.
        let timeout_ud = encode(OpKind::Timeout, 5);
        assert!(tw.is_active(timeout_ud));
    }
}
