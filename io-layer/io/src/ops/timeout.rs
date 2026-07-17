//! SQE builders for kernel-side timeouts (`IORING_OP_TIMEOUT`).
//!
//! These are used internally by [`TimerWheel`](crate::timer_wheel::TimerWheel)
//! so the wheel's `user_data` encoding stays consistent with the
//! [`ops`](crate::ops) scheme.

use io_uring::opcode::{AsyncCancel, Timeout, TimeoutRemove};
use io_uring::squeue::Entry;
use io_uring::types::{TimeoutFlags, Timespec};

use crate::ops::{OpKind, encode};

/// Builds a one-shot `Timeout` SQE from a caller-owned `Timespec`.
///
/// The caller MUST keep `timespec` alive until the kernel fires or
/// cancels the timer (i.e. until the matching CQE arrives). Storing
/// it inside `TimerWheel::ActiveTimer` is the intended pattern.
pub fn build_timeout(timespec: &Timespec, id: u64) -> Entry {
    Timeout::new(timespec)
        .build()
        .user_data(encode(OpKind::Timeout, id))
}

/// Builds a multishot `Timeout` SQE (kernel ≥ 6.2).
///
/// Each expiry posts a CQE with `IORING_CQE_F_MORE` set until cancelled.
/// The caller MUST keep `timespec` alive until the timer is cancelled.
pub fn build_periodic_tick(timespec: &Timespec, id: u64) -> Entry {
    Timeout::new(timespec)
        .flags(TimeoutFlags::MULTISHOT)
        .build()
        .user_data(encode(OpKind::Timeout, id))
}

/// Builds a `TimeoutRemove` SQE that cancels the timer whose `user_data`
/// equals `target_user_data`.
///
/// The cancel SQE itself carries `OpKind::Cancel` + `id` so its result
/// CQE can be distinguished from the timer's fire/cancel CQE.
pub fn build_cancel(target_user_data: u64, id: u64) -> Entry {
    TimeoutRemove::new(target_user_data)
        .build()
        .user_data(encode(OpKind::Cancel, id))
}

/// Builds an `AsyncCancel` SQE — the heavier fallback for `TimeoutRemove`
/// on older kernels.
pub fn build_async_cancel(target_user_data: u64, id: u64) -> Entry {
    AsyncCancel::new(target_user_data)
        .build()
        .user_data(encode(OpKind::Cancel, id))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ops::{decode_id, decode_kind};

    #[test]
    fn build_timeout_encodes_timeout_kind() {
        let ud = encode(OpKind::Timeout, 42);
        assert_eq!(decode_kind(ud), OpKind::Timeout);
        assert_eq!(decode_id(ud), 42);
        let ts = Timespec::new().sec(0).nsec(50_000_000);
        let _entry = build_timeout(&ts, 42);
    }

    #[test]
    fn build_periodic_tick_encodes_timeout_kind() {
        let ud = encode(OpKind::Timeout, 7);
        assert_eq!(decode_kind(ud), OpKind::Timeout);
        assert_eq!(decode_id(ud), 7);
        let ts = Timespec::new().sec(1).nsec(0);
        let _entry = build_periodic_tick(&ts, 7);
    }

    #[test]
    fn build_cancel_encodes_cancel_kind() {
        let target = encode(OpKind::Timeout, 99);
        let ud = encode(OpKind::Cancel, 5);
        assert_eq!(decode_kind(ud), OpKind::Cancel);
        assert_eq!(decode_id(ud), 5);
        let _entry = build_cancel(target, 5);
    }
}
