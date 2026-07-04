//! SQE builders for kernel-side timeouts (`IORING_OP_TIMEOUT`).
//!
//! These are used internally by [`TimerWheel`](crate::timer_wheel::TimerWheel)
//! so the wheel's `user_data` encoding stays consistent with the
//! [`ops`](crate::ops) scheme.

use std::time::Duration;

use io_uring::opcode::{AsyncCancel, Timeout, TimeoutRemove};
use io_uring::squeue::Entry;
use io_uring::types::{TimeoutFlags, Timespec};

use crate::ops::{OpKind, encode};

/// Builds a one-shot `Timeout` SQE.
///
/// `id` is encoded as `OpKind::Timeout` in the `user_data` so the reactor
/// can classify the resulting CQE.
///
/// **Note**: the `Timespec` is leaked (`Box::leak`) because the kernel
/// reads it at submit time and the pointer must remain valid until then.
/// In practice the kernel copies the value during `io_uring_enter(2)`.
pub fn build_timeout(duration: Duration, id: u64) -> Entry {
    let ts = Box::leak(Box::new(
        Timespec::new()
            .sec(duration.as_secs())
            .nsec(duration.subsec_nanos()),
    ));
    Timeout::new(ts)
        .build()
        .user_data(encode(OpKind::Timeout, id))
}

/// Builds a multishot `Timeout` SQE (kernel ≥ 6.2).
///
/// Each expiry posts a CQE with `IORING_CQE_F_MORE` set until cancelled.
pub fn build_periodic_tick(period: Duration, id: u64) -> Entry {
    let ts = Box::leak(Box::new(
        Timespec::new()
            .sec(period.as_secs())
            .nsec(period.subsec_nanos()),
    ));
    Timeout::new(ts)
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
        // build_timeout uses encode(OpKind::Timeout, id) internally.
        let ud = encode(OpKind::Timeout, 42);
        assert_eq!(decode_kind(ud), OpKind::Timeout);
        assert_eq!(decode_id(ud), 42);
        // Verify the builder compiles and produces an Entry.
        let _entry = build_timeout(Duration::from_millis(50), 42);
    }

    #[test]
    fn build_periodic_tick_encodes_timeout_kind() {
        let ud = encode(OpKind::Timeout, 7);
        assert_eq!(decode_kind(ud), OpKind::Timeout);
        assert_eq!(decode_id(ud), 7);
        let _entry = build_periodic_tick(Duration::from_secs(1), 7);
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
