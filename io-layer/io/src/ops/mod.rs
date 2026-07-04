//! IO operations: user_data encoding scheme + typed SQE builders.
//!
//! Every SQE submitted by the IO layer carries a `user_data` that encodes
//! both the **operation kind** and a caller-supplied **ID** in a single
//! `u64`:
//!
//! ```text
//!   bits [63:60]  OpKind  (4 bits → 16 possible op types)
//!   bits [59: 0]  ID      (60 bits → ~10¹⁸ unique IDs)
//! ```
//!
//! The [`CompletionDecoder`](crate::completion::CompletionDecoder) uses
//! [`decode_kind`] to classify each CQE without any HashMap lookup — just
//! a shift and compare. Zero runtime overhead.

pub mod recv;
pub mod send;
pub mod timeout;

pub use recv::{ParsedFrame, Receiver};
pub use send::Sender;

/// Operation kind encoded in the top 4 bits of `user_data`.
///
/// The numeric values are part of the wire ABI and must not change.
#[repr(u64)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum OpKind {
    /// `RECVMSG` multishot — kernel fills a provided buffer and posts a CQE.
    Recv = 0,
    /// `SEND_ZC` — zero-copy send, two-CQE lifecycle (send + notification).
    Send = 1,
    /// `TIMEOUT` — kernel posts a CQE after the specified duration.
    Timeout = 2,
    /// `ASYNC_CANCEL` — cancels a previously-submitted operation.
    Cancel = 3,
}

/// Bit position of the OpKind field within `user_data`.
pub const OP_SHIFT: u32 = 60;

/// Bitmask for the ID portion of `user_data` (low 60 bits).
pub const ID_MASK: u64 = (1u64 << OP_SHIFT) - 1;

/// Encodes an [`OpKind`] + `id` into a single `user_data`.
///
/// ```
/// use io::ops::{OpKind, encode, decode_kind, decode_id};
///
/// let ud = encode(OpKind::Send, 42);
/// assert_eq!(decode_kind(ud), OpKind::Send);
/// assert_eq!(decode_id(ud), 42);
/// ```
#[must_use]
pub const fn encode(kind: OpKind, id: u64) -> u64 {
    ((kind as u64) << OP_SHIFT) | (id & ID_MASK)
}

/// Extracts the [`OpKind`] from a `user_data` value.
///
/// Returns [`OpKind::Recv`] for values with invalid high bits (defensive
/// default — should not occur with correctly-encoded `user_data`).
#[must_use]
pub const fn decode_kind(user_data: u64) -> OpKind {
    match user_data >> OP_SHIFT {
        0 => OpKind::Recv,
        1 => OpKind::Send,
        2 => OpKind::Timeout,
        3 => OpKind::Cancel,
        _ => OpKind::Recv,
    }
}

/// Extracts the ID from a `user_data` value (low 60 bits).
#[must_use]
pub const fn decode_id(user_data: u64) -> u64 {
    user_data & ID_MASK
}

// ─── SQE submission helper ─────────────────────────────────────────

use io_uring::IoUring;
use io_uring::squeue::Entry;

use crate::error::IoError;

/// Pushes one SQE to the submission queue and submits the ring.
///
/// Maps the queue-full case to [`IoError::Backpressure`].
pub(crate) fn push_entry(ring: &mut IoUring, entry: Entry) -> Result<(), IoError> {
    let mut sq = ring.submission();
    // SAFETY: `entry` is a well-formed SQE from an opcode builder.
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

    #[test]
    fn round_trip_all_kinds() {
        let id = 0x0BAD_F00D_CAFE_BABE & ID_MASK;
        for kind in [OpKind::Recv, OpKind::Send, OpKind::Timeout, OpKind::Cancel] {
            let ud = encode(kind, id);
            assert_eq!(decode_kind(ud), kind);
            assert_eq!(decode_id(ud), id);
        }
    }

    #[test]
    fn id_is_masked_to_60_bits() {
        // Feed a full 64-bit value as id — only the low 60 bits survive.
        let ud = encode(OpKind::Send, u64::MAX);
        assert_eq!(decode_kind(ud), OpKind::Send);
        assert_eq!(decode_id(ud), ID_MASK);
    }

    #[test]
    fn zero_user_data_decodes_as_recv_zero() {
        assert_eq!(decode_kind(0), OpKind::Recv);
        assert_eq!(decode_id(0), 0);
    }

    #[test]
    fn opkind_repr_values_are_stable() {
        // These values are ABI — do not change.
        assert_eq!(OpKind::Recv as u64, 0);
        assert_eq!(OpKind::Send as u64, 1);
        assert_eq!(OpKind::Timeout as u64, 2);
        assert_eq!(OpKind::Cancel as u64, 3);
    }
}
