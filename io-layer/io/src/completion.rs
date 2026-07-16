//! Centralized CQE decoder: maps raw `io_uring::cqueue::Entry` → typed
//! [`Completion`].
//!
//! The reactor matches on [`Completion`] variants instead of inspecting
//! raw CQE flags (`IORING_CQE_F_BUFFER`, `IORING_CQE_F_MORE`,
//! `IORING_CQE_F_NOTIF`, etc.). This keeps all io_uring flag knowledge
//! inside this module — the Runtime layer never sees a CQE.
//!
//! # Dispatch table
//!
//! | OpKind (user_data high bits) | `res >= 0` | `res < 0` | `flags & NOTIF` |
//! |-----------------------------|------------|-----------|-----------------|
//! | `Recv`  | [`Receive`](Completion::Receive) | [`ReceiveError`](Completion::ReceiveError) | n/a |
//! | `Send`                      | [`Completion::Send`] | [`Completion::Error`] | [`Completion::SendBufferReclaim`] |
//! | `Timeout`                   | [`Completion::Timeout`] | [`Completion::Error`] | n/a |
//! | `Cancel`                    | [`Completion::Cancel`] | [`Completion::Cancel`] | n/a |

use io_uring::cqueue;

use crate::ops::{OpKind, decode_id, decode_kind};

/// A decoded io_uring completion.
///
/// Produced by [`CompletionDecoder::decode`]; consumed by the reactor's
/// dispatch loop.
#[derive(Debug, Clone)]
pub enum Completion {
    /// A multishot `RECVMSG` completion. The kernel filled buffer `buf_id`
    /// with `len` bytes. If `more` is `false`, multishot has terminated
    /// and RX must be re-armed.
    Receive {
        /// Caller-supplied ID from `user_data` (low 60 bits).
        id: u64,
        /// Provided-buffer id the kernel filled (`IORING_CQE_F_BUFFER`).
        /// `None` if the kernel did not use a provided buffer.
        buf_id: Option<u16>,
        /// Payload length in bytes (`cqe.result`).
        len: usize,
        /// `true` if more CQEs will follow without re-arming
        /// (`IORING_CQE_F_MORE`).
        more: bool,
    },

    /// A multishot `RECVMSG` error. The kernel reported a negative `res`
    /// (e.g. `-ENOBUFS` when the buffer group ran dry, or `-ECANCELED`
    /// when the multishot was terminated).
    ///
    /// If `more` is `false`, the multishot has terminated and RX must be
    /// re-armed. If `more` is `true` (e.g. `-ENOBUFS`), the multishot is
    /// still armed — just refill buffers and continue.
    ReceiveError {
        /// Caller-supplied ID from `user_data`.
        id: u64,
        /// Negated errno (e.g. `-libc::ENOBUFS`, `-libc::ECANCELED`).
        errno: i32,
        /// `true` if the multishot is still armed (more CQEs expected).
        more: bool,
    },

    /// First CQE of a `SEND_ZC`: the kernel accepted the send; buffer is
    /// still in-flight (NIC DMA pending).
    Send {
        /// Caller-supplied ID from `user_data`.
        id: u64,
        /// Positive = bytes accepted; negative would have been
        /// [`Completion::Error`].
        res: i32,
    },

    /// Second CQE of a `SEND_ZC` (`IORING_CQE_F_NOTIF`): the NIC finished
    /// DMA and the buffer may now be recycled into the pool.
    SendBufferReclaim {
        /// Caller-supplied ID from `user_data`.
        id: u64,
    },

    /// A `TIMEOUT` operation fired.
    Timeout {
        /// Caller-supplied ID from `user_data`.
        id: u64,
    },

    /// An `ASYNC_CANCEL` completed (or a cancelled operation reported
    /// `-ECANCELED`).
    Cancel {
        /// Caller-supplied ID from `user_data`.
        id: u64,
        /// `res` from the cancel CQE (0 = success, negative = errno).
        res: i32,
    },

    /// A CQE reported a negative `res` (kernel errno). The reactor should
    /// inspect `errno` for recovery (e.g. `-ENOBUFS` → buffer exhaustion,
    /// `-ECANCELED` → cancelled by request).
    Error {
        /// Caller-supplied ID from `user_data` (0 if untagged).
        id: u64,
        /// Negated errno value (e.g. `-libc::ENOBUFS`).
        errno: i32,
    },
}

/// Stateless decoder that maps CQE entries to typed [`Completion`]s.
///
/// ```
/// use io::completion::{Completion, CompletionDecoder};
///
/// let decoder = CompletionDecoder::new();
/// ```
///
/// The decoder reads the [`OpKind`] from the high
/// bits of `user_data` and dispatches accordingly. No HashMap lookup,
/// no allocation — just a shift + match.
#[derive(Debug, Clone, Copy, Default)]
pub struct CompletionDecoder;

impl CompletionDecoder {
    /// Creates a new decoder.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }

    /// Decodes a single CQE entry into a [`Completion`].
    #[inline]
    #[must_use]
    pub fn decode(&self, cqe: &cqueue::Entry) -> Completion {
        Self::decode_raw(cqe.user_data(), cqe.result(), cqe.flags())
    }

    /// Decodes from raw CQE field values (used by [`decode`](Self::decode)
    /// and by unit tests that cannot construct `cqueue::Entry` directly).
    #[inline]
    fn decode_raw(user_data: u64, res: i32, flags: u32) -> Completion {
        let kind = decode_kind(user_data);
        let id = decode_id(user_data);

        match kind {
            OpKind::Recv => Self::decode_recv(id, res, flags),
            OpKind::Send => Self::decode_send(id, res, flags),
            OpKind::Timeout => Completion::Timeout { id },
            OpKind::Cancel => Completion::Cancel { id, res },
        }
    }

    #[inline]
    fn decode_recv(id: u64, res: i32, flags: u32) -> Completion {
        if res < 0 {
            return Completion::ReceiveError {
                id,
                errno: res,
                more: cqueue::more(flags),
            };
        }
        let buf_id = cqueue::buffer_select(flags);
        let more = cqueue::more(flags);
        Completion::Receive {
            id,
            buf_id,
            len: res as usize,
            more,
        }
    }

    #[inline]
    fn decode_send(id: u64, res: i32, flags: u32) -> Completion {
        // Notification CQE (second half of SEND_ZC lifecycle).
        if cqueue::notif(flags) {
            return Completion::SendBufferReclaim { id };
        }
        if res < 0 {
            return Completion::Error { id, errno: res };
        }
        Completion::Send { id, res }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ops::encode;

    #[test]
    fn decode_recv_success() {
        let ud = encode(OpKind::Recv, 42);
        // Simulate IORING_CQE_F_BUFFER flag with buf_id=7
        let flags = (1u32 << 0) | (7u32 << 16);
        match CompletionDecoder::decode_raw(ud, 1024, flags) {
            Completion::Receive {
                id,
                buf_id,
                len,
                more,
            } => {
                assert_eq!(id, 42);
                assert_eq!(buf_id, Some(7));
                assert_eq!(len, 1024);
                assert!(!more);
            }
            other => panic!("expected Receive, got {other:?}"),
        }
    }

    #[test]
    fn decode_recv_error() {
        let ud = encode(OpKind::Recv, 5);
        match CompletionDecoder::decode_raw(ud, -libc::ENOBUFS, 0) {
            Completion::ReceiveError { id, errno, more } => {
                assert_eq!(id, 5);
                assert_eq!(errno, -libc::ENOBUFS);
                assert!(!more);
            }
            other => panic!("expected ReceiveError, got {other:?}"),
        }
    }

    #[test]
    fn decode_send_success() {
        let ud = encode(OpKind::Send, 99);
        match CompletionDecoder::decode_raw(ud, 512, 0) {
            Completion::Send { id, res } => {
                assert_eq!(id, 99);
                assert_eq!(res, 512);
            }
            other => panic!("expected Send, got {other:?}"),
        }
    }

    #[test]
    fn decode_send_notification() {
        let ud = encode(OpKind::Send, 7);
        // IORING_CQE_F_NOTIF = 1 << 3
        match CompletionDecoder::decode_raw(ud, 0, 1 << 3) {
            Completion::SendBufferReclaim { id } => assert_eq!(id, 7),
            other => panic!("expected SendBufferReclaim, got {other:?}"),
        }
    }

    #[test]
    fn decode_timeout() {
        let ud = encode(OpKind::Timeout, 123);
        match CompletionDecoder::decode_raw(ud, -libc::ETIME, 0) {
            Completion::Timeout { id } => assert_eq!(id, 123),
            other => panic!("expected Timeout, got {other:?}"),
        }
    }

    #[test]
    fn decode_cancel() {
        let ud = encode(OpKind::Cancel, 55);
        match CompletionDecoder::decode_raw(ud, 0, 0) {
            Completion::Cancel { id, res } => {
                assert_eq!(id, 55);
                assert_eq!(res, 0);
            }
            other => panic!("expected Cancel, got {other:?}"),
        }
    }
}
