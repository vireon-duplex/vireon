//! Errors returned by the send-policy crate.

use thiserror::Error;

/// All errors emitted by [`crate::SendPolicyTable`].
///
/// Note that the [`crate::StreamOpenMeta`] decode errors live in
/// `delivery-policy` because that crate owns the wire format. The send
/// side consumes the decoded policy byte and surfaces its own table-level
/// failure modes here.
#[derive(Debug, Error)]
pub enum SendPolicyError {
    /// [`SendPolicyTable::bind`](crate::SendPolicyTable::bind) was called
    /// past the configured [`MAX_TRACKED_STREAMS`](crate::table::MAX_TRACKED_STREAMS).
    ///
    /// Bounded to prevent a misbehaving caller from exhausting reactor
    /// memory by registering millions of streams.
    #[error("send-policy table full (cap={cap})")]
    TableFull {
        /// The capacity that was exceeded.
        cap: usize,
    },
}
