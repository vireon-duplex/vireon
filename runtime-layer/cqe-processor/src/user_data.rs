//! user_data helpers (Layer 3).
//!
//! Re-exports the IO Layer's `OpKind`/`encode`/`decode_*` so the CQE
//! Processor does not depend on `io_uring` directly for user_data parsing.
//! The processor still touches `io_uring::IoUring` for raw ring draining
//! (no abstraction for that in the IO Layer), but it never decodes CQE
//! fields itself — `io::CompletionDecoder` does the work.

pub use io::ops::{OpKind, decode_id, decode_kind, encode};

/// Number of reserved OpKind discriminant values. Matches the high-4-bit
/// scheme used by the IO Layer.
pub const OP_KIND_COUNT: usize = 16;

/// Maps an `OpKind` to a table index in `[0, OP_KIND_COUNT)`.
#[inline]
#[must_use]
pub const fn opkind_to_index(kind: OpKind) -> usize {
    kind as usize
}

/// Returns the human-readable name for `kind`. Used by tracing/logging.
#[must_use]
pub const fn opkind_name(kind: OpKind) -> &'static str {
    match kind {
        OpKind::Recv => "recv",
        OpKind::Send => "send",
        OpKind::Timeout => "timeout",
        OpKind::Cancel => "cancel",
    }
}
