//! [`Frame`] — frame-level hooks + extension frame policy.
//!
//! Phase 9 — Frame Engine. See `DESIGN.md` §4.9.
//!
//! RFC 9000 §19 defines 60+ frame types. quiche parses them all
//! internally; Vireon's role is:
//!
//! - Surface per-frame events for stats / observability
//! - Configure unknown-frame policy (ignore / close) per RFC 9000
//!   §19.21
//! - Encode / decode extension frames not yet supported by quiche
//!   (e.g. DATAGRAM (RFC 9221) — though quiche supports it
//!   natively)
//! - Batch frame-level metrics (count by frame type) for export
//!
//! No frame parsing happens in Vireon — quiche owns the parser.

/// Phase-0 skeleton. Phase 9 will hold the frame-type counters and
/// the extension-frame dispatch table.
#[derive(Default)]
pub struct Frame {
    /// Frames seen since engine start (sum across all types).
    frames_seen: u64,
}

impl Frame {
    /// TODO(phase-9): per-type counters + unknown-frame policy.
    #[must_use]
    pub const fn new() -> Self {
        Self { frames_seen: 0 }
    }

    /// TODO(phase-9): `fn on_frame(&mut self, ty: u64)`.
    pub fn _placeholder(&self) {}
}

/// Phase-9 policy for frames quiche doesn't recognize.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum UnknownFramePolicy {
    /// Silently ignore the frame (RFC 9000 §19.21 permits this).
    #[default]
    Ignore,
    /// Close the connection with a PROTOCOL_VIOLATION error.
    Close,
}

/// DATAGRAM frame extension (RFC 9221) — configured via Phase 9
/// policy but actually emitted/consumed by quiche.
#[derive(Clone, Copy, Debug)]
pub struct DatagramConfig {
    /// Maximum DATAGRAM payload we are willing to accept.
    pub max_datagram_payload: u16,
    /// Whether the application is allowed to send DATAGRAMs.
    pub enabled: bool,
}

impl Default for DatagramConfig {
    fn default() -> Self {
        Self {
            max_datagram_payload: 1200,
            enabled: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn placeholder() {
        let _f = Frame::new();
        // TODO(phase-9): per-type counters, DATAGRAM handling,
        // extension-frame dispatch.
    }
}
