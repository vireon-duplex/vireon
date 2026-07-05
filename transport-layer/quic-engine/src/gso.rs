//! [`GsoBatcher`] — `UDP_SEGMENT` (Generic Segmentation Offload)
//! batching for outgoing packets.
//!
//! Phase 10 — Packet Scheduler (batching half). See `DESIGN.md`
//! §4.10.
//!
//! GSO lets the kernel split a single large `send()` into multiple
//! UDP datagrams with the same destination. Combined with
//! `sendmmsg`, this collapses dozens of syscalls per tick into one.
//!
//! Wire format (Linux `UDP_SEGMENT`):
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │ single contiguous buffer                                    │
//! │ ┌─────────┬─────────┬─────────┬─────────┬─────────┐         │
//! │ │ packet0 │ packet1 │ packet2 │ packet3 │   ...   │         │
//! │ │ seg_sz  │ seg_sz  │ seg_sz  │ seg_sz  │         │         │
//! │ └─────────┴─────────┴─────────┴─────────┴─────────┘         │
//! │       sent via one send() with UDP_SEGMENT = seg_sz         │
//! └─────────────────────────────────────────────────────────────┘
//! ```
//!
//! All packets in a batch MUST have the same destination and the
//! same segment size. The scheduler hands batches to the GSO module
//! which performs the final coalescing before handing the buffer
//! to [`send-queue`].

/// Maximum number of packets coalesced into a single GSO burst.
/// Matches Cloudflare's quiche-server default.
pub const GSO_MAX_BATCH: usize = 64;

/// Maximum GSO segment size. Must fit in a single UDP datagram on
/// the path MTU.
pub const GSO_MAX_SEGMENT: usize = 1300;

/// Phase-0 skeleton. Phase 10 populates the actual batching loop
/// and the `send-queue::submit_gso` integration.
#[derive(Default)]
pub struct GsoBatcher {
    /// Number of packets currently buffered for the next burst.
    queued: usize,
    /// Configured maximum batch size.
    max_batch: usize,
}

impl GsoBatcher {
    #[must_use]
    pub fn new() -> Self {
        Self {
            queued: 0,
            max_batch: GSO_MAX_BATCH,
        }
    }

    /// TODO(phase-10): `fn push(&mut self, pkt: &[u8], dest: SocketAddr)`
    /// and `fn flush(&mut self, sink: &mut send_queue::SendQueue)`.
    pub fn _placeholder(&self) {}

    /// Whether the batcher has any packets waiting.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.queued == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn placeholder() {
        let b = GsoBatcher::new();
        assert!(b.is_empty());
        // TODO(phase-10): UDP_SEGMENT batching, send-queue flush,
        // fallback to sendmmsg when GSO is unsupported.
    }
}
