//! [`GsoBatcher`] — `UDP_SEGMENT` (Generic Segmentation Offload)
//! batching for outgoing packets.
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
//! same segment size (except the last, which may be shorter).

use std::net::SocketAddr;

use io::Sender;
use io::error::IoError;
use profiler::profile;

/// Maximum number of packets coalesced into a single GSO burst.
/// Matches Cloudflare's quiche-server default.
pub const GSO_MAX_BATCH: usize = 64;

/// Flush threshold in bytes. Prevents oversized kernel send buffers when
/// packets are large (e.g. jumbo frames). With standard 1400-byte QUIC
/// packets this rarely triggers before `GSO_MAX_BATCH`; it acts as a
/// safety net for high-MTU paths.
const GSO_FLUSH_BYTES: usize = 64 * 1024;

/// Pre-allocated batch buffer capacity. Large enough for
/// `GSO_MAX_BATCH` full-size jumbo packets.
const BATCH_BUF_CAPACITY: usize = GSO_MAX_BATCH * 65_535;

/// Collects outgoing QUIC packets and flushes them via `UDP_SEGMENT`
/// when multiple packets share the same destination and segment size.
///
/// When a packet arrives that doesn't match the current batch's
/// `(dest, seg_size)`, the pending batch is flushed first, then the
/// new packet starts a fresh batch.
///
/// If the kernel rejects GSO (e.g. older kernel without `UDP_SEGMENT`
/// support), the batcher automatically falls back to per-packet
/// `send_blocking` for all subsequent sends.
pub struct GsoBatcher {
    /// Contiguous buffer of concatenated same-size packets.
    buf: Vec<u8>,
    /// Segment size of the current batch (0 = batch empty).
    seg_size: usize,
    /// Destination of the current batch.
    dest: Option<SocketAddr>,
    /// Number of packets in the current batch.
    count: usize,
    /// Whether the kernel accepted GSO on the last attempt.
    /// Once false, all subsequent sends use `send_blocking`.
    gso_supported: bool,
}

impl Default for GsoBatcher {
    fn default() -> Self {
        Self::new()
    }
}

impl GsoBatcher {
    #[must_use]
    pub fn new() -> Self {
        Self {
            buf: Vec::with_capacity(BATCH_BUF_CAPACITY),
            seg_size: 0,
            dest: None,
            count: 0,
            gso_supported: true,
        }
    }

    /// Whether the batcher has any packets waiting.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Pushes a packet. If it doesn't match the current batch's
    /// `(dest, seg_size)`, or the batch is full, the pending batch is
    /// flushed first via `sender`.
    #[profile]
    pub fn push(&mut self, pkt: &[u8], dest: SocketAddr, sender: &Sender) -> Result<(), IoError> {
        let needs_flush = self.count >= GSO_MAX_BATCH
            || self.buf.len() + pkt.len() >= GSO_FLUSH_BYTES
            || (self.count > 0 && (self.dest != Some(dest) || pkt.len() != self.seg_size));

        if needs_flush {
            // Flush the old batch. Errors here (e.g. EAGAIN on a
            // non-blocking socket) must not prevent the new packet from
            // being queued — the old batch is already lost regardless.
            let _ = self.flush(sender);
        }

        if self.count == 0 {
            // Start a new batch.
            self.seg_size = pkt.len();
            self.dest = Some(dest);
        }

        self.buf.extend_from_slice(pkt);
        self.count += 1;
        Ok(())
    }

    /// Flushes any remaining batched packets. If only one packet is
    /// pending, uses `send_blocking` (no GSO benefit for a single
    /// packet). If GSO is unsupported, falls back to `send_blocking`
    /// for each segment.
    #[profile]
    pub fn flush(&mut self, sender: &Sender) -> Result<(), IoError> {
        if self.count == 0 {
            return Ok(());
        }

        let Some(dest) = self.dest else {
            self.reset();
            return Ok(());
        };

        // Single packet — no GSO benefit, just send_blocking.
        if self.count == 1 || !self.gso_supported {
            return self.flush_individual(sender, dest);
        }

        // Try GSO.
        match sender.send_gso(&self.buf, self.seg_size as u16, dest) {
            Ok(_) => {
                self.reset();
                Ok(())
            }
            Err(e) => {
                let errno = match &e {
                    IoError::Submit(io_err) => io_err.raw_os_error(),
                    _ => None,
                };
                match errno {
                    // EAGAIN/EWOULDBLOCK (11) — socket send buffer full on
                    // a non-blocking socket. Transient: fall back to
                    // individual sends WITHOUT permanently disabling GSO.
                    Some(11) => self.flush_individual(sender, dest),
                    // EMSGSIZE (90) — total GSO buffer exceeds the kernel's
                    // max send size for this interface (common on loopback
                    // with large segments). Fall back to individual sends;
                    // each packet alone is within the UDP max datagram size.
                    Some(90) => self.flush_individual(sender, dest),
                    // EOPNOTSUPP (95), EINVAL (22), ENOTSUP (251) — kernel
                    // doesn't support UDP_SEGMENT. Disable GSO permanently.
                    Some(95) | Some(22) | Some(251) => {
                        tracing::warn!(
                            errno,
                            "GSO unsupported — falling back to per-packet send_blocking"
                        );
                        self.gso_supported = false;
                        self.flush_individual(sender, dest)
                    }
                    // Any other error: fall back to individual sends rather
                    // than dropping the entire batch.
                    _ => self.flush_individual(sender, dest),
                }
            }
        }
    }

    /// Sends each segment individually via `send_blocking`.
    ///
    /// Resets the batch state even on error to prevent duplicate sends
    /// when the caller retries.
    #[profile]
    fn flush_individual(&mut self, sender: &Sender, dest: SocketAddr) -> Result<(), IoError> {
        let seg = self.seg_size;
        let total = self.buf.len();
        let mut off = 0usize;
        while off < total {
            let end = (off + seg).min(total);
            if let Err(e) = sender.send_blocking(&self.buf[off..end], dest) {
                // Reset even on partial failure so the next push/flush
                // starts fresh — no duplicate sends of already-sent
                // segments.
                self.reset();
                return Err(e);
            }
            off = end;
        }
        self.reset();
        Ok(())
    }

    fn reset(&mut self) {
        self.buf.clear();
        self.seg_size = 0;
        self.dest = None;
        self.count = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn batcher_starts_empty() {
        let b = GsoBatcher::new();
        assert!(b.is_empty());
        assert_eq!(b.count, 0);
    }

    #[test]
    fn capacity_is_reasonable() {
        // Should hold at least GSO_MAX_BATCH jumbo packets.
        assert!(GSO_MAX_BATCH * 65_535 <= BATCH_BUF_CAPACITY);
    }
}
