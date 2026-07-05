//! [`ProvidedBufferRing`] — io_uring `IORING_REGISTER_PBUF_RING`
//! for zero-copy RX.
//!
//! See `DESIGN.md` §10 (buffer pool integration) and §13 (io_uring
//! integration).
//!
//! # Why provided buffers?
//!
//! Without a buf-ring, every RECVMSG requires a per-SQE buffer
//! pointer. With a buf-ring, the kernel posts received datagrams
//! directly into pre-registered buffers — no per-SQE pointer, no
//! allocation on the hot path. The CQE carries a `buf_index` that
//! identifies which slot was filled; the reactor returns it via
//! the buf-ring when the consumer is done.

use crate::config::ReactorConfig;

/// A registered provided-buffer group.
// TODO(phase-4): allocate + register the buffer group with the ring.
pub struct ProvidedBufferRing {
    entries: u32,
}

impl ProvidedBufferRing {
    /// Phase-0 placeholder. Phase 4 will allocate the buffer group,
    /// mmap the ring, and call
    /// `io_uring_register_buf_ring(...)` via `io-layer/io`.
    #[must_use]
    pub const fn placeholder(cfg: &ReactorConfig) -> Self {
        Self {
            entries: cfg.buf_ring_entries,
        }
    }

    /// Configured ring depth.
    #[must_use]
    pub const fn entries(&self) -> u32 {
        self.entries
    }

    // TODO(phase-4):
    //   pub fn create(cfg: &ReactorConfig) -> Result<Self, ReactorError>
    //   pub fn register_with(&mut self, ring: &mut RingOwner) -> Result<(), ReactorError>
    //   pub fn return_buffer(&self, buf_id: u32) -> Result<(), ReactorError>
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn placeholder() {
        let r = ProvidedBufferRing::placeholder(&ReactorConfig::default());
        assert_eq!(r.entries(), crate::config::DEFAULT_BUF_RING_ENTRIES);
        // TODO(phase-4): real buf-ring registration.
    }
}
