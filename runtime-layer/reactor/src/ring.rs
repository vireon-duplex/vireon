//! [`RingOwner`] — owns the io_uring ring for this reactor.
//!
//! See `DESIGN.md` §13 (io_uring integration).
//!
//! Phase 2 will populate this with:
//! - `IoUringCtx` (from `io-layer/io`) wrapped in
//!   `IORING_SETUP_SINGLE_ISSUER | DEFER_TASKRUN | COOP_TASKRUN`
//! - multishot RECVMSG registration (one per UDP socket)
//! - provided-buffer ring registration
//!   (`IORING_REGISTER_PBUF_RING`)
//! - TIMEOUT / TIMEOUT_REMOVE for the timer wheel
//! - SEND_ZC submissions from the send queue

use crate::config::ReactorConfig;

/// Owns the io_uring ring + registered resources for one reactor.
// TODO(phase-2): wrap IoUringCtx and add registration lifecycle.
pub struct RingOwner {
    ring_entries: u32,
}

impl RingOwner {
    /// Phase-0 placeholder. Phase 2 will construct a real ring via
    /// `io::IoUringCtx::init(...)` and register resources.
    #[must_use]
    pub const fn placeholder(cfg: &ReactorConfig) -> Self {
        Self {
            ring_entries: cfg.ring_entries,
        }
    }

    /// Configured ring depth.
    #[must_use]
    pub const fn ring_entries(&self) -> u32 {
        self.ring_entries
    }

    // TODO(phase-2):
    //   pub fn submit_and_wait(&mut self, n: u32) -> Result<u32, ReactorError>
    //   pub fn register_multishot_recv(&mut self, fd: RawFd) -> Result<(), ReactorError>
    //   pub fn register_buf_ring(&mut self, ring: &ProvidedBufferRing) -> Result<(), ReactorError>
    //   pub fn arm_timeout(&mut self, task_id: u64, deadline_ns: u64) -> Result<(), ReactorError>
    //   pub fn cancel(&mut self, user_data: u64) -> Result<(), ReactorError>
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn placeholder() {
        let r = RingOwner::placeholder(&ReactorConfig::default());
        assert_eq!(r.ring_entries(), crate::config::DEFAULT_RING_ENTRIES);
        // TODO(phase-2): real ring setup + registration.
    }
}
