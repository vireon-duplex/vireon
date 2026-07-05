//! [`EventfdWake`] — cross-thread wake via `eventfd` registered
//! with the io_uring ring.
//!
//! See `DESIGN.md` §3 (event loop), §12 (lock-free design), and
//! §13 (io_uring integration).
//!
//! # Pattern
//!
//! ```text
//! other thread                reactor thread
//! ─────────────                ──────────────
//! wake():                      park_on_ring():
//!   write(eventfd, 1)            io_uring_enter(GETEVENTS)
//!                              ─► kernel posts CQE for eventfd
//!                              drain_cq:
//!                                on_wake()
//!                                stats.wakeups += 1
//! ```
//!
//! This matches Glommio and tokio-uring. No `mio`, no epoll on the
//! wake path — pure io_uring.

/// Cross-thread wake primitive backed by an `eventfd(2)` descriptor
/// registered with this reactor's io_uring ring.
// TODO(phase-3): implement eventfd + io_uring registration.
pub struct EventfdWake {
    fd: i32,
}

impl EventfdWake {
    /// Phase-0 placeholder. Phase 3 will replace this with
    /// [`EventfdWake::create`], which calls `eventfd(0, EFD_CLOEXEC)`
    /// and registers the fd with the ring for POLL/EPOLL via multishot
    /// `IORING_OP_POLL_ADD`.
    #[must_use]
    pub const fn placeholder() -> Self {
        Self { fd: -1 }
    }

    /// Whether this wake is initialised (has a real fd).
    #[must_use]
    pub const fn is_open(&self) -> bool {
        self.fd >= 0
    }

    // TODO(phase-3):
    //   pub fn create() -> Result<Self, ReactorError>
    //   pub fn wake(&self) -> Result<(), ReactorError>
    //   pub fn drain(&self) -> Result<u64, ReactorError>
    //   pub fn register_with_ring(&mut self, ring: &mut RingOwner) -> Result<(), ReactorError>
}

impl Default for EventfdWake {
    fn default() -> Self {
        Self::placeholder()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn placeholder() {
        let w = EventfdWake::placeholder();
        assert!(!w.is_open());
        // TODO(phase-3): real eventfd + ring registration.
    }
}
