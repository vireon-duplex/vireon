//! `IoUringCtx` — typed wrapper around `io_uring::IoUring` (ring setup,
//! fixed-buffer registration, sparse-file registration).
//!
//! The reactor owns exactly one `IoUringCtx` per core. Setup follows the
//! Vireon latency profile:
//!
//! * `IORING_SETUP_SINGLE_ISSUER` — only the owning reactor thread submits.
//! * `IORING_SETUP_DEFER_TASKRUN` — task-work is deferred to
//!   `io_uring_enter(2)`, halving context-switch cost vs. the legacy path.
//! * `IORING_SETUP_COOP_TASKRUN` — task-work runs at well-defined points
//!   only, eliminating spurious `EINTR`-style wakeups.
//! * Optional `IORING_SETUP_SQPOLL` — kernel thread consumes SQEs on its
//!   own; enabled when `enable_sqpoll = true`.

use io_uring::IoUring;

use crate::error::IoError;

/// Owns the io_uring instance plus its registered resources.
pub struct IoUringCtx {
    ring: IoUring,
    sqpoll: bool,
}

impl IoUringCtx {
    /// Creates a new ring with `entries` SQ/CQ slots.
    ///
    /// `entries` is rounded up to the next power of two by the kernel; we
    /// still validate `entries > 0` so callers fail loudly on misconfig.
    ///
    /// When `enable_sqpoll` is `true`, an `SQPOLL` kernel thread is spun
    /// up with a 1-second idle timeout. `SQPOLL` requires `CAP_SYS_ADMIN`
    /// or elevated privileges on kernels < 6.0; the error surfaces as
    /// [`IoError::RingSetup`].
    pub fn init_ring(entries: u32, enable_sqpoll: bool) -> Result<Self, IoError> {
        if entries == 0 {
            return Err(IoError::InvalidConfig(
                "io_uring entries must be > 0".into(),
            ));
        }

        let mut builder = IoUring::builder();
        builder
            .setup_single_issuer()
            .setup_defer_taskrun()
            .setup_coop_taskrun();
        if enable_sqpoll {
            // 1000 ms idle before the SQPOLL thread parks itself.
            builder.setup_sqpoll(1000);
        }

        let ring = builder.build(entries).map_err(IoError::RingSetup)?;
        Ok(Self {
            ring,
            sqpoll: enable_sqpoll,
        })
    }

    /// Registers an array of fixed buffers (`IORING_REGISTER_BUFFERS`).
    ///
    /// Once registered, `ReadFixed`/`WriteFixed`/`SendZc` may reference
    /// them by index without re-validating the pointer on every submit.
    ///
    /// # Safety
    ///
    /// The caller guarantees that every `iovec` points to memory that:
    /// 1. is valid for the lifetime of this ring,
    /// 2. is page-aligned (or at least naturally aligned for the access),
    /// 3. is not mutated by any other thread while in flight.
    pub unsafe fn register_fixed_buffers(&self, iovs: &[libc::iovec]) -> Result<(), IoError> {
        // SAFETY: caller asserts iovec validity (see doc comment).
        unsafe { self.ring.submitter().register_buffers(iovs) }.map_err(IoError::RingSetup)
    }

    /// Registers a sparse file table (`IORING_REGISTER_FILES_SPARSE`).
    ///
    /// Sparse tables let the kernel allocate slots lazily on
    /// `IORING_FILE_INDEX_ALLOC` accept. Available since kernel 5.19.
    pub fn register_sparse_files(&self, max_files: u32) -> Result<(), IoError> {
        self.ring
            .submitter()
            .register_files_sparse(max_files)
            .map_err(IoError::RingSetup)
    }

    /// Returns whether `SQPOLL` was requested for this ring.
    ///
    /// Even when `true`, the SQPOLL thread may be parked; consult
    /// `submission_queue().need_wakeup()` before calling `submit()`.
    #[inline]
    #[must_use]
    pub fn sqpoll(&self) -> bool {
        self.sqpoll
    }

    /// Immutable access to the inner ring.
    #[inline]
    #[must_use]
    pub fn ring(&self) -> &IoUring {
        &self.ring
    }

    /// Mutable access to the inner ring.
    #[inline]
    #[must_use]
    pub fn ring_mut(&mut self) -> &mut IoUring {
        &mut self.ring
    }
}

impl std::fmt::Debug for IoUringCtx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IoUringCtx")
            .field("sqpoll", &self.sqpoll)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn init_ring_rejects_zero_entries() {
        let err = IoUringCtx::init_ring(0, false).expect_err("zero entries");
        assert!(matches!(err, IoError::InvalidConfig(_)));
    }

    #[test]
    fn init_ring_succeeds_on_this_kernel() {
        // entries=4 is the smallest legal ring; it must succeed on any
        // kernel >= 5.1 with io_uring available.
        let ctx = IoUringCtx::init_ring(4, false).expect("ring init");
        assert!(!ctx.sqpoll());
        assert_eq!(ctx.ring().params().sq_entries(), 4);
    }

    #[test]
    fn sqpoll_flag_is_recorded() {
        // Requesting SQPOLL without CAP_SYS_ADMIN will usually fail; we
        // only assert the flag plumbing when the kernel honours it.
        if let Ok(ctx) = IoUringCtx::init_ring(4, true) {
            assert!(ctx.sqpoll());
        }
    }
}
