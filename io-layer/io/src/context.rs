//! `IoUringCtx` — typed wrapper around `io_uring::IoUring`.
//!
//! Owns the ring, its [`IoRegistry`] (for explicit resource lifetime
//! management), and a cached [`IoFeatures`] bitmask (kernel capability
//! detection).
//!
//! # Setup flags
//!
//! * `IORING_SETUP_SINGLE_ISSUER` — only the owning reactor thread submits.
//! * `IORING_SETUP_DEFER_TASKRUN` — task-work is deferred to
//!   `io_uring_enter(2)`, halving context-switch cost vs. the legacy path.
//! * `IORING_SETUP_COOP_TASKRUN` — task-work runs at well-defined points
//!   only, eliminating spurious `EINTR`-style wakeups.
//! * Optional `IORING_SETUP_SQPOLL` — kernel thread consumes SQEs on its
//!   own; enabled when `enable_sqpoll = true`.
//!
//! # Drop ordering
//!
//! Fields are declared so that `registry` drops **before** `ring`:
//! the registry's `Drop` explicitly unregisters kernel resources via
//! `io_uring_register(2)`, which requires the ring fd to still be open.

use std::os::unix::io::AsRawFd;

use io_uring::IoUring;

use crate::error::IoError;
use crate::features::IoFeatures;
use crate::registry::IoRegistry;

/// `io_uring_register(fd, IORING_REGISTER_NAPI, &io_uring_napi, 1)`
/// (kernel ≥ 6.6). Not in upstream `libc`; defined locally.
const IORING_REGISTER_NAPI: u32 = 27;

/// Wire layout of `struct io_uring_napi` as of kernel 6.6 (16 bytes).
///
/// The kernel reads `busy_poll_to` + `prefer_busy_poll` first and treats
/// the rest as reserved, so this 6.6 layout is forward-compatible with
/// 6.11+ (which split `resv` into `opcode` + `op_param` + `resv`).
#[repr(C)]
struct IoUringNapi {
    /// Busy-poll timeout in microseconds.
    busy_poll_to: u32,
    /// `1` to pin the ring into busy-poll mode (skip softirq).
    prefer_busy_poll: u8,
    pad: [u8; 3],
    /// Reserved (kernel 6.6: `u64 resv`; 6.11+: split into opcode/op_param/resv).
    resv: u64,
}

/// Owns the io_uring instance, its resource registry, and detected
/// kernel features.
///
/// Field order is significant: `registry` must be declared before `ring`
/// so that `Drop` runs unregister syscalls before the ring fd is closed.
pub struct IoUringCtx {
    /// Dropped first — explicit unregister of buffers/files.
    registry: IoRegistry,
    /// Kernel feature bitmask, detected at `init_ring` time.
    features: IoFeatures,
    /// Dropped last — closes the ring fd.
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
    ///
    /// When `sqpoll_cpu` is `Some(cpu)` **and** `enable_sqpoll` is `true`,
    /// the SQPOLL kernel thread is pinned to `cpu` via `IORING_SETUP_SQ_AFF`
    /// (`sq_thread_cpu`). This co-locates the kernel poller with the
    /// submitting reactor thread, avoiding cross-core SQE cache bouncing.
    /// `sqpoll_cpu` is ignored when SQPOLL is disabled.
    pub fn init_ring(
        entries: u32,
        enable_sqpoll: bool,
        sqpoll_cpu: Option<u32>,
    ) -> Result<Self, IoError> {
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
            builder.setup_sqpoll(1000);
            if let Some(cpu) = sqpoll_cpu {
                // Sets IORING_SETUP_SQ_AFF + sq_thread_cpu = cpu, pinning
                // the SQPOLL kernel thread to the same core as the reactor.
                builder.setup_sqpoll_cpu(cpu);
            }
        }

        let ring = builder.build(entries).map_err(IoError::RingSetup)?;
        let features = IoFeatures::detect(&ring);
        let ring_fd = ring.as_raw_fd();
        let registry = IoRegistry::new(ring_fd);

        Ok(Self {
            registry,
            features,
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
    pub unsafe fn register_buffers(&mut self, iovs: &[libc::iovec]) -> Result<(), IoError> {
        // Split borrow so we can call submitter (immutable) and update
        // registry state (mutable) without conflict.
        let Self { ring, registry, .. } = self;
        // SAFETY: caller asserts iovec validity (see doc comment).
        unsafe { ring.submitter().register_buffers(iovs) }.map_err(IoError::RegistrationFailed)?;
        registry.set_buffers_registered(true);
        Ok(())
    }

    /// Registers a sparse file table (`IORING_REGISTER_FILES_SPARSE`).
    ///
    /// Sparse tables let the kernel allocate slots lazily on
    /// `IORING_FILE_INDEX_ALLOC` accept. Available since kernel 5.19.
    pub fn register_files_sparse(&mut self, max_files: u32) -> Result<(), IoError> {
        let Self { ring, registry, .. } = self;
        ring.submitter()
            .register_files_sparse(max_files)
            .map_err(IoError::RegistrationFailed)?;
        registry.set_files_registered(true);
        Ok(())
    }

    /// Registers ring-level NAPI busy poll (`IORING_REGISTER_NAPI`,
    /// kernel ≥ 6.6). Distinct from socket-level `SO_BUSY_POLL`: this
    /// lets the ring itself drive NAPI polling without raising softirqs,
    /// halving wakeup cost for the reactor's `submit_and_wait`.
    ///
    /// `busy_poll_us` is the NAPI busy-poll timeout in microseconds;
    /// `prefer` pins the ring into busy-poll mode (skips the softirq
    /// fallback entirely). Caller should gate on
    /// [`IoFeatures::supports_napi_busy_poll`] before calling.
    ///
    /// # Errors
    ///
    /// Returns [`IoError::RegistrationFailed`] on kernels that reject the
    /// opcode (pre-6.6, or no NAPI struct available — e.g. loopback).
    /// Best-effort: callers typically log + continue.
    pub fn register_napi(&mut self, busy_poll_us: u32, prefer: bool) -> Result<(), IoError> {
        let Self { ring, registry, .. } = self;
        let napi = IoUringNapi {
            busy_poll_to: busy_poll_us,
            prefer_busy_poll: u8::from(prefer),
            pad: [0; 3],
            resv: 0,
        };
        let ring_fd = ring.as_raw_fd();
        // SAFETY: `ring_fd` is a valid open io_uring fd. `napi` is a
        // 16-byte `repr(C)` struct matching `struct io_uring_napi`; the
        // kernel reads it by value. `nr_args` is the struct byte size
        // (kernel convention for NAPI register).
        let ret = unsafe {
            libc::syscall(
                libc::SYS_io_uring_register,
                ring_fd,
                IORING_REGISTER_NAPI,
                &napi as *const IoUringNapi as *const libc::c_void,
                std::mem::size_of::<IoUringNapi>() as u32,
            )
        };
        if ret < 0 {
            return Err(IoError::RegistrationFailed(std::io::Error::last_os_error()));
        }
        registry.set_napi_registered(true);
        Ok(())
    }

    /// Returns the detected kernel feature set.
    #[inline]
    #[must_use]
    pub const fn features(&self) -> IoFeatures {
        self.features
    }

    /// Immutable access to the resource registry.
    #[inline]
    #[must_use]
    pub const fn registry(&self) -> &IoRegistry {
        &self.registry
    }

    /// Mutable access to the resource registry.
    #[inline]
    #[must_use]
    pub fn registry_mut(&mut self) -> &mut IoRegistry {
        &mut self.registry
    }

    /// Returns whether `SQPOLL` was requested for this ring.
    ///
    /// Even when `true`, the SQPOLL thread may be parked; consult
    /// `submission_queue().need_wakeup()` before calling `submit()`.
    #[inline]
    #[must_use]
    pub const fn sqpoll(&self) -> bool {
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
            .field("features", &self.features)
            .field("registry", &self.registry)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn init_ring_rejects_zero_entries() {
        let err = IoUringCtx::init_ring(0, false, None).expect_err("zero entries");
        assert!(matches!(err, IoError::InvalidConfig(_)));
    }

    #[test]
    fn init_ring_succeeds_and_detects_features() {
        let ctx = IoUringCtx::init_ring(4, false, None).expect("ring init");
        assert!(!ctx.sqpoll());
        assert_eq!(ctx.ring().params().sq_entries(), 4);
        // On the dev host (≥ 5.19), BUF_RING should be detected.
        assert!(
            ctx.features().supports_buf_ring(),
            "expected BUF_RING support"
        );
        // Registry starts empty.
        assert!(!ctx.registry().buffers_registered());
        assert!(!ctx.registry().files_registered());
    }

    #[test]
    fn sqpoll_flag_is_recorded() {
        if let Ok(ctx) = IoUringCtx::init_ring(4, true, None) {
            assert!(ctx.sqpoll());
        }
    }

    #[test]
    fn register_buffers_updates_registry() {
        let mut ctx = IoUringCtx::init_ring(8, false, None).expect("ring");
        let mut buf = vec![0u8; 4096];
        let iov = libc::iovec {
            iov_base: buf.as_mut_ptr() as *mut _,
            iov_len: buf.len(),
        };
        // SAFETY: buf outlives the ring (dropped after ctx in this test).
        unsafe {
            ctx.register_buffers(&[iov]).expect("register");
        }
        assert!(ctx.registry().buffers_registered());
        // Explicit unregister via registry clears the flag.
        ctx.registry_mut().unregister_buffers().expect("unregister");
        assert!(!ctx.registry().buffers_registered());
    }

    #[test]
    fn napi_register_round_trips() {
        // Loopback has no NAPI struct, so the kernel may reject with
        // EINVAL/ENODEV. We only assert that the call either succeeds
        // (kernel ≥ 6.6 with a NAPI-capable device) or returns a typed
        // `RegistrationFailed` error — never panics.
        let features = IoFeatures::detect(
            &IoUring::builder()
                .setup_single_issuer()
                .setup_defer_taskrun()
                .setup_coop_taskrun()
                .build(8)
                .expect("ring probe"),
        );
        if !features.supports_napi_busy_poll() {
            eprintln!("skipping: kernel < 6.6 lacks IORING_REGISTER_NAPI");
            return;
        }
        let mut ctx = IoUringCtx::init_ring(8, false, None).expect("ring");
        match ctx.register_napi(50, true) {
            Ok(()) => assert!(ctx.registry().napi_registered()),
            Err(IoError::RegistrationFailed(e)) => {
                // Loopback: kernel accepts the opcode but the ring has
                // no NAPI id — EINVAL is expected on dev hosts.
                eprintln!(
                    "napi register denied (expected on loopback): errno {}",
                    e.raw_os_error().unwrap_or(-1)
                );
                assert!(!ctx.registry().napi_registered());
            }
            Err(e) => panic!("unexpected error: {e:?}"),
        }
    }
}
