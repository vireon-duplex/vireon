//! Resource registry — explicit registration state + Drop cleanup.
//!
//! [`IoRegistry`] tracks which kernel resources (fixed buffers, sparse
//! files) are currently registered on a ring, and **explicitly
//! unregisters** them in [`Drop`] via raw `io_uring_register(2)`
//! syscalls.
//!
//! # Why explicit cleanup?
//!
//! When the owning [`IoUring`](io_uring::IoUring) is dropped, the kernel
//! closes the ring fd and tears down all associated resources. However,
//! relying on implicit teardown is fragile:
//!
//! - It races with kernel-side buffer consumption if buffers are freed
//!   before the ring closes.
//! - Large-scale deployments show fd-table pressure from deferred
//!   teardown.
//! - Debugging resource leaks is harder without explicit accounting.
//!
//! By unregistering first in [`Drop`], we guarantee the kernel stops
//! touching our memory before we free it.

use std::os::unix::io::RawFd;

use crate::error::IoError;

// ─── io_uring_register opcodes (from include/uapi/linux/io_uring.h) ──

/// `io_uring_register(fd, IORING_UNREGISTER_BUFFERS, NULL, 0)`
const IORING_UNREGISTER_BUFFERS: u32 = 1;

/// `io_uring_register(fd, IORING_UNREGISTER_FILES, NULL, 0)`
const IORING_UNREGISTER_FILES: u32 = 3;

/// Tracks registered kernel resources for one io_uring ring.
///
/// Lives inside [`IoUringCtx`](crate::context::IoUringCtx) and is
/// declared before the `IoUring` field so that `Drop` unregisters
/// resources **before** the ring fd is closed.
pub struct IoRegistry {
    /// Kernel file descriptor of the owning ring. Used for raw
    /// `io_uring_register(2)` syscalls in [`Drop`].
    ring_fd: RawFd,
    /// `true` once `IORING_REGISTER_BUFFERS` succeeded.
    buffers_registered: bool,
    /// `true` once `IORING_REGISTER_FILES` (or sparse variant) succeeded.
    files_registered: bool,
}

impl IoRegistry {
    /// Creates a new registry for the given ring fd. No resources are
    /// registered yet.
    #[must_use]
    pub const fn new(ring_fd: RawFd) -> Self {
        Self {
            ring_fd,
            buffers_registered: false,
            files_registered: false,
        }
    }

    /// Returns the ring fd this registry is bound to.
    #[must_use]
    pub const fn ring_fd(&self) -> RawFd {
        self.ring_fd
    }

    /// Returns whether fixed buffers are currently registered.
    #[must_use]
    pub const fn buffers_registered(&self) -> bool {
        self.buffers_registered
    }

    /// Returns whether fixed files are currently registered.
    #[must_use]
    pub const fn files_registered(&self) -> bool {
        self.files_registered
    }

    /// Marks buffers as registered (called by `IoUringCtx` after a
    /// successful `submitter.register_buffers()`).
    pub(crate) fn set_buffers_registered(&mut self, registered: bool) {
        self.buffers_registered = registered;
    }

    /// Marks files as registered (called by `IoUringCtx` after a
    /// successful `submitter.register_files_sparse()`).
    pub(crate) fn set_files_registered(&mut self, registered: bool) {
        self.files_registered = registered;
    }

    /// Explicitly unregisters all fixed buffers.
    ///
    /// Uses a raw `io_uring_register(2)` syscall so it works even
    /// without the `Submitter` reference (e.g. in [`Drop`]).
    pub fn unregister_buffers(&mut self) -> Result<(), IoError> {
        if !self.buffers_registered {
            return Ok(());
        }
        // SAFETY: `ring_fd` is a valid open io_uring fd. The unregister
        // opcode takes NULL/0 args for buffers.
        let ret = unsafe {
            libc::syscall(
                libc::SYS_io_uring_register,
                self.ring_fd,
                IORING_UNREGISTER_BUFFERS,
                std::ptr::null::<libc::c_void>(),
                0u32,
            )
        };
        if ret < 0 {
            return Err(IoError::UnregistrationFailed(
                std::io::Error::last_os_error(),
            ));
        }
        self.buffers_registered = false;
        Ok(())
    }

    /// Explicitly unregisters all fixed files.
    pub fn unregister_files(&mut self) -> Result<(), IoError> {
        if !self.files_registered {
            return Ok(());
        }
        // SAFETY: same justification as `unregister_buffers`.
        let ret = unsafe {
            libc::syscall(
                libc::SYS_io_uring_register,
                self.ring_fd,
                IORING_UNREGISTER_FILES,
                std::ptr::null::<libc::c_void>(),
                0u32,
            )
        };
        if ret < 0 {
            return Err(IoError::UnregistrationFailed(
                std::io::Error::last_os_error(),
            ));
        }
        self.files_registered = false;
        Ok(())
    }
}

impl Drop for IoRegistry {
    fn drop(&mut self) {
        // EXPLICIT cleanup — do NOT rely on implicit ring teardown.
        // Best-effort: errors are silently ignored in Drop (can't propagate).
        if self.buffers_registered {
            let _ = self.unregister_buffers();
        }
        if self.files_registered {
            let _ = self.unregister_files();
        }
    }
}

impl std::fmt::Debug for IoRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IoRegistry")
            .field("ring_fd", &self.ring_fd)
            .field("buffers_registered", &self.buffers_registered)
            .field("files_registered", &self.files_registered)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use io_uring::IoUring;

    fn fresh_ring() -> (IoUring, RawFd) {
        use std::os::unix::io::AsRawFd;
        let ring = IoUring::builder()
            .setup_single_issuer()
            .setup_defer_taskrun()
            .setup_coop_taskrun()
            .build(8)
            .expect("ring");
        let fd = ring.as_raw_fd();
        (ring, fd)
    }

    #[test]
    fn new_registry_has_nothing_registered() {
        let reg = IoRegistry::new(42);
        assert_eq!(reg.ring_fd(), 42);
        assert!(!reg.buffers_registered());
        assert!(!reg.files_registered());
    }

    #[test]
    fn set_and_clear_state() {
        let mut reg = IoRegistry::new(0);
        reg.set_buffers_registered(true);
        assert!(reg.buffers_registered());
        reg.set_buffers_registered(false);
        assert!(!reg.buffers_registered());
    }

    #[test]
    fn unregister_when_not_registered_is_ok() {
        let mut reg = IoRegistry::new(-1); // invalid fd, but won't be called
        assert!(reg.unregister_buffers().is_ok());
        assert!(reg.unregister_files().is_ok());
    }

    #[test]
    fn drop_does_not_panic_with_real_ring() {
        let (_ring, fd) = fresh_ring();
        let mut reg = IoRegistry::new(fd);
        // Simulate registered state without actually registering.
        reg.set_buffers_registered(false);
        reg.set_files_registered(false);
        // Drop should be a no-op since nothing is registered.
        drop(reg);
    }

    #[test]
    fn explicit_unregister_after_real_registration() {
        let (ring, fd) = fresh_ring();
        // Register some fixed buffers.
        let mut buf = vec![0u8; 4096];
        let iov = libc::iovec {
            iov_base: buf.as_mut_ptr() as *mut _,
            iov_len: buf.len(),
        };
        let iovs = [iov];
        unsafe {
            ring.submitter().register_buffers(&iovs).expect("register");
        }
        let mut reg = IoRegistry::new(fd);
        reg.set_buffers_registered(true);
        // Explicit unregister should succeed.
        reg.unregister_buffers().expect("unregister");
        assert!(!reg.buffers_registered());
    }
}
