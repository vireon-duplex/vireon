//! Process-wide registry of worker socket file descriptors.
//!
//! Solves the ghost-socket problem: when a worker thread is killed mid-flight
//! (panic → abort, SIGSEGV, second SIGINT calling `process::exit`), the
//! `Drop` chain that normally closes the UDP fd never runs. The kernel keeps
//! the SO_REUSEPORT socket alive on its bound port, and subsequent test runs
//! route handshakes to the dead socket — leaking ~25–100% of connections
//! depending on how many sockets remain.
//!
//! This registry provides an async-signal-safe last-resort cleanup path:
//! - Each worker calls [`SocketRegistry::register`] after `bind_with_reuseport`.
//! - The signal handler / panic hook calls [`SocketRegistry::close_all`],
//!   which iterates slots and `libc::close`s every fd.
//!
//! Design constraints:
//! - **No `Mutex`** — signal handlers cannot acquire it.
//! - **No allocation** — `Vec::push` is not async-signal-safe.
//! - **Const-constructible** — the static is valid from program start, so a
//!   signal arriving during another thread's init still sees a consistent
//!   state.
//!
//! The fixed capacity (128) is far above any realistic worker count. Overflow
//! is logged via `eprintln!` (signal-safe) and the fd is left unregistered —
//! it will still be closed by its owner's `Drop` on a clean exit, just not by
//! `close_all` on a crash.

use std::os::fd::RawFd;
use std::sync::atomic::{AtomicI64, Ordering};

/// Maximum number of fds the registry can hold. One slot per worker; 128 is
/// generous (the largest test configurations use ≤64 workers).
const MAX_FDS: usize = 128;

/// Sentinel stored in empty slots. Real Unix fds are ≥0, so any negative
/// value is unambiguous.
const EMPTY: i64 = -1;

/// Process-global registry instance.
///
/// # Safety
/// All operations are lock-free atomic reads/writes. `register` is safe to
/// call from any thread. `close_all` is safe to call from a signal handler.
pub static REGISTRY: SocketRegistry = SocketRegistry::new();

/// Fixed-capacity lock-free registry of socket fds.
pub struct SocketRegistry {
    entries: [AtomicI64; MAX_FDS],
}

impl SocketRegistry {
    /// Construct an empty registry. Every slot starts at `EMPTY`.
    #[must_use]
    pub const fn new() -> Self {
        // `const { … }; N` repeat syntax is stable since Rust 1.79. Each
        // slot is independently initialised to `EMPTY`.
        Self {
            entries: [const { AtomicI64::new(EMPTY) }; MAX_FDS],
        }
    }

    /// Register a socket fd so it can be force-closed on a crash exit.
    ///
    /// Walks the slot array and stores `fd` in the first `EMPTY` slot via
    /// CAS. Safe to call from any thread; safe to call multiple times with
    /// the same fd (each call adds a new slot — callers should register
    /// each fd exactly once).
    ///
    /// On overflow (more than `MAX_FDS` live fds), logs to stderr and
    /// returns without storing. The fd is still owned by its `Socket` and
    /// will close normally on a graceful exit.
    pub fn register(&self, fd: RawFd) {
        let fd_i = fd as i64;
        for slot in &self.entries {
            if slot
                .compare_exchange(EMPTY, fd_i, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                return;
            }
        }
        // SAFETY: eprintln! is not strictly async-signal-safe but this path
        // runs from a normal worker thread, not a signal handler. Fall back
        // to a no-op rather than aborting.
        eprintln!(
            "[socket_registry] WARN: MAX_FDS={MAX_FDS} exceeded, fd={fd} not registered — close_all will not close it on crash"
        );
    }

    /// Close every registered fd, then empty all slots.
    ///
    /// For each fd, calls `shutdown(fd, SHUT_RDWR)` before `close(fd)`.
    /// The `shutdown` call is critical for sockets with pending io_uring
    /// multishot RECV operations: it forces the kernel to terminate those
    /// operations and release the socket inode reference immediately.
    /// Without `shutdown`, the kernel defers io_uring cleanup for ~60s
    /// after `close`, leaving ghost UDP sockets that block the port.
    ///
    /// For non-socket fds (e.g. io_uring ring fds), `shutdown` returns
    /// `ENOTSOCK` which is ignored — `close` then proceeds normally.
    ///
    /// Both `shutdown(2)` and `close(2)` are async-signal-safe per POSIX.
    /// Idempotent: re-running finds every slot at `EMPTY` and does nothing.
    pub fn close_all(&self) {
        for slot in &self.entries {
            // Acquire the fd exclusively; concurrent close_all calls
            // observe EMPTY and skip.
            let fd = slot.swap(EMPTY, Ordering::AcqRel);
            if fd >= 0 {
                // SAFETY: `shutdown(2)` and `close(2)` are async-signal-safe.
                // EBADF/ENOTSOCK on invalid/non-socket fds is harmless.
                unsafe {
                    libc::shutdown(fd as RawFd, libc::SHUT_RDWR);
                    libc::close(fd as RawFd);
                }
            }
        }
    }

    /// Number of fds currently registered (diagnostic only).
    #[cfg(test)]
    pub fn count(&self) -> usize {
        self.entries
            .iter()
            .filter(|s| s.load(Ordering::Acquire) != EMPTY)
            .count()
    }
}

impl Default for SocketRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_and_close_all() {
        let reg = SocketRegistry::new();
        // Use eventfd fds as disposable test fds (cheap to create, no network).
        let fds: Vec<RawFd> = (0..4)
            .map(|_| {
                let fd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC) };
                assert!(fd >= 0, "eventfd failed");
                fd
            })
            .collect();
        for &fd in &fds {
            reg.register(fd);
        }
        assert_eq!(reg.count(), 4);
        reg.close_all();
        assert_eq!(reg.count(), 0);
        // All fds should now be invalid — close returns EBADF.
        for &fd in &fds {
            let rc = unsafe { libc::close(fd) };
            assert_eq!(rc, -1);
            assert_eq!(unsafe { *libc::__errno_location() }, libc::EBADF);
        }
    }

    #[test]
    fn close_all_is_idempotent() {
        let reg = SocketRegistry::new();
        let fd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC) };
        assert!(fd >= 0);
        reg.register(fd);
        reg.close_all();
        reg.close_all(); // No-op on second call
        reg.close_all(); // And again
        assert_eq!(reg.count(), 0);
    }
}
