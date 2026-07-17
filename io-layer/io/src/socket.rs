//! UDP socket abstraction — owns the fd, spawns [`Receiver`]/[`Sender`].
//!
//! [`Socket`] is the **only** owner of the underlying file descriptor.
//! [`Receiver`] and [`Sender`] borrow the fd (via `RawFd` copy) but never
//! close it — the [`Socket`] drop closes the fd when the reactor tears down.

use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::os::unix::io::{AsRawFd, RawFd};

use socket2::{Domain, Protocol, Socket as Sock, Type};

use crate::error::IoError;
use crate::ops::{Receiver, Sender};

/// `SO_PREFER_BUSY_POLL` (kernel 5.11+) — pins the socket into busy-poll
/// mode so the kernel skips the softirq wakeup path entirely. Not in all
/// `libc` crate versions, so defined here by its protocol value.
const SO_PREFER_BUSY_POLL: libc::c_int = 74;

/// Default `SO_BUSY_POLL` window in microseconds. The kernel busy-polls the
/// NIC's NAPI struct for this long before falling back to a softirq. Mirrors
/// the kernel-nic "Balanced" / "LowLatency" preset (50 µs).
const DEFAULT_BUSY_POLL_US: libc::c_int = 50;

/// Owned UDP socket with `SO_REUSEPORT` binding.
///
/// Created via [`Socket::bind`] or [`Socket::bind_with_reuseport`]. The
/// socket is non-blocking (io_uring requirement) and bound to
/// `0.0.0.0:{port}`. Port `0` requests an ephemeral port from the kernel.
pub struct Socket {
    sock: Sock,
}

impl Socket {
    /// Binds a UDP socket on `0.0.0.0:{port}` with `SO_REUSEADDR` +
    /// `SO_REUSEPORT`. No CPU affinity or busy-poll options are applied —
    /// use [`bind_with_reuseport`](Self::bind_with_reuseport) for the
    /// per-core production path.
    pub fn bind(port: u16) -> Result<Self, IoError> {
        Self::setup(port, None)
    }

    /// Binds a UDP socket on `0.0.0.0:{port}` with `SO_REUSEADDR` +
    /// `SO_REUSEPORT`, and — when `cpu` is provided — applies the per-core
    /// socket options that let the kernel steer packets and busy-poll
    /// without softirq wakeups:
    ///
    /// - `SO_INCOMING_CPU` — hints the kernel to deliver packets received on
    ///   the RSS queue owned by `cpu` to this socket.
    /// - `SO_BUSY_POLL` — busy-poll the NIC NAPI struct for
    ///   [`DEFAULT_BUSY_POLL_US`] µs before raising a softirq.
    /// - `SO_PREFER_BUSY_POLL` — pin the socket into busy-poll mode so the
    ///   softirq path is skipped entirely.
    ///
    /// All three are best-effort via raw `setsockopt(2)` and silently
    /// ignored on kernels/devices that don't support them (loopback has no
    /// NAPI struct; old kernels lack `SO_PREFER_BUSY_POLL`).
    pub fn bind_with_reuseport(port: u16, cpu: u32) -> Result<Self, IoError> {
        Self::setup(port, Some(cpu))
    }

    fn setup(port: u16, cpu: Option<u32>) -> Result<Self, IoError> {
        let sock =
            Sock::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP)).map_err(IoError::Socket)?;
        sock.set_reuse_address(true).map_err(IoError::Socket)?;
        // SO_REUSEPORT — kernel hashes (src_ip, src_port) across sockets.
        sock.set_reuse_port(true).map_err(IoError::Socket)?;
        sock.set_nonblocking(true).map_err(IoError::Socket)?;

        if let Some(cpu) = cpu {
            apply_per_core_options(sock.as_raw_fd(), cpu);
        }

        let addr = SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, port);
        sock.bind(&socket2::SockAddr::from(SocketAddr::V4(addr)))
            .map_err(IoError::Socket)?;

        Ok(Self { sock })
    }

    /// Raw file descriptor for io_uring SQEs.
    #[inline]
    #[must_use]
    pub fn raw_fd(&self) -> RawFd {
        <Sock as std::os::unix::io::AsRawFd>::as_raw_fd(&self.sock)
    }

    /// Returns the bound local address.
    pub fn local_addr(&self) -> Result<SocketAddr, IoError> {
        self.sock
            .local_addr()
            .map_err(IoError::Socket)
            .and_then(|sa| {
                sa.as_socket()
                    .ok_or_else(|| IoError::Unsupported("non-IPv4 socket address".into()))
            })
    }

    /// Creates a [`Receiver`] that reads from this socket into the
    /// provided-buffer group `bgid`. The receiver borrows the fd but
    /// never closes it.
    #[must_use]
    pub fn receiver(&self, bgid: u16) -> Receiver {
        Receiver::new(self.raw_fd(), bgid)
    }

    /// Creates a [`Sender`] that writes to this socket via `SEND_ZC`.
    #[must_use]
    pub fn sender(&self) -> Sender {
        Sender::new(self.raw_fd())
    }
}

impl std::fmt::Debug for Socket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Socket")
            .field("fd", &self.raw_fd())
            .finish_non_exhaustive()
    }
}

/// Applies the per-core socket options (`SO_INCOMING_CPU`, `SO_BUSY_POLL`,
/// `SO_PREFER_BUSY_POLL`) to `fd`. Best-effort: every failure is logged at
/// debug level and ignored, matching the kernel-nic policy. Loopback and
/// older kernels reject these with `EINVAL`/`ENOPROTOOPT`; the socket still
/// works via the normal softirq path.
fn apply_per_core_options(fd: RawFd, cpu: u32) {
    // SO_INCOMING_CPU — steer packets from the RSS queue owned by `cpu`.
    setsockopt_int(fd, libc::SOL_SOCKET, libc::SO_INCOMING_CPU, cpu as libc::c_int);
    // SO_BUSY_POLL — busy-poll NAPI for DEFAULT_BUSY_POLL_US µs.
    setsockopt_int(
        fd,
        libc::SOL_SOCKET,
        libc::SO_BUSY_POLL,
        DEFAULT_BUSY_POLL_US,
    );
    // SO_PREFER_BUSY_POLL — pin the socket into busy-poll mode.
    setsockopt_int(fd, libc::SOL_UDP, SO_PREFER_BUSY_POLL, 1);
}

/// Sets an integer socket option. Returns the kernel errno on failure
/// (caller decides whether to log or propagate). `value` is a raw `c_int`
/// so callers don't have to cast at every site.
fn setsockopt_int(fd: RawFd, level: libc::c_int, name: libc::c_int, value: libc::c_int) {
    // SAFETY: `value` is a valid `c_int` by value; `setsockopt` copies it.
    // `fd` is a valid open socket owned by the caller.
    let rc = unsafe {
        libc::setsockopt(
            fd,
            level,
            name,
            &value as *const libc::c_int as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        )
    };
    if rc != 0 {
        let errno = unsafe { *libc::__errno_location() };
        tracing::debug!(
            fd,
            level,
            opt = name,
            errno,
            "setsockopt ignored (unsupported on this kernel/device)"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bind_on_ephemeral_port_succeeds() {
        let sock = Socket::bind(0).expect("bind");
        assert!(sock.raw_fd() > 0);
    }

    #[test]
    fn bind_with_reuseport_succeeds() {
        let sock = Socket::bind_with_reuseport(0, 2).expect("bind");
        assert!(sock.raw_fd() > 0);
    }

    #[test]
    fn local_addr_returns_bound_port() {
        let sock = Socket::bind(0).expect("bind");
        let addr = sock.local_addr().expect("local_addr");
        assert!(addr.port() > 0);
    }

    #[test]
    fn receiver_and_sender_inherit_fd() {
        let sock = Socket::bind(0).expect("bind");
        let recv = sock.receiver(7);
        let send = sock.sender();
        assert_eq!(recv.raw_fd(), sock.raw_fd());
        assert_eq!(send.raw_fd(), sock.raw_fd());
        assert_eq!(recv.bgid(), 7);
    }
}
