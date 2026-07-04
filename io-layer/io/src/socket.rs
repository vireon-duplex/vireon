//! UDP socket abstraction — owns the fd, spawns [`Receiver`]/[`Sender`].
//!
//! [`Socket`] is the **only** owner of the underlying file descriptor.
//! [`Receiver`] and [`Sender`] borrow the fd (via `RawFd` copy) but never
//! close it — the [`Socket`] drop closes the fd when the reactor tears down.

use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::os::unix::io::RawFd;

use socket2::{Domain, Protocol, Socket as Sock, Type};

use crate::error::IoError;
use crate::ops::{Receiver, Sender};

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
    /// `SO_REUSEPORT`.
    pub fn bind(port: u16) -> Result<Self, IoError> {
        Self::setup(port)
    }

    /// Like [`Socket::bind`]; `cpu` is recorded for future eBPF-based flow
    /// steering (currently unused). `SO_REUSEPORT` is enabled; the kernel's
    /// default hash steers flows across sockets.
    pub fn bind_with_reuseport(port: u16, _cpu: u32) -> Result<Self, IoError> {
        Self::setup(port)
    }

    fn setup(port: u16) -> Result<Self, IoError> {
        let sock =
            Sock::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP)).map_err(IoError::Socket)?;
        sock.set_reuse_address(true).map_err(IoError::Socket)?;
        // SO_REUSEPORT — kernel hashes (src_ip, src_port) across sockets.
        sock.set_reuse_port(true).map_err(IoError::Socket)?;
        sock.set_nonblocking(true).map_err(IoError::Socket)?;

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
