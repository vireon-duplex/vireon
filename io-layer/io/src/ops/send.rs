//! Zero-copy send path — `IORING_OP_SEND_ZC`.
//!
//! [`Sender`] queues sends that reference registered fixed buffers by
//! index. The kernel produces **two** CQEs per send:
//!
//! 1. **Send CQE** — `res >= 0` means the kernel accepted the send; the
//!    buffer is still "in flight" (NIC DMA pending).
//! 2. **Notification CQE** — `IORING_CQE_F_NOTIF` flag set; only now may
//!    the buffer be reused.
//!
//! Both CQEs carry `user_data = encode(OpKind::Send, id)` so the
//! [`CompletionDecoder`](crate::completion::CompletionDecoder) classifies
//! them identically and distinguishes via the `NOTIF` flag.

use std::net::SocketAddr;
use std::os::unix::io::RawFd;

use io_uring::IoUring;
use io_uring::opcode::SendZc;
use io_uring::types::Fd;

use crate::error::IoError;
use crate::ops::{OpKind, encode, push_entry};

/// Zero-copy send handle. Borrows the socket fd (never closes it).
///
/// Created by [`Socket::sender`](crate::socket::Socket::sender).
pub struct Sender {
    fd: RawFd,
}

impl Sender {
    /// Creates a new sender for the given fd.
    #[must_use]
    pub fn new(fd: RawFd) -> Self {
        Self { fd }
    }

    /// Raw file descriptor (borrowed from the owning [`Socket`](crate::socket::Socket)).
    #[inline]
    #[must_use]
    pub fn raw_fd(&self) -> RawFd {
        self.fd
    }

    /// Queues a zero-copy send of `len` bytes from registered fixed
    /// buffer `buf_index`.
    ///
    /// The caller must ensure:
    /// 1. `buf_index` was registered via
    ///    [`IoUringCtx::register_buffers`](crate::context::IoUringCtx::register_buffers).
    /// 2. The buffer contains at least `len` valid bytes starting at the
    ///    region's base.
    /// 3. The caller does **not** recycle the buffer until the matching
    ///    notification CQE arrives.
    ///
    /// `id` is encoded into `user_data` for CQE matching.
    pub fn send_zc(
        &self,
        ring: &mut IoUring,
        buf_index: u16,
        len: u32,
        dest: SocketAddr,
        id: u64,
    ) -> Result<(), IoError> {
        let fd = Fd(self.fd);
        let user_data = encode(OpKind::Send, id);
        match dest {
            SocketAddr::V4(v4) => {
                // Stack-local sockaddr_in; valid through the `submit()`
                // call inside `push_entry` (kernel copies at submit time).
                let sin = libc::sockaddr_in {
                    sin_family: libc::AF_INET as u16,
                    sin_port: v4.port().to_be(),
                    sin_addr: libc::in_addr {
                        s_addr: u32::from(*v4.ip()).to_be(),
                    },
                    sin_zero: [0; 8],
                };
                let entry = SendZc::new(fd, std::ptr::null(), len)
                    .buf_index(Some(buf_index))
                    .dest_addr(&sin as *const _ as *const libc::sockaddr)
                    .dest_addr_len(std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t)
                    .build()
                    .user_data(user_data);
                push_entry(ring, entry)
            }
            SocketAddr::V6(_) => Err(IoError::Unsupported(
                "IPv6 send path not implemented; use IPv4".into(),
            )),
        }
    }
}

impl std::fmt::Debug for Sender {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Sender").field("fd", &self.fd).finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, SocketAddrV4};

    fn fresh_ring() -> IoUring {
        IoUring::builder()
            .setup_single_issuer()
            .setup_defer_taskrun()
            .setup_coop_taskrun()
            .build(16)
            .expect("ring")
    }

    #[test]
    fn send_zc_to_ipv4_pushes_sqe() {
        let mut ring = fresh_ring();
        let send = Sender::new(0);
        let dest = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(1, 2, 3, 4), 99));
        // The kernel will fail with -EBADF or similar on submit since we
        // haven't registered fixed buffers; we only assert the SQE-build
        // path doesn't panic.
        let _ = send.send_zc(&mut ring, 0, 4, dest, 0x123);
    }

    #[test]
    fn send_zc_to_ipv6_is_unsupported() {
        let mut ring = fresh_ring();
        let send = Sender::new(0);
        let dest = SocketAddr::new(std::net::Ipv6Addr::LOCALHOST.into(), 80);
        let err = send
            .send_zc(&mut ring, 0, 4, dest, 0x123)
            .expect_err("ipv6");
        assert!(matches!(err, IoError::Unsupported(_)));
    }
}
