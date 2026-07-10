//! Multishot receive path — `IORING_OP_RECVMSG`.
//!
//! [`Receiver`] arms a multishot recv that posts a CQE for every incoming
//! datagram, drawing buffers from the provided-buffer group `bgid`. The
//! `user_data` of each CQE encodes `OpKind::Recv` in the high bits so the
//! [`CompletionDecoder`](crate::completion::CompletionDecoder) can classify
//! it without a lookup.

use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::os::unix::io::RawFd;

use io_uring::IoUring;
use io_uring::opcode::RecvMsgMulti;
use io_uring::types::{Fd, RecvMsgOut};

use crate::error::IoError;
use crate::ops::{OpKind, encode, push_entry};

/// Maximum space the kernel will reserve in each provided buffer for the
/// peer's socket address. `sockaddr_storage` is 128 bytes; we cap at 128
/// for IPv4/IPv6 compatibility.
const PEER_ADDR_LEN: usize = 128;

/// Decoded application frame carried inside a recv'd buffer.
#[derive(Debug, Clone, Copy)]
pub struct ParsedFrame<'a> {
    /// First byte of the payload (see [`crate::common::DataPlane`]).
    pub opcode: u8,
    /// Next 8 bytes, little-endian.
    pub stream_id: u64,
    /// Remaining payload bytes.
    pub payload: &'a [u8],
    /// Source address the kernel decoded from the buffer's name field.
    pub peer_addr: SocketAddr,
}

/// Raw datagram payload + peer address — no application-level header decode.
///
/// Used by upper layers (e.g. QUIC engine) that need the full UDP payload
/// without the 9-byte `(opcode, stream_id)` interpretation that
/// [`ParsedFrame`] applies.
#[derive(Debug, Clone, Copy)]
pub struct DatagramMeta<'a> {
    /// Full UDP datagram payload (no io_uring header).
    pub payload: &'a [u8],
    /// Source address the kernel decoded from the buffer's name field.
    pub peer_addr: SocketAddr,
}

/// Multishot-recv handle. Borrows the socket fd (never closes it).
///
/// Created by [`Socket::receiver`](crate::socket::Socket::receiver). Stores
/// the `msghdr` template needed by both the submit and parse paths.
pub struct Receiver {
    fd: RawFd,
    bgid: u16,
    /// Multishot-recvmsg template: only `msg_namelen` and `msg_controllen`
    /// are consulted by the kernel (at SQE submit time). Kept alive in
    /// `self` so the pointer is stable across re-arming.
    msg_hdr: libc::msghdr,
}

impl Receiver {
    /// Creates a new receiver for the given fd + buffer-group id.
    #[must_use]
    pub fn new(fd: RawFd, bgid: u16) -> Self {
        let msg_hdr = libc::msghdr {
            msg_name: std::ptr::null_mut(),
            msg_namelen: PEER_ADDR_LEN as u32,
            msg_iov: std::ptr::null_mut(),
            msg_iovlen: 0,
            msg_control: std::ptr::null_mut(),
            msg_controllen: 0,
            msg_flags: 0,
        };
        Self { fd, bgid, msg_hdr }
    }

    /// Raw file descriptor (borrowed from the owning [`Socket`](crate::socket::Socket)).
    #[inline]
    #[must_use]
    pub fn raw_fd(&self) -> RawFd {
        self.fd
    }

    /// Buffer-group id used when arming multishot recv.
    #[inline]
    #[must_use]
    pub fn bgid(&self) -> u16 {
        self.bgid
    }

    /// Arms a multishot `RECVMSG` on the socket.
    ///
    /// `id` is the caller-supplied ID encoded into `user_data` for CQE
    /// matching. The kernel will post a CQE for every incoming datagram,
    /// drawing buffers from the provided-buffer group `bgid`. When a CQE
    /// lacks `IORING_CQE_F_MORE`, the multishot has terminated and the
    /// caller must re-arm.
    pub fn arm(&self, ring: &mut IoUring, id: u64) -> Result<(), IoError> {
        let fd = Fd(self.fd);
        let user_data = encode(OpKind::Recv, id);
        // SAFETY: `msg_hdr` lives in `self` for the duration of the call;
        // the kernel reads `msg_namelen`/`msg_controllen` at submit time
        // and does not retain the pointer afterwards.
        let entry = RecvMsgMulti::new(fd, &self.msg_hdr as *const _, self.bgid)
            .build()
            .user_data(user_data);
        push_entry(ring, entry)
    }

    /// Parses a provided buffer into raw payload + peer address — no
    /// application-level header decode.
    ///
    /// Unlike [`parse`](Self::parse), this returns the **full** UDP payload
    /// without stripping the first 9 bytes. Upper layers that interpret the
    /// payload themselves (e.g. QUIC engine feeding to quiche) should use
    /// this method.
    pub fn parse_raw<'a>(&self, raw: &'a [u8]) -> Result<DatagramMeta<'a>, IoError> {
        let (payload, name) = self.split_buffer(raw)?;
        let peer_addr = decode_peer_addr(name);
        Ok(DatagramMeta { payload, peer_addr })
    }

    /// Splits a kernel-filled provided buffer into (payload, name) slices.
    ///
    /// Shared helper for [`parse`](Self::parse) and
    /// [`parse_raw`](Self::parse_raw).
    fn split_buffer<'a>(&self, raw: &'a [u8]) -> Result<(&'a [u8], &'a [u8]), IoError> {
        let out = RecvMsgOut::parse(raw, &self.msg_hdr)
            .map_err(|()| IoError::InvalidConfig("truncated io_uring_recvmsg_out buffer".into()))?;

        let payload_view = out.payload_data();
        let payload_off = payload_view.as_ptr() as usize - raw.as_ptr() as usize;
        let payload_len = payload_view.len();
        let name_view = out.name_data();
        let name_off = name_view.as_ptr() as usize - raw.as_ptr() as usize;
        let name_len = name_view.len();

        let payload = raw
            .get(
                payload_off
                    ..payload_off
                        .checked_add(payload_len)
                        .ok_or_else(|| IoError::InvalidConfig("payload range overflow".into()))?,
            )
            .ok_or_else(|| IoError::InvalidConfig("payload slice out of bounds".into()))?;
        let name = raw
            .get(
                name_off
                    ..name_off
                        .checked_add(name_len)
                        .ok_or_else(|| IoError::InvalidConfig("name range overflow".into()))?,
            )
            .ok_or_else(|| IoError::InvalidConfig("name slice out of bounds".into()))?;

        Ok((payload, name))
    }

    /// Parses a provided buffer filled by the kernel into a [`ParsedFrame`].
    ///
    /// `raw` is the whole buffer the kernel posted (header + name +
    /// control + payload). Uses [`RecvMsgOut::parse`] with `self.msg_hdr`
    /// to slice it, then decodes the first 9 payload bytes as
    /// `(opcode:u8, stream_id:u64_le)`.
    pub fn parse<'a>(&self, raw: &'a [u8]) -> Result<ParsedFrame<'a>, IoError> {
        let (payload, name) = self.split_buffer(raw)?;

        if payload.len() < 9 {
            return Err(IoError::InvalidConfig(format!(
                "payload too short for frame header: {} bytes",
                payload.len()
            )));
        }
        let opcode = payload[0];
        let stream_id = u64::from_le_bytes(
            payload[1..9]
                .try_into()
                .map_err(|_| IoError::InvalidConfig("stream_id slice".into()))?,
        );

        let peer_addr = decode_peer_addr(name);

        Ok(ParsedFrame {
            opcode,
            stream_id,
            payload: &payload[9..],
            peer_addr,
        })
    }
}

impl std::fmt::Debug for Receiver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Receiver")
            .field("fd", &self.fd)
            .field("bgid", &self.bgid)
            .finish_non_exhaustive()
    }
}

/// Best-effort decode of the kernel-reported peer address. Falls back to
/// `0.0.0.0:0` when the name field is empty or not an IPv4 address.
fn decode_peer_addr(name_data: &[u8]) -> SocketAddr {
    if name_data.len() < std::mem::size_of::<libc::sockaddr_in>() {
        return SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0));
    }
    // SAFETY: we just checked the length covers `sockaddr_in`; the bytes
    // are kernel-written and aligned within the provided buffer.
    let sin: libc::sockaddr_in =
        unsafe { std::ptr::read_unaligned(name_data.as_ptr() as *const libc::sockaddr_in) };
    if sin.sin_family != libc::AF_INET as u16 {
        return SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0));
    }
    let port = u16::from_be(sin.sin_port);
    let ip = u32::from_be(sin.sin_addr.s_addr);
    SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::from(ip), port))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fresh_ring() -> IoUring {
        IoUring::builder()
            .setup_single_issuer()
            .setup_defer_taskrun()
            .setup_coop_taskrun()
            .build(16)
            .expect("ring")
    }

    #[test]
    fn arm_pushes_an_sqe() {
        let mut ring = fresh_ring();
        let recv = Receiver::new(0, 1);
        recv.arm(&mut ring, 0xABC).expect("arm");
        assert!(ring.submission().is_empty());
    }

    #[test]
    fn parse_rejects_short_input() {
        let recv = Receiver::new(0, 1);
        let short = [0u8; 4];
        let err = recv.parse(&short).expect_err("short");
        assert!(matches!(err, IoError::InvalidConfig(_)));
    }

    #[test]
    fn decode_peer_addr_handles_empty_and_v4() {
        let empty = decode_peer_addr(&[]);
        assert_eq!(empty.port(), 0);

        let mut buf = [0u8; 16];
        let sin = libc::sockaddr_in {
            sin_family: libc::AF_INET as u16,
            sin_port: 8080u16.to_be(),
            sin_addr: libc::in_addr {
                s_addr: u32::from(Ipv4Addr::new(10, 0, 0, 1)).to_be(),
            },
            sin_zero: [0; 8],
        };
        unsafe {
            std::ptr::write_unaligned(buf.as_mut_ptr() as *mut libc::sockaddr_in, sin);
        }
        let decoded = decode_peer_addr(&buf);
        assert_eq!(decoded.port(), 8080);
        assert_eq!(decoded.ip().to_string(), "10.0.0.1");
    }
}
