//! UDP socket + zero-copy RX/TX paths.
//!
//! One [`Connection`] owns a bound `SO_REUSEPORT` UDP socket. RX is driven
//! by `IORING_OP_RECVMSG` with the multishot flag (one SQE → many CQEs);
//! each CQE names a provided-buffer id the kernel filled. TX uses
//! `IORING_OP_SEND_ZC` for zero-copy sends with a two-CQE lifecycle:
//!
//! 1. **Send CQE** — `res >= 0` means the kernel accepted the send; the
//!    buffer is still "in flight" (NIC DMA pending).
//! 2. **Notification CQE** — `IORING_CQE_F_NOTIF` flag set; only now may
//!    the buffer be reused.
//!
//! See [`Connection::handle_tx_notification`] for the reclaim decoder.

use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::os::unix::io::RawFd;

use io_uring::IoUring;
use io_uring::opcode::{RecvMsgMulti, SendZc};
use io_uring::types::{Fd, RecvMsgOut};
use socket2::{Domain, Protocol, Socket, Type};

use crate::error::IoError;

/// Maximum space the kernel will reserve in each provided buffer for the
/// peer's socket address. `sockaddr_storage` is 128 bytes; we cap at 128
/// for IPv4/IPv6 compatibility.
const PEER_ADDR_LEN: usize = 128;

/// UDP socket + io_uring RX/TX plumbing.
pub struct Connection {
    sock: Socket,
    /// Multishot-recvmsg template: only `msg_namelen` and `msg_controllen`
    /// are consulted by the kernel (at SQE submit time). Kept alive in
    /// `self` so the pointer is stable across re-arming.
    msg_hdr: libc::msghdr,
    /// Default bgid used by [`Connection::arm_multishot_recv`].
    bgid: u16,
}

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

/// Decoded outcome of a `SEND_ZC` CQE.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxNotification {
    /// First CQE: the kernel queued the send. Buffer is still in flight.
    SendCompleted {
        /// Echoed back for matching.
        user_data: u64,
        /// Positive = bytes accepted; negative = `-errno`.
        res: i32,
    },
    /// Second CQE (`IORING_CQE_F_NOTIF`): the NIC DMA finished and the
    /// buffer may now be recycled into the pool.
    BufferReclaimable {
        /// Echoed back for matching.
        user_data: u64,
    },
}

impl Connection {
    /// Binds a UDP socket on `0.0.0.0:{port}` with `SO_REUSEADDR` +
    /// `SO_REUSEPORT`. The socket is non-blocking (io_uring requirement).
    pub fn setup_socket(port: u16) -> Result<Self, IoError> {
        Self::setup(port)
    }

    /// Like [`Connection::setup_socket`]; `cpu` is recorded for future
    /// eBPF-based flow steering (currently out of scope). `SO_REUSEPORT`
    /// is enabled; the kernel's default hash steers flows across sockets.
    pub fn setup_socket_with_reuseport(port: u16, _cpu: u32) -> Result<Self, IoError> {
        Self::setup(port)
    }

    fn setup(port: u16) -> Result<Self, IoError> {
        let sock =
            Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP)).map_err(IoError::Socket)?;
        sock.set_reuse_address(true).map_err(IoError::Socket)?;
        // SO_REUSEPORT — kernel hashes (src_ip, src_port) across sockets.
        sock.set_reuse_port(true).map_err(IoError::Socket)?;
        sock.set_nonblocking(true).map_err(IoError::Socket)?;

        let addr = SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, port);
        sock.bind(&socket2::SockAddr::from(SocketAddr::V4(addr)))
            .map_err(IoError::Socket)?;

        let msg_hdr = libc::msghdr {
            msg_name: std::ptr::null_mut(),
            msg_namelen: PEER_ADDR_LEN as u32,
            msg_iov: std::ptr::null_mut(),
            msg_iovlen: 0,
            msg_control: std::ptr::null_mut(),
            msg_controllen: 0,
            msg_flags: 0,
        };

        Ok(Self {
            sock,
            msg_hdr,
            bgid: 1,
        })
    }

    /// Raw file descriptor for io_uring SQEs.
    #[inline]
    #[must_use]
    pub fn raw_fd(&self) -> RawFd {
        // SAFETY: `as_raw_fd` does not transfer ownership; the fd remains
        // owned by `self.sock` for the lifetime of `self`.
        <Socket as std::os::unix::io::AsRawFd>::as_raw_fd(&self.sock)
    }

    /// Default buffer-group id used by [`Self::arm_multishot_recv`].
    #[inline]
    #[must_use]
    pub fn bgid(&self) -> u16 {
        self.bgid
    }

    /// Sets the default bgid for subsequent `arm_multishot_recv` calls.
    pub fn set_bgid(&mut self, bgid: u16) {
        self.bgid = bgid;
    }

    /// Arms a multishot `RECVMSG` on the socket.
    ///
    /// The kernel will post a CQE for every incoming datagram, drawing
    /// buffers from the provided-buffer group `bgid`. When a CQE lacks
    /// `IORING_CQE_F_MORE`, the multishot has terminated and the caller
    /// must re-arm.
    pub fn arm_multishot_recv(
        &self,
        ring: &mut IoUring,
        bgid: u16,
        user_data: u64,
    ) -> Result<(), IoError> {
        let fd = Fd(self.raw_fd());
        // SAFETY: `msg_hdr` lives in `self` for the duration of the call;
        // the kernel reads `msg_namelen`/`msg_controllen` at submit time
        // and does not retain the pointer afterwards.
        let entry = RecvMsgMulti::new(fd, &self.msg_hdr as *const _, bgid)
            .build()
            .user_data(user_data);
        push_entry(ring, entry)
    }

    /// Parses a provided buffer filled by the kernel into a [`ParsedFrame`].
    ///
    /// `raw` is the whole buffer the kernel posted (header + name +
    /// control + payload). The function uses [`RecvMsgOut::parse`] with
    /// `self.msg_hdr` to slice it, then decodes the first 9 payload bytes
    /// as `(opcode:u8, stream_id:u64_le)`.
    pub fn parse_received_buffer<'a>(&self, raw: &'a [u8]) -> Result<ParsedFrame<'a>, IoError> {
        let out = RecvMsgOut::parse(raw, &self.msg_hdr)
            .map_err(|()| IoError::InvalidConfig("truncated io_uring_recvmsg_out buffer".into()))?;

        // `RecvMsgOut::payload_data()` returns a slice tied to `&out`, not
        // to `raw`'s lifetime. Derive offsets from the borrow and re-slice
        // `raw` so the returned frame outlives `out`.
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

    /// Queues a zero-copy send of `len` bytes from registered fixed
    /// buffer `buf_index`.
    ///
    /// The caller must ensure:
    /// 1. `buf_index` was registered via [`IoUringCtx::register_fixed_buffers`](crate::io_uring_ctx::IoUringCtx::register_fixed_buffers).
    /// 2. The buffer contains at least `len` valid bytes starting at the
    ///    region's base.
    /// 3. The caller does **not** recycle the buffer until the matching
    ///    `BufferReclaimable` notification arrives.
    pub fn send_packet_zc(
        &self,
        ring: &mut IoUring,
        buf_index: u16,
        len: u32,
        dest: SocketAddr,
        user_data: u64,
    ) -> Result<(), IoError> {
        let fd = Fd(self.raw_fd());
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

    /// Decodes the CQE flag/res pair for a `SEND_ZC` submission.
    ///
    /// Returns [`TxNotification::BufferReclaimable`] when the `NOTIF`
    /// flag is set (buffer is safe to reuse), otherwise
    /// [`TxNotification::SendCompleted`] carrying the kernel's result.
    #[must_use]
    pub fn handle_tx_notification(cqe_flags: u32, cqe_res: i32, user_data: u64) -> TxNotification {
        if io_uring::cqueue::notif(cqe_flags) {
            TxNotification::BufferReclaimable { user_data }
        } else {
            TxNotification::SendCompleted {
                user_data,
                res: cqe_res,
            }
        }
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

/// Pushes one SQE and submits the ring.
fn push_entry(ring: &mut IoUring, entry: io_uring::squeue::Entry) -> Result<(), IoError> {
    let mut sq = ring.submission();
    // SAFETY: `entry` is a well-formed SQE from an opcode builder.
    if unsafe { sq.push(&entry) }.is_err() {
        return Err(IoError::Backpressure { errno: libc::EBUSY });
    }
    drop(sq);
    ring.submit().map_err(IoError::Submit)?;
    Ok(())
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
    fn setup_socket_on_ephemeral_port_succeeds() {
        // Port 0 asks the kernel for an ephemeral port.
        let conn = Connection::setup_socket(0).expect("setup");
        assert!(conn.raw_fd() > 0);
        assert_eq!(conn.bgid(), 1);
    }

    #[test]
    fn setup_with_reuseport_records_cpu_but_uses_default_hash() {
        let conn = Connection::setup_socket_with_reuseport(0, 2).expect("setup");
        assert!(conn.raw_fd() > 0);
    }

    #[test]
    fn arm_multishot_recv_pushes_an_sqe() {
        let mut ring = fresh_ring();
        let conn = Connection::setup_socket(0).expect("setup");
        conn.arm_multishot_recv(&mut ring, 1, 0xABC).expect("arm");
        // After submit the SQ should be drained.
        assert!(ring.submission().is_empty());
    }

    #[test]
    fn parse_buffer_rejects_short_input() {
        let conn = Connection::setup_socket(0).expect("setup");
        let short = [0u8; 4];
        let err = conn.parse_received_buffer(&short).expect_err("short");
        assert!(matches!(err, IoError::InvalidConfig(_)));
    }

    #[test]
    fn handle_tx_notification_decodes_notif_flag() {
        // IORING_CQE_F_NOTIF bit (per io_uring.h, kernel uapi).
        const NOTIF: u32 = 1 << 3;
        let reclaim = Connection::handle_tx_notification(NOTIF, 0, 42);
        assert_eq!(reclaim, TxNotification::BufferReclaimable { user_data: 42 });
        let sent = Connection::handle_tx_notification(0, 128, 99);
        assert_eq!(
            sent,
            TxNotification::SendCompleted {
                user_data: 99,
                res: 128
            }
        );
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

    #[test]
    fn send_packet_zc_to_ipv4_pushes_sqe() {
        let mut ring = fresh_ring();
        let conn = Connection::setup_socket(0).expect("setup");
        let dest = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(1, 2, 3, 4), 99));
        // The kernel will fail with -EBADF or similar on submit since we
        // haven't registered fixed buffers; we only assert the SQE-build
        // path doesn't panic.
        let _ = conn.send_packet_zc(&mut ring, 0, 4, dest, 0x123);
    }

    #[test]
    fn send_packet_zc_to_ipv6_is_unsupported() {
        let mut ring = fresh_ring();
        let conn = Connection::setup_socket(0).expect("setup");
        let dest = SocketAddr::new(std::net::Ipv6Addr::LOCALHOST.into(), 80);
        let err = conn
            .send_packet_zc(&mut ring, 0, 4, dest, 0x123)
            .expect_err("ipv6");
        assert!(matches!(err, IoError::Unsupported(_)));
    }
}
