//! UDP socket abstraction — owns the fd, spawns [`Receiver`]/[`Sender`].
//!
//! [`Socket`] is the **only** owner of the underlying file descriptor.
//! [`Receiver`] and [`Sender`] borrow the fd (via `RawFd` copy) but never
//! close it — the [`Socket`] drop closes the fd when the reactor tears
//! down.
//!
//! # Policy ownership
//!
//! All socket-option policy (`SO_REUSEPORT`, `SO_INCOMING_CPU`,
//! `SO_BUSY_POLL`, `SO_PREFER_BUSY_POLL`, …) lives in
//! `kernel_nic::SocketBuilder`. The IO Layer's [`Socket::bind_with_reuseport`]
//! is a thin shim that builds a [`kernel_nic::SocketSpec`], delegates to
//! `SocketBuilder`, then reclaims the configured fd for io_uring use.

use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};

use socket2::{Domain, Protocol, Socket as Sock, Type};

use crate::error::IoError;
use crate::ops::{Receiver, Sender};

/// Owned UDP socket with `SO_REUSEPORT` binding.
///
/// Created via [`Socket::bind`] (minimal dev/test path) or
/// [`Socket::bind_with_reuseport`] (production per-core path that applies
/// busy-poll + `SO_INCOMING_CPU` policy via `kernel_nic::SocketBuilder`).
/// The socket is non-blocking (io_uring requirement) and bound to
/// `0.0.0.0:{port}`. Port `0` requests an ephemeral port from the kernel.
pub struct Socket {
    sock: Sock,
}

impl Socket {
    /// Binds a minimal UDP socket on `0.0.0.0:{port}` with `SO_REUSEADDR`
    /// and `SO_REUSEPORT`. No CPU affinity or busy-poll options are applied.
    /// See [`bind_with_reuseport`](Self::bind_with_reuseport) for the
    /// per-core production path.
    pub fn bind(port: u16) -> Result<Self, IoError> {
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

    /// Binds a per-core UDP socket via `kernel_nic::SocketBuilder`, which
    /// is the single source of truth for socket-option policy. The
    /// resulting socket has `SO_REUSEADDR` + `SO_REUSEPORT` plus — when
    /// the kernel/device supports them and `busy_poll` requests them —
    /// the per-core options that let the kernel steer packets and
    /// busy-poll without softirq wakeups:
    ///
    /// - `SO_INCOMING_CPU` — steer packets from the RSS queue owned by
    ///   `cpu` to this socket. **Conditionally applied**: only set when
    ///   `features.supports_reuseport_ebpf()` reports the eBPF steering
    ///   capability as present. Without eBPF, `SO_INCOMING_CPU` causes
    ///   the kernel to pick sockets by the CPU running the RX softirq,
    ///   which on loopback (no RSS) varies per packet and silently
    ///   misroutes packets to workers that do not own the QUIC
    ///   connection — collapsing multi-worker throughput by ~1000x.
    /// - `SO_BUSY_POLL` — busy-poll the NIC NAPI struct for
    ///   `busy_poll.mode.us()` µs before raising a softirq.
    /// - `SO_PREFER_BUSY_POLL` — pin the socket into busy-poll mode so
    ///   the softirq path is skipped entirely.
    ///
    /// `features` is a snapshot from `kernel_nic::KernelFeatures::detect()`
    /// taken once at reactor startup; passing it in avoids re-probing the
    /// kernel on every socket bind. `busy_poll` carries both the mode
    /// (`Off`/`Static`/`Latency`/`Throughput`/`Adaptive`) and the
    /// `SO_PREFER_BUSY_POLL` capability flag.
    ///
    /// All options are best-effort: `kernel_nic::SocketBuilder` silently
    /// skips opts that the kernel rejects (loopback has no NAPI struct;
    /// old kernels lack `SO_PREFER_BUSY_POLL`).
    pub fn bind_with_reuseport(
        port: u16,
        cpu: u32,
        features: kernel_nic::KernelFeatures,
        busy_poll: kernel_nic::BusyPollPolicy,
    ) -> Result<Self, IoError> {
        let mut spec = kernel_nic::SocketSpec::for_reuseport(port, cpu, /* group_id */ 0);
        spec.busy_poll_us = busy_poll.mode.us();
        spec.prefer_busy_poll = busy_poll.should_prefer();
        // SO_INCOMING_CPU is a kernel hint that on its own steers packets
        // by the CPU running the RX softirq. On a loopback or
        // non-RSS-configured NIC, that CPU varies per packet and the
        // resulting socket pick can land on a worker that does not own
        // the QUIC connection — silently dropping it. The opt is only
        // safe when an eBPF `SK_REUSEPORT` program is going to be
        // attached, because the program routes by QUIC DCID and ignores
        // `SO_INCOMING_CPU`. Skip the hint when the probe in
        // `KernelFeatures::detect` reported the capability as missing.
        if !features.supports_reuseport_ebpf() {
            spec.incoming_cpu = None;
        }

        let builder = kernel_nic::SocketBuilder::new(spec, features);
        let configured = builder.build()?;
        let fd = configured.into_raw_fd();
        // SAFETY: `ConfiguredSocket::into_raw_fd` transferred exclusive fd
        // ownership to us (it `forget`s itself). Wrapping in `OwnedFd`
        // restores Drop-managed close semantics.
        let owned = unsafe { OwnedFd::from_raw_fd(fd) };
        let sock = Sock::from(owned);
        // io_uring requires non-blocking sockets; kernel-nic builds
        // blocking sockets by default (its own clients may block).
        sock.set_nonblocking(true).map_err(IoError::Socket)?;

        Ok(Self { sock })
    }

    /// Raw file descriptor for io_uring SQEs.
    #[inline]
    #[must_use]
    pub fn raw_fd(&self) -> RawFd {
        <Sock as AsRawFd>::as_raw_fd(&self.sock)
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
        let features = kernel_nic::KernelFeatures::detect().expect("features");
        let policy = kernel_nic::BusyPollPolicy::new(kernel_nic::BusyPollMode::Off, false);
        let sock = Socket::bind_with_reuseport(0, 2, features, policy).expect("bind");
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

    // ── Phase 5 verification: getsockopt round-trip ──────────────────
    //
    // These tests confirm the kernel actually *accepted* the per-core
    // socket options (vs silently returning ENOPROTOOPT). Loopback
    // devices accept the opts but they have no effect (no NAPI struct);
    // a real NIC is needed to observe busy-poll behaviour. We only
    // verify kernel acceptance here.

    /// `getsockopt(SOL_SOCKET, SO_BUSY_POLL)` helper. Returns `None` if
    /// the kernel rejects the option (old kernel / loopback quirk).
    fn getsockopt_int(fd: RawFd, level: i32, opt: i32) -> Option<libc::c_int> {
        let mut val: libc::c_int = 0;
        let mut len = std::mem::size_of::<libc::c_int>() as libc::socklen_t;
        // SAFETY: `val` is a valid c_int by value; getsockopt writes ≤ len.
        let rc = unsafe {
            libc::getsockopt(
                fd,
                level,
                opt,
                &mut val as *mut _ as *mut libc::c_void,
                &mut len,
            )
        };
        if rc == 0 { Some(val) } else { None }
    }

    #[test]
    fn busy_poll_round_trips_via_getsockopt() {
        let features = kernel_nic::KernelFeatures::detect().expect("features");
        if !features.supports_busy_poll() {
            eprintln!("skipping: SO_BUSY_POLL unsupported on this kernel");
            return;
        }
        let policy =
            kernel_nic::BusyPollPolicy::new(kernel_nic::BusyPollMode::Latency { us: 50 }, false);
        let sock = Socket::bind_with_reuseport(0, 0, features, policy).expect("bind");
        match getsockopt_int(sock.raw_fd(), libc::SOL_SOCKET, libc::SO_BUSY_POLL) {
            Some(v) => assert_eq!(v, 50, "SO_BUSY_POLL did not round-trip"),
            None => {
                eprintln!("skipping assertion: getsockopt(SO_BUSY_POLL) denied (loopback quirk)")
            }
        }
    }

    #[test]
    fn prefer_busy_poll_round_trips() {
        // SO_PREFER_BUSY_POLL = 74 (not in all libc versions).
        const SO_PREFER_BUSY_POLL: libc::c_int = 74;
        let features = kernel_nic::KernelFeatures::detect().expect("features");
        if !features.supports_prefer_busy_poll() {
            eprintln!("skipping: SO_PREFER_BUSY_POLL unsupported on this kernel");
            return;
        }
        let policy =
            kernel_nic::BusyPollPolicy::new(kernel_nic::BusyPollMode::Latency { us: 50 }, true);
        let sock = Socket::bind_with_reuseport(0, 0, features, policy).expect("bind");
        match getsockopt_int(sock.raw_fd(), libc::SOL_UDP, SO_PREFER_BUSY_POLL) {
            Some(v) => assert_eq!(v, 1, "SO_PREFER_BUSY_POLL did not round-trip"),
            None => eprintln!("skipping assertion: getsockopt(SO_PREFER_BUSY_POLL) denied"),
        }
    }

    #[test]
    fn incoming_cpu_round_trips() {
        let features = kernel_nic::KernelFeatures::detect().expect("features");
        if !features.supports_incoming_cpu() {
            eprintln!("skipping: SO_INCOMING_CPU unsupported on this kernel");
            return;
        }
        // SO_INCOMING_CPU is only applied when eBPF REUSEPORT steering is
        // available; otherwise it causes multi-worker misrouting on
        // loopback. Skip the round-trip assertion when the eBPF
        // capability is missing (the option is intentionally not set).
        if !features.supports_reuseport_ebpf() {
            eprintln!("skipping: SO_INCOMING_CPU intentionally not set (no eBPF capability)");
            return;
        }
        let policy = kernel_nic::BusyPollPolicy::default();
        let sock = Socket::bind_with_reuseport(0, 0, features, policy).expect("bind");
        match getsockopt_int(sock.raw_fd(), libc::SOL_SOCKET, libc::SO_INCOMING_CPU) {
            Some(v) => assert_eq!(v, 0, "SO_INCOMING_CPU did not round-trip"),
            None => eprintln!("skipping assertion: getsockopt(SO_INCOMING_CPU) denied"),
        }
    }

    /// Verifies that when eBPF REUSEPORT steering is unavailable, the
    /// socket intentionally skips `SO_INCOMING_CPU` to avoid multi-worker
    /// misrouting on loopback. The opt value defaults to -1 ("unset").
    #[test]
    fn no_incoming_cpu_when_ebpf_unavailable() {
        let features = kernel_nic::KernelFeatures::detect().expect("features");
        if !features.supports_incoming_cpu() {
            eprintln!("skipping: SO_INCOMING_CPU unsupported on this kernel");
            return;
        }
        if features.supports_reuseport_ebpf() {
            eprintln!("skipping: eBPF REUSEPORT capability present (opt IS applied)");
            return;
        }
        let policy = kernel_nic::BusyPollPolicy::default();
        let sock = Socket::bind_with_reuseport(0, 0, features, policy).expect("bind");
        match getsockopt_int(sock.raw_fd(), libc::SOL_SOCKET, libc::SO_INCOMING_CPU) {
            // Kernel default for unset SO_INCOMING_CPU is -1.
            Some(v) => assert_eq!(
                v, -1,
                "SO_INCOMING_CPU should be unset (-1) when eBPF is unavailable, got {v}"
            ),
            None => eprintln!("skipping assertion: getsockopt(SO_INCOMING_CPU) denied"),
        }
    }
}
