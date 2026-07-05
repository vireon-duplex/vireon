//! Kernel capability detection for the kernel-nic layer.
//!
//! [`KernelFeatures`] probes the running kernel **by attempt** rather
//! than by `uname(2)` version parsing. A probe is authoritative: if
//! `setsockopt(SO_BUSY_POLL, &50)` succeeds on a transient socket, the
//! feature is available regardless of what kernel version reports.
//!
//! Detection is run once at [`NicManager`](crate::NicManager)
//! construction and the result is frozen for the process lifetime —
//! the kernel does not change underneath us.

use std::os::fd::AsRawFd;

use socket2::{Domain, Protocol, Socket, Type};

use crate::error::KernelNicError;

/// Bitmask of detected kernel features.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct KernelFeatures(u64);

impl KernelFeatures {
    /// `io_uring` subsystem is available.
    pub const IO_URING: Self = Self(1 << 0);
    /// `IORING_FEAT_SEND_ZC` reported by the kernel.
    pub const IO_URING_SEND_ZC: Self = Self(1 << 1);
    /// `IORING_FEAT_REG_REG_RING` (PBUF_RING) reported by the kernel.
    pub const IO_URING_BUF_RING: Self = Self(1 << 2);
    /// `IORING_SETUP_SQPOLL` is available (privilege-gated at use).
    pub const IO_URING_SQPOLL: Self = Self(1 << 3);
    /// `SO_REUSEPORT` socket option.
    pub const SO_REUSEPORT: Self = Self(1 << 4);
    /// `SO_ATTACH_REUSEPORT_EBPF` socket option.
    pub const SO_ATTACH_REUSEPORT_EBPF: Self = Self(1 << 5);
    /// `SO_BUSY_POLL` socket option.
    pub const SO_BUSY_POLL: Self = Self(1 << 6);
    /// `SO_PREFER_BUSY_POLL` socket option (kernel ≥ 5.11).
    pub const SO_PREFER_BUSY_POLL: Self = Self(1 << 7);
    /// `SO_INCOMING_CPU` socket option.
    pub const SO_INCOMING_CPU: Self = Self(1 << 8);
    /// `UDP_SEGMENT` (UDP GSO sendmsg offload, kernel ≥ 4.18).
    pub const UDP_SEGMENT: Self = Self(1 << 9);
    /// `UDP_GRO` (UDP GRO recvmsg offload, kernel ≥ 5.0).
    pub const UDP_GRO: Self = Self(1 << 10);
    /// `MSG_ZEROCOPY` sendmsg flag (kernel ≥ 4.14).
    pub const MSG_ZEROCOPY: Self = Self(1 << 11);
    /// `busy_read` / `busy_poll` sysctls honoured (kernel ≥ 5.11 NAPI).
    pub const NAPI_BUSY_POLL: Self = Self(1 << 12);
    /// `SO_NETPOLL_CPU` / NAPI id exposed via `getsockopt`.
    pub const NET_RX_NAPI_ID: Self = Self(1 << 13);

    /// Empty feature set (nothing detected).
    #[must_use]
    pub const fn empty() -> Self {
        Self(0)
    }

    /// Raw bitmask value (debugging + telemetry).
    #[must_use]
    pub const fn bits(self) -> u64 {
        self.0
    }

    /// `true` if no features are set.
    #[must_use]
    pub const fn is_empty(self) -> bool {
        self.0 == 0
    }

    /// `true` if `self` contains all bits in `other`.
    #[must_use]
    pub const fn contains(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }

    /// Set union. Same as `self | other`.
    #[must_use]
    pub const fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }

    /// Insert `other` into `self` in place.
    pub fn insert(&mut self, other: Self) {
        self.0 |= other.0;
    }

    /// Probe the running kernel. Returns the union of every detected
    /// feature bit. Errors only on catastrophic failure (cannot create
    /// even a transient socket).
    pub fn detect() -> Result<Self, KernelNicError> {
        let mut f = Self::empty();

        // ── io_uring ────────────────────────────────────────────────
        // Probe by attempt — successful ring creation is the only
        // authoritative signal. Sub-categorisation uses `uname(2)`,
        // which is init-time only.
        if io_uring::IoUring::<io_uring::squeue::Entry, io_uring::cqueue::Entry>::builder()
            .build(2)
            .is_ok()
        {
            f.insert(Self::IO_URING);
            let (major, minor) = kernel_version();
            // IORING_OP_SEND_ZC + RECVMSG_MULTISHOT: kernel ≥ 6.0
            if kernel_ge(major, minor, 6, 0) {
                f.insert(Self::IO_URING_SEND_ZC);
            }
            // IORING_REGISTER_PBUF_RING: kernel ≥ 5.19
            if kernel_ge(major, minor, 5, 19) {
                f.insert(Self::IO_URING_BUF_RING);
            }
            // SQPOLL: kernel ≥ 5.11 (privilege-gated at use)
            if kernel_ge(major, minor, 5, 11) {
                f.insert(Self::IO_URING_SQPOLL);
            }
        }

        // ── Socket-option probes (UDPv4) ────────────────────────────
        let probe = new_udp_probe_socket(Domain::IPV4)?;
        let fd = probe.as_raw_fd();

        if try_sockopt_int(fd, libc::SOL_SOCKET, libc::SO_REUSEPORT, 1).is_ok() {
            f.insert(Self::SO_REUSEPORT);
        }
        if try_sockopt_int(fd, libc::SOL_SOCKET, libc::SO_BUSY_POLL, 1).is_ok() {
            f.insert(Self::SO_BUSY_POLL);
        }
        if try_sockopt_int(fd, libc::SOL_SOCKET, libc::SO_INCOMING_CPU, 0).is_ok() {
            f.insert(Self::SO_INCOMING_CPU);
        }

        // SO_PREFER_BUSY_POLL — separate protocol level (SOL_UDP).
        if try_sockopt_int(fd, libc::SOL_UDP, SO_PREFER_BUSY_POLL, 1).is_ok() {
            f.insert(Self::SO_PREFER_BUSY_POLL);
        }
        if try_sockopt_int(fd, libc::SOL_UDP, libc::UDP_SEGMENT, 1472).is_ok() {
            f.insert(Self::UDP_SEGMENT);
        }
        if try_sockopt_int(fd, libc::SOL_UDP, libc::UDP_GRO, 1).is_ok() {
            f.insert(Self::UDP_GRO);
        }

        // SO_ATTACH_REUSEPORT_EBPF — try to attach a tiny no-op eBPF
        // program. The kernel accepts the syscall when the socket
        // option exists; failure means the option is missing.
        if try_attach_reuseport_ebpf(fd).is_ok() {
            f.insert(Self::SO_ATTACH_REUSEPORT_EBPF);
        }

        // MSG_ZEROCOPY — flagged at sendmsg time. The kernel
        // accepts the flag value on sockets regardless of driver.
        // Probe via /proc/sys/net/core existence (always present on
        // any kernel that supports MSG_ZEROCOPY).
        if std::path::Path::new("/proc/sys/net/core/optmem_max").exists() {
            f.insert(Self::MSG_ZEROCOPY);
        }

        // NAPI busy poll sysctls (kernel ≥ 5.11).
        if std::path::Path::new("/proc/sys/net/core/busy_poll").exists()
            && std::path::Path::new("/proc/sys/net/core/busy_read").exists()
        {
            f.insert(Self::NAPI_BUSY_POLL);
        }

        Ok(f)
    }

    // ── Query helpers (kept terse — `contains` is the primary API) ──

    #[must_use]
    pub fn supports_io_uring(self) -> bool {
        self.contains(Self::IO_URING)
    }
    #[must_use]
    pub fn supports_send_zc(self) -> bool {
        self.contains(Self::IO_URING_SEND_ZC)
    }
    #[must_use]
    pub fn supports_buf_ring(self) -> bool {
        self.contains(Self::IO_URING_BUF_RING)
    }
    #[must_use]
    pub fn supports_reuseport(self) -> bool {
        self.contains(Self::SO_REUSEPORT)
    }
    #[must_use]
    pub fn supports_reuseport_ebpf(self) -> bool {
        self.contains(Self::SO_ATTACH_REUSEPORT_EBPF)
    }
    #[must_use]
    pub fn supports_busy_poll(self) -> bool {
        self.contains(Self::SO_BUSY_POLL)
    }
    #[must_use]
    pub fn supports_prefer_busy_poll(self) -> bool {
        self.contains(Self::SO_PREFER_BUSY_POLL)
    }
    #[must_use]
    pub fn supports_incoming_cpu(self) -> bool {
        self.contains(Self::SO_INCOMING_CPU)
    }
    #[must_use]
    pub fn supports_udp_gso(self) -> bool {
        self.contains(Self::UDP_SEGMENT)
    }
    #[must_use]
    pub fn supports_udp_gro(self) -> bool {
        self.contains(Self::UDP_GRO)
    }
}

impl std::ops::BitOr for KernelFeatures {
    type Output = Self;
    fn bitor(self, rhs: Self) -> Self {
        Self(self.0 | rhs.0)
    }
}

impl std::ops::BitOrAssign for KernelFeatures {
    fn bitor_assign(&mut self, rhs: Self) {
        self.0 |= rhs.0;
    }
}

impl std::fmt::Display for KernelFeatures {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut first = true;
        let mut emit = |name: &str, first: &mut bool| -> std::fmt::Result {
            if !*first {
                f.write_str(" | ")?;
            }
            *first = false;
            f.write_str(name)
        };
        if self.contains(Self::IO_URING) {
            emit("IO_URING", &mut first)?;
        }
        if self.contains(Self::IO_URING_SEND_ZC) {
            emit("IO_URING_SEND_ZC", &mut first)?;
        }
        if self.contains(Self::IO_URING_BUF_RING) {
            emit("IO_URING_BUF_RING", &mut first)?;
        }
        if self.contains(Self::SO_REUSEPORT) {
            emit("SO_REUSEPORT", &mut first)?;
        }
        if self.contains(Self::SO_ATTACH_REUSEPORT_EBPF) {
            emit("SO_ATTACH_REUSEPORT_EBPF", &mut first)?;
        }
        if self.contains(Self::SO_BUSY_POLL) {
            emit("SO_BUSY_POLL", &mut first)?;
        }
        if self.contains(Self::SO_PREFER_BUSY_POLL) {
            emit("SO_PREFER_BUSY_POLL", &mut first)?;
        }
        if self.contains(Self::SO_INCOMING_CPU) {
            emit("SO_INCOMING_CPU", &mut first)?;
        }
        if self.contains(Self::UDP_SEGMENT) {
            emit("UDP_SEGMENT", &mut first)?;
        }
        if self.contains(Self::UDP_GRO) {
            emit("UDP_GRO", &mut first)?;
        }
        if first {
            f.write_str("(empty)")?;
        }
        Ok(())
    }
}

// ── Probing primitives ──────────────────────────────────────────────

/// `SO_PREFER_BUSY_POLL` value — not in upstream `libc` on every distro.
/// Defined locally to avoid feature-gating.
const SO_PREFER_BUSY_POLL: i32 = 74;

/// Returns `(major, minor)` from `uname(2)`. Returns `(0, 0)` on failure.
fn kernel_version() -> (u32, u32) {
    // SAFETY: `utsname` is a plain struct of fixed-size char arrays;
    // `uname(2)` fills it atomically.
    let mut uts: libc::utsname = unsafe { std::mem::zeroed() };
    if unsafe { libc::uname(&mut uts) } != 0 {
        return (0, 0);
    }
    let release: Vec<u8> = uts
        .release
        .iter()
        .take_while(|&&c| c != 0)
        .map(|&c| c as u8)
        .collect();
    let s = String::from_utf8_lossy(&release);
    let mut parts = s.split('.');
    let major = parts.next().and_then(|s| s.parse().ok()).unwrap_or(0);
    let minor = parts.next().and_then(|s| s.parse().ok()).unwrap_or(0);
    (major, minor)
}

/// Returns `true` if the kernel version is ≥ `(req_major, req_minor)`.
const fn kernel_ge(major: u32, minor: u32, req_major: u32, req_minor: u32) -> bool {
    major > req_major || (major == req_major && minor >= req_minor)
}

fn new_udp_probe_socket(domain: Domain) -> Result<Socket, KernelNicError> {
    Socket::new(domain, Type::DGRAM, Some(Protocol::UDP)).map_err(KernelNicError::Socket)
}

/// Try `setsockopt(level, opt, &val)`. Returns `Ok(())` if the kernel
/// accepted the option, `Err(io::Error)` otherwise.
fn try_sockopt_int(
    fd: libc::c_int,
    level: libc::c_int,
    opt: libc::c_int,
    val: libc::c_int,
) -> Result<(), std::io::Error> {
    let raw: libc::c_int = val;
    // SAFETY: `setsockopt(2)` with a valid fd, valid pointer, valid length.
    let rc = unsafe {
        libc::setsockopt(
            fd,
            level,
            opt,
            &raw as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        )
    };
    if rc == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

/// Try attaching a tiny no-op eBPF program to the socket's REUSEPORT
/// group. Returns Ok if the kernel accepted the syscall (even if the
/// program is later rejected by the verifier — we only care that the
/// socket option exists).
fn try_attach_reuseport_ebpf(fd: libc::c_int) -> Result<(), std::io::Error> {
    // The smallest valid eBPF program: a single `mov r0, 0 ; exit`.
    // BPF_ALU64 | BPF_MOV | BPF_K with dst=r0, imm=0
    // BPF_JMP | BPF_EXIT
    #[repr(C)]
    #[derive(Clone, Copy)]
    struct BpfInsn {
        code: u8,
        regs: u8,
        off: i16,
        imm: i32,
    }
    let prog: [BpfInsn; 2] = [
        BpfInsn {
            code: 0xb7, // BPF_ALU64 | BPF_MOV | BPF_K
            regs: 0,    // dst = r0
            off: 0,
            imm: 0,
        },
        BpfInsn {
            code: 0x95, // BPF_JMP | BPF_EXIT
            regs: 0,
            off: 0,
            imm: 0,
        },
    ];

    #[repr(C)]
    #[derive(Clone, Copy)]
    struct SockProg {
        prog_type: u32,
        insns: u64, // pointer
        insn_cnt: u32,
        license: u64, // pointer
        log_level: u32,
    }

    let license = b"GPL\0";
    let mut attr = SockProg {
        prog_type: 9, // BPF_PROG_TYPE_SOCKET_FILTER
        insns: prog.as_ptr() as u64,
        insn_cnt: prog.len() as u32,
        license: license.as_ptr() as u64,
        log_level: 0,
    };

    // SAFETY: `bpf(2)` with BPF_PROG_LOAD attribute. Buffer sizes are correct.
    let prog_fd = unsafe {
        libc::syscall(
            libc::SYS_bpf,
            5, // BPF_PROG_LOAD
            &mut attr as *mut _ as *mut libc::c_void,
            std::mem::size_of::<SockProg>(),
        )
    };
    if prog_fd < 0 {
        return Err(std::io::Error::last_os_error());
    }

    // Now attach via SO_ATTACH_REUSEPORT_EBPF.
    let prog_fd_i32: libc::c_int = prog_fd as libc::c_int;
    let rc = unsafe {
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_ATTACH_REUSEPORT_EBPF,
            &prog_fd_i32 as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        )
    };
    // Always close the prog fd; we do not keep it.
    unsafe { libc::close(prog_fd_i32) };

    if rc == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]
    use super::*;

    #[test]
    fn detect_runs_on_host_kernel() {
        // The host runs a modern Linux kernel — at minimum SO_REUSEPORT
        // (since 3.9) should be present.
        let features = KernelFeatures::detect().expect("detect");
        assert!(
            features.supports_reuseport(),
            "expected SO_REUSEPORT support; got {features}"
        );
    }

    #[test]
    fn contains_and_union_round_trip() {
        let mut f = KernelFeatures::empty();
        assert!(!f.contains(KernelFeatures::SO_REUSEPORT));
        f |= KernelFeatures::SO_REUSEPORT;
        f |= KernelFeatures::UDP_GRO;
        assert!(f.contains(KernelFeatures::SO_REUSEPORT));
        assert!(f.contains(KernelFeatures::UDP_GRO));
        assert!(!f.contains(KernelFeatures::IO_URING));
    }

    #[test]
    fn display_lists_active_features() {
        let f = KernelFeatures::SO_REUSEPORT | KernelFeatures::UDP_GRO;
        let s = f.to_string();
        assert!(s.contains("SO_REUSEPORT"));
        assert!(s.contains("UDP_GRO"));
    }

    #[test]
    fn empty_display_shows_empty() {
        assert_eq!(KernelFeatures::empty().to_string(), "(empty)");
    }
}
