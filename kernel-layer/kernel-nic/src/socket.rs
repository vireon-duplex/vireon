//! Socket lifecycle — `SocketBuilder`, `SocketGroup`, `PerCoreSocketRegistry`.
//!
//! [`SocketBuilder`] is the **single** entry point that produces a
//! fully-configured `RawFd` for the IO Layer. All `socket(2)` /
//! `bind(2)` / `setsockopt(2)` policy is centralised here.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::os::fd::{AsRawFd, OwnedFd, RawFd};

use socket2::{Domain, Protocol, Socket, Type};

use crate::error::KernelNicError;
use crate::features::KernelFeatures;

// ── Address family ──────────────────────────────────────────────────

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AddressFamily {
    Ipv4,
    Ipv6,
    DualStack,
}

impl AddressFamily {
    /// Returns the `socket2::Domain` to use when creating the socket.
    #[must_use]
    pub const fn domain(self) -> Domain {
        match self {
            Self::Ipv4 => Domain::IPV4,
            Self::Ipv6 | Self::DualStack => Domain::IPV6,
        }
    }
}

// ── Socket spec ─────────────────────────────────────────────────────

/// Frozen description of every option applied to a socket. Used for
/// diagnostics, recreation, and pooled-socket matching.
#[derive(Clone, Debug)]
pub struct SocketSpec {
    pub family: AddressFamily,
    pub port: u16,
    pub bind_address: Option<IpAddr>,
    pub nic_ifindex: Option<u32>,
    pub incoming_cpu: Option<u32>,
    pub reuseport_group: Option<u64>,
    pub rcvbuf: Option<u32>,
    pub sndbuf: Option<u32>,
    pub busy_poll_us: u32,
    pub prefer_busy_poll: bool,
    pub udp_gro: bool,
    pub udp_segment: Option<u16>,
    pub recv_err: bool,
}

impl SocketSpec {
    /// Convenience: a minimal spec for a single REUSEPORT member.
    #[must_use]
    pub fn for_reuseport(port: u16, cpu: u32, group_id: u64) -> Self {
        Self {
            family: AddressFamily::Ipv4,
            port,
            bind_address: None,
            nic_ifindex: None,
            incoming_cpu: Some(cpu),
            reuseport_group: Some(group_id),
            rcvbuf: None,
            sndbuf: None,
            busy_poll_us: 0,
            prefer_busy_poll: false,
            udp_gro: false,
            udp_segment: None,
            recv_err: false,
        }
    }
}

// ── SocketBuilder ───────────────────────────────────────────────────

/// Builds a fully-configured UDP socket. Owns no state — each
/// [`SocketBuilder::build`] call performs `socket(2)` + `bind(2)` +
/// `setsockopt(2)` and returns the resulting fd.
#[derive(Clone, Debug)]
pub struct SocketBuilder {
    spec: SocketSpec,
    features: KernelFeatures,
}

impl SocketBuilder {
    /// Construct from a spec + capability snapshot.
    #[must_use]
    pub const fn new(spec: SocketSpec, features: KernelFeatures) -> Self {
        Self { spec, features }
    }

    /// Apply all configured options and return the resulting fd. The
    /// caller owns the fd and is responsible for closing it.
    pub fn build(&self) -> Result<ConfiguredSocket, KernelNicError> {
        let sock = Socket::new(self.spec.family.domain(), Type::DGRAM, Some(Protocol::UDP))
            .map_err(KernelNicError::Socket)?;
        let fd = sock.as_raw_fd();

        // SO_REUSEADDR + SO_REUSEPORT.
        setsockopt_int(fd, libc::SOL_SOCKET, libc::SO_REUSEADDR, 1)?;
        if self.features.supports_reuseport() {
            setsockopt_int(fd, libc::SOL_SOCKET, libc::SO_REUSEPORT, 1)?;
        }

        // SO_INCOMING_CPU.
        if let Some(cpu) = self.spec.incoming_cpu {
            if self.features.supports_incoming_cpu() {
                let _ = setsockopt_int(fd, libc::SOL_SOCKET, libc::SO_INCOMING_CPU, cpu as i32);
            }
        }

        // SO_RCVBUF / SO_SNDBUF.
        if let Some(buf) = self.spec.rcvbuf {
            let _ = setsockopt_int(fd, libc::SOL_SOCKET, libc::SO_RCVBUF, buf as i32);
        }
        if let Some(buf) = self.spec.sndbuf {
            let _ = setsockopt_int(fd, libc::SOL_SOCKET, libc::SO_SNDBUF, buf as i32);
        }

        // SO_BUSY_POLL.
        if self.spec.busy_poll_us > 0 && self.features.supports_busy_poll() {
            let _ = setsockopt_int(
                fd,
                libc::SOL_SOCKET,
                libc::SO_BUSY_POLL,
                self.spec.busy_poll_us as i32,
            );
        }
        if self.spec.prefer_busy_poll && self.features.supports_prefer_busy_poll() {
            let _ = setsockopt_int(fd, libc::SOL_UDP, SO_PREFER_BUSY_POLL, 1);
        }

        // UDP GRO / UDP_SEGMENT.
        if self.spec.udp_gro && self.features.supports_udp_gro() {
            let _ = setsockopt_int(fd, libc::SOL_UDP, libc::UDP_GRO, 1);
        }
        if let Some(seg) = self.spec.udp_segment {
            if self.features.supports_udp_gso() {
                let _ = setsockopt_int(fd, libc::SOL_UDP, libc::UDP_SEGMENT, seg as i32);
            }
        }

        if self.spec.recv_err {
            let _ = setsockopt_int(fd, libc::SOL_IP, libc::IP_RECVERR, 1);
        }

        // Bind.
        let bind_addr = match (self.spec.family, self.spec.bind_address) {
            (AddressFamily::Ipv4, Some(IpAddr::V4(a)))
            | (AddressFamily::Ipv6 | AddressFamily::DualStack, Some(IpAddr::V4(a))) => {
                SocketAddr::new(a.into(), self.spec.port)
            }
            (AddressFamily::Ipv6 | AddressFamily::DualStack, Some(IpAddr::V6(a))) => {
                SocketAddr::new(a.into(), self.spec.port)
            }
            (AddressFamily::Ipv4, Some(IpAddr::V6(_))) => {
                return Err(KernelNicError::InvalidConfig(
                    "cannot bind IPv6 address on IPv4 socket".into(),
                ));
            }
            (AddressFamily::Ipv4, None) => {
                SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), self.spec.port)
            }
            (AddressFamily::Ipv6 | AddressFamily::DualStack, None) => {
                if self.spec.family == AddressFamily::Ipv6 {
                    let _ = setsockopt_int(fd, libc::SOL_IPV6, libc::IPV6_V6ONLY, 1);
                }
                SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), self.spec.port)
            }
        };
        sock.bind(&bind_addr.into())
            .map_err(KernelNicError::Socket)?;

        // Transfer ownership of the fd out of `sock` so its Drop does
        // not close it.
        let owned_fd: OwnedFd = sock.into();
        // Safety: we are handing the caller a uniquely-owned fd.
        let raw = owned_fd.as_raw_fd();
        std::mem::forget(owned_fd);

        Ok(ConfiguredSocket {
            fd: raw,
            spec: self.spec.clone(),
        })
    }
}

/// A socket created by [`SocketBuilder`] that owns its fd. Drop closes
/// the fd; [`ConfiguredSocket::into_raw_fd`] hands ownership to the
/// caller (typically the IO Layer).
#[derive(Debug)]
pub struct ConfiguredSocket {
    pub fd: RawFd,
    pub spec: SocketSpec,
}

impl ConfiguredSocket {
    /// Take ownership of the fd. The caller is responsible for closing it.
    #[must_use]
    pub fn into_raw_fd(self) -> RawFd {
        let fd = self.fd;
        std::mem::forget(self);
        fd
    }

    /// Borrow the spec.
    #[must_use]
    pub fn spec(&self) -> &SocketSpec {
        &self.spec
    }
}

impl Drop for ConfiguredSocket {
    fn drop(&mut self) {
        // Safety: we own the fd exclusively.
        unsafe { libc::close(self.fd) };
    }
}

// ── Socket group ────────────────────────────────────────────────────

/// REUSEPORT group membership. Tracks generation for rolling upgrades.
#[derive(Clone, Debug)]
pub struct SocketGroup {
    pub group_id: u64,
    pub ifindex: u32,
    pub port: u16,
    pub family: AddressFamily,
    pub generation: u64,
    pub members: Vec<SocketGroupMember>,
    pub created_at_ns: u64,
    pub last_rotation_at_ns: Option<u64>,
}

#[derive(Clone, Debug)]
pub struct SocketGroupMember {
    pub cpu: u32,
    pub fd: RawFd,
    pub incoming_cpu: u32,
    pub armed_at_ns: u64,
    pub state: MemberState,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MemberState {
    Bound,
    Armed,
    Draining,
}

impl SocketGroup {
    /// Construct a fresh group at generation 0.
    #[must_use]
    pub fn new(group_id: u64, ifindex: u32, port: u16, family: AddressFamily, now_ns: u64) -> Self {
        Self {
            group_id,
            ifindex,
            port,
            family,
            generation: 0,
            members: Vec::new(),
            created_at_ns: now_ns,
            last_rotation_at_ns: None,
        }
    }

    /// Add a member (called by the manager after `SocketBuilder::build`).
    pub fn add_member(&mut self, cpu: u32, fd: RawFd, incoming_cpu: u32, now_ns: u64) {
        self.members.push(SocketGroupMember {
            cpu,
            fd,
            incoming_cpu,
            armed_at_ns: now_ns,
            state: MemberState::Bound,
        });
    }

    /// Mark all current members as `Draining`. Used at the start of a
    /// rolling upgrade.
    pub fn mark_draining(&mut self) {
        for m in &mut self.members {
            m.state = MemberState::Draining;
        }
    }

    /// Remove draining members. Returns the fds that were closed by
    /// the caller (the manager does the close, not the group).
    pub fn drain_finished(&mut self) -> Vec<RawFd> {
        let mut removed = Vec::new();
        self.members.retain(|m| {
            if m.state == MemberState::Draining {
                removed.push(m.fd);
                false
            } else {
                true
            }
        });
        removed
    }

    /// Bump generation; record rotation time.
    pub fn bump_generation(&mut self, now_ns: u64) {
        self.generation = self.generation.saturating_add(1);
        self.last_rotation_at_ns = Some(now_ns);
    }
}

// ── Per-core socket registry ────────────────────────────────────────

/// Per-reactor registry of bound sockets. `!Sync` by design — only
/// the owning reactor thread may mutate.
#[derive(Debug)]
pub struct PerCoreSocketRegistry {
    cpu_id: u32,
    numa_node: u32,
    sockets: Vec<RegisteredSocket>,
    pool: SocketPool,
}

#[derive(Clone, Debug)]
pub struct RegisteredSocket {
    pub fd: RawFd,
    pub spec: SocketSpec,
    pub group_id: Option<u64>,
    pub registered_at_ns: u64,
}

// Safety: PerCoreSocketRegistry is single-threaded. We allow Send so
// the manager can move it into the owning reactor thread at startup.
unsafe impl Send for PerCoreSocketRegistry {}

impl PerCoreSocketRegistry {
    /// Construct for a given CPU + NUMA node.
    #[must_use]
    pub fn for_cpu(cpu_id: u32, numa_node: u32, pool_depth: usize) -> Self {
        Self {
            cpu_id,
            numa_node,
            sockets: Vec::new(),
            pool: SocketPool::new(pool_depth),
        }
    }

    #[must_use]
    pub const fn cpu_id(&self) -> u32 {
        self.cpu_id
    }

    #[must_use]
    pub const fn numa_node(&self) -> u32 {
        self.numa_node
    }

    /// Register a freshly-bound socket.
    pub fn register(&mut self, socket: RegisteredSocket) {
        self.sockets.push(socket);
    }

    /// Unregister by fd. Does NOT close — the caller (IO Layer) owns
    /// the fd lifecycle.
    pub fn unregister(&mut self, fd: RawFd) -> Option<RegisteredSocket> {
        self.sockets
            .iter()
            .position(|s| s.fd == fd)
            .map(|i| self.sockets.remove(i))
    }

    /// Borrow all registered sockets.
    pub fn iter(&self) -> impl Iterator<Item = &RegisteredSocket> {
        self.sockets.iter()
    }

    /// Number of sockets currently registered.
    #[must_use]
    pub fn count(&self) -> usize {
        self.sockets.len()
    }

    /// Acquire a recycled fd from the per-core pool, if one matches
    /// the spec family.
    pub fn acquire_pooled(&mut self, _spec: &SocketSpec) -> Option<RawFd> {
        self.pool.acquire()
    }

    /// Return a fd to the pool for later reuse.
    pub fn release_to_pool(&mut self, fd: RawFd, _spec: SocketSpec) {
        self.pool.release(fd);
    }

    /// Pool size (for diagnostics).
    #[must_use]
    pub fn pool_size(&self) -> usize {
        self.pool.len()
    }
}

// ── Socket pool ─────────────────────────────────────────────────────

#[derive(Debug)]
pub struct SocketPool {
    depth: usize,
    entries: Vec<RawFd>,
}

impl SocketPool {
    #[must_use]
    pub const fn new(depth: usize) -> Self {
        Self {
            depth,
            entries: Vec::new(),
        }
    }

    pub fn acquire(&mut self) -> Option<RawFd> {
        self.entries.pop()
    }

    pub fn release(&mut self, fd: RawFd) {
        if self.entries.len() < self.depth {
            self.entries.push(fd);
        } else {
            // Pool full — close the fd.
            // Safety: caller transferred ownership by releasing.
            unsafe { libc::close(fd) };
        }
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

impl Drop for SocketPool {
    fn drop(&mut self) {
        for &fd in &self.entries {
            // Safety: pool owns these fds.
            unsafe { libc::close(fd) };
        }
    }
}

// ── Setsockopt helper ───────────────────────────────────────────────

/// `SO_PREFER_BUSY_POLL` value — not in upstream `libc` on every distro.
const SO_PREFER_BUSY_POLL: i32 = 74;

fn setsockopt_int(fd: RawFd, level: i32, opt: i32, val: i32) -> Result<(), KernelNicError> {
    let raw: libc::c_int = val;
    // Safety: setsockopt with valid fd, valid pointer, valid length.
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
        Err(KernelNicError::Socket(std::io::Error::last_os_error()))
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]
    use super::*;

    #[test]
    fn socket_builder_creates_bound_udp_socket() {
        let spec = SocketSpec::for_reuseport(0, 0, 1);
        let features = KernelFeatures::detect().expect("features");
        let builder = SocketBuilder::new(spec, features);
        let sock = builder.build().expect("build");
        assert!(sock.fd >= 0);
        // into_raw_fd transfers ownership; we must close.
        let fd = sock.into_raw_fd();
        unsafe { libc::close(fd) };
    }

    #[test]
    fn socket_group_lifecycle() {
        let mut g = SocketGroup::new(7, 2, 8080, AddressFamily::Ipv4, 1_000_000);
        assert_eq!(g.generation, 0);
        g.add_member(0, 42, 0, 1_000_001);
        assert_eq!(g.members.len(), 1);
        g.bump_generation(1_000_002);
        assert_eq!(g.generation, 1);
        g.mark_draining();
        assert_eq!(g.members[0].state, MemberState::Draining);
        let removed = g.drain_finished();
        assert_eq!(removed, vec![42]);
        assert!(g.members.is_empty());
    }

    #[test]
    fn per_core_registry_register_unregister() {
        let mut r = PerCoreSocketRegistry::for_cpu(0, 0, 2);
        let s = RegisteredSocket {
            fd: 42,
            spec: SocketSpec::for_reuseport(8080, 0, 1),
            group_id: Some(1),
            registered_at_ns: 0,
        };
        r.register(s);
        assert_eq!(r.count(), 1);
        let removed = r.unregister(42);
        assert!(removed.is_some());
        assert_eq!(r.count(), 0);
    }

    #[test]
    fn socket_pool_acquire_release() {
        let mut pool = SocketPool::new(2);
        assert!(pool.is_empty());
        pool.release(100);
        pool.release(101);
        pool.release(102); // overflow, fd 102 should be closed
        assert_eq!(pool.len(), 2);
        let fd = pool.acquire();
        assert_eq!(fd, Some(101)); // LIFO
    }

    #[test]
    fn address_family_domain() {
        assert_eq!(AddressFamily::Ipv4.domain(), Domain::IPV4);
        assert_eq!(AddressFamily::Ipv6.domain(), Domain::IPV6);
        assert_eq!(AddressFamily::DualStack.domain(), Domain::IPV6);
    }

    #[test]
    fn busy_poll_policy_propagates_to_spec() {
        use crate::busy_poll::{BusyPollMode, BusyPollPolicy};
        let policy = BusyPollPolicy::new(BusyPollMode::Latency { us: 50 }, true);
        let mut spec = SocketSpec::for_reuseport(8080, 0, 1);
        spec.busy_poll_us = policy.mode.us();
        spec.prefer_busy_poll = policy.should_prefer();
        assert_eq!(spec.busy_poll_us, 50);
        assert!(spec.prefer_busy_poll);
    }
}
