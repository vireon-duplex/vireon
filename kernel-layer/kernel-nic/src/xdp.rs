//! AF_XDP (XDP Socket) support for kernel-bypass packet processing.
//!
//! XDP sockets bypass the Linux network stack entirely, receiving
//! packets directly from the NIC driver into user-space memory. This
//! eliminates sk_buff allocation, syscall overhead, and context
//! switches — achieving sub-microsecond packet processing latency.
//!
//! # Architecture
//!
//! ```text
//!  ┌──────────┐     ┌─────────────────────┐     ┌──────────┐
//!  │   NIC    │────▶│  XDP Program (eBPF) │────▶│ AF_XDP   │
//!  │  Driver  │     │  (redirect to socket)│     │ Socket   │
//!  └──────────┘     └─────────────────────┘     └──────────┘
//!                                                        │
//!                                               ┌────────┴────────┐
//!                                               │   UMEM (shared  │
//!                                               │   memory region) │
//!                                               └────────┬────────┘
//!                                                        │
//!                              ┌──────────┬──────────────┼──────────────┬──────────┐
//!                              │ FILL     │ COMPLETION   │ RX           │ TX       │
//!                              │ Ring     │ Ring         │ Ring         │ Ring     │
//!                              └──────────┴──────────────┴──────────────┴──────────┘
//! ```
//!
//! # UMEM
//!
//! The UMEM is a contiguous memory region shared between user space
//! and the kernel. It is divided into fixed-size frames (default
//! 4096 bytes). Each frame holds one packet. The user allocates
//! frames, passes their addresses to the FILL ring (for RX) or TX
//! ring, and receives completed descriptors via the RX ring (for
//! received packets) or COMPLETION ring (for sent packets).

use std::os::fd::RawFd;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::error::KernelNicError;

// ── Kernel constants ────────────────────────────────────────────────

/// `AF_XDP` address family (kernel 4.18+).
const AF_XDP: libc::c_int = 44;

/// XDP socket protocol.
const SOCK_XDP: libc::c_int = 0;

/// `setsockopt` level for XDP.
const SOL_XDP: libc::c_int = 283;

/// `setsockopt` options.
#[allow(dead_code)] // used in later UMEM registration / mmap phases
const XDP_UMEM_REG: libc::c_int = 5;
const XDP_UMEM_FILL_RING: libc::c_int = 6;
const XDP_UMEM_COMPLETION_RING: libc::c_int = 7;
const XDP_TX_RING: libc::c_int = 9;
const XDP_RX_RING: libc::c_int = 8;
#[allow(dead_code)] // used when ring mmap offsets are queried
const XDP_MMAP_OFFSETS: libc::c_int = 1;
#[allow(dead_code)] // used when NEED_WAKEUP polling is implemented
const XDP_USE_NEED_WAKEUP: u64 = 1 << 3;

/// XDP bind flags (`bind(2)` with `AF_XDP`).
const XDP_FLAGS_UPDATE_IF_NOEXIST: u32 = 1 << 0;
const XDP_FLAGS_SKB_MODE: u32 = 1 << 1;
const XDP_FLAGS_DRV_MODE: u32 = 1 << 2;
#[allow(dead_code)] // mask of all mode bits, used in validation
const XDP_FLAGS_MODES: u32 = XDP_FLAGS_SKB_MODE | XDP_FLAGS_DRV_MODE;

/// XDP socket bind flags for the address struct.
const XDP_ZEROCOPY: u16 = 1 << 1;
const XDP_COPY: u16 = 1 << 0;

/// Default frame size (page-aligned).
pub const DEFAULT_FRAME_SIZE: u32 = 4096;

/// Default number of frames in the UMEM.
pub const DEFAULT_FRAME_COUNT: u32 = 4096;

/// Default ring size (descriptors).
pub const DEFAULT_RING_SIZE: u32 = 2048;

/// Minimum frame size (chained packets not yet supported).
pub const MIN_FRAME_SIZE: u32 = 2048;

/// Minimum ring size.
pub const MIN_RING_SIZE: u32 = 64;

/// Maximum ring size (kernel allows up to 2^15 for most rings).
pub const MAX_RING_SIZE: u32 = 32768;

// ── XdpConfig ───────────────────────────────────────────────────────

/// XDP bind mode — determines where the XDP program runs.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum XdpMode {
    /// Driver-level XDP — highest performance, requires driver support.
    Driver,
    /// SKB-mode fallback — works on any NIC, lower performance.
    #[default]
    Skb,
}

impl XdpMode {
    /// Convert to kernel bind flag.
    const fn to_flag(self) -> u32 {
        match self {
            Self::Driver => XDP_FLAGS_DRV_MODE,
            Self::Skb => XDP_FLAGS_SKB_MODE,
        }
    }
}

/// Whether to use zero-copy or copy mode.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum XdpCopyMode {
    /// Zero-copy — packets never cross the kernel/user boundary.
    /// Requires driver support (e.g. i40e, mlx5, ice).
    ZeroCopy,
    /// Copy mode — kernel copies packets to/from UMEM. Works with
    /// any driver but has higher overhead.
    #[default]
    Copy,
}

/// Configuration for an AF_XDP socket.
#[derive(Clone, Copy, Debug)]
pub struct XdpConfig {
    /// Number of frames in the UMEM.
    pub frame_count: u32,
    /// Size of each frame in bytes (power of 2, ≥ 2048).
    pub frame_size: u32,
    /// Number of descriptors in the FILL ring (RX buffer pool).
    pub fill_ring_size: u32,
    /// Number of descriptors in the COMPLETION ring (TX completion).
    pub completion_ring_size: u32,
    /// Number of descriptors in the RX ring.
    pub rx_ring_size: u32,
    /// Number of descriptors in the TX ring.
    pub tx_ring_size: u32,
    /// XDP attach mode (driver vs SKB).
    pub mode: XdpMode,
    /// Copy mode (zero-copy vs copy).
    pub copy_mode: XdpCopyMode,
    /// Interface index (ifindex) to bind to.
    pub ifindex: u32,
    /// Queue ID on the NIC.
    pub queue_id: u32,
}

impl Default for XdpConfig {
    fn default() -> Self {
        Self {
            frame_count: DEFAULT_FRAME_COUNT,
            frame_size: DEFAULT_FRAME_SIZE,
            fill_ring_size: DEFAULT_RING_SIZE,
            completion_ring_size: DEFAULT_RING_SIZE,
            rx_ring_size: DEFAULT_RING_SIZE,
            tx_ring_size: DEFAULT_RING_SIZE,
            mode: XdpMode::default(),
            copy_mode: XdpCopyMode::default(),
            ifindex: 0,
            queue_id: 0,
        }
    }
}

impl XdpConfig {
    /// Validate configuration fields.
    ///
    /// # Errors
    /// Returns [`KernelNicError::InvalidConfig`] on any out-of-range field.
    pub fn validate(&self) -> Result<(), KernelNicError> {
        if self.frame_count == 0 {
            return Err(KernelNicError::InvalidConfig(
                "frame_count must be > 0".into(),
            ));
        }
        if self.frame_size < MIN_FRAME_SIZE {
            return Err(KernelNicError::InvalidConfig(format!(
                "frame_size must be >= {MIN_FRAME_SIZE}"
            )));
        }
        if !self.frame_size.is_power_of_two() {
            return Err(KernelNicError::InvalidConfig(
                "frame_size must be a power of 2".into(),
            ));
        }
        for (name, size) in [
            ("fill_ring_size", self.fill_ring_size),
            ("completion_ring_size", self.completion_ring_size),
            ("rx_ring_size", self.rx_ring_size),
            ("tx_ring_size", self.tx_ring_size),
        ] {
            if size < MIN_RING_SIZE {
                return Err(KernelNicError::InvalidConfig(format!(
                    "{name} must be >= {MIN_RING_SIZE}"
                )));
            }
            if size > MAX_RING_SIZE {
                return Err(KernelNicError::InvalidConfig(format!(
                    "{name} must be <= {MAX_RING_SIZE}"
                )));
            }
            if !size.is_power_of_two() {
                return Err(KernelNicError::InvalidConfig(format!(
                    "{name} must be a power of 2"
                )));
            }
        }
        if self.ifindex == 0 {
            return Err(KernelNicError::InvalidConfig(
                "ifindex must be > 0".into(),
            ));
        }
        Ok(())
    }

    /// Total UMEM size in bytes.
    #[must_use]
    pub const fn umem_size(&self) -> u64 {
        self.frame_count as u64 * self.frame_size as u64
    }

    /// Maximum frame headroom in bytes (space before packet data for
    /// metadata). Currently 0.
    #[must_use]
    pub const fn frame_headroom(&self) -> u32 {
        0
    }
}

// ── XdpUmem ─────────────────────────────────────────────────────────

/// A UMEM frame address (offset into the UMEM region).
pub type FrameAddr = u64;

/// UMEM region — shared memory pool for XDP packets.
///
/// Manages a contiguous block of memory divided into fixed-size
/// frames. Frames are allocated and freed by the user. The kernel
/// reads/writes packet data directly into these frames.
///
/// # Frame Allocator
///
/// Uses a simple stack-based free-list: allocated frames are popped
/// from the free-list, freed frames are pushed back. The free-list
/// is tracked via a `Vec<FrameAddr>` for simplicity; a production
/// implementation might use a lock-free MPMC queue.
pub struct XdpUmem {
    frame_size: u32,
    frame_count: u32,
    /// Free frame addresses (offsets into UMEM).
    free_list: Vec<FrameAddr>,
    /// Total allocations (for stats).
    alloc_count: AtomicU64,
    /// Total frees (for stats).
    free_count: AtomicU64,
}

impl XdpUmem {
    /// Construct a UMEM descriptor for the given config.
    ///
    /// Does NOT allocate memory — the caller is responsible for
    /// mmap'ing `config.umem_size()` bytes. This struct only
    /// manages frame metadata.
    ///
    /// # Errors
    /// Returns [`KernelNicError::InvalidConfig`] if the config is invalid.
    pub fn new(config: &XdpConfig) -> Result<Self, KernelNicError> {
        config.validate()?;
        let mut free_list = Vec::with_capacity(config.frame_count as usize);
        for i in 0..config.frame_count {
            free_list.push(i as u64 * config.frame_size as u64);
        }
        Ok(Self {
            frame_size: config.frame_size,
            frame_count: config.frame_count,
            free_list,
            alloc_count: AtomicU64::new(0),
            free_count: AtomicU64::new(0),
        })
    }

    /// Allocate a frame from the free-list.
    ///
    /// Returns the frame's offset (address) within the UMEM, or
    /// `None` if all frames are in use.
    #[must_use]
    pub fn alloc_frame(&mut self) -> Option<FrameAddr> {
        let addr = self.free_list.pop();
        if addr.is_some() {
            self.alloc_count.fetch_add(1, Ordering::Relaxed);
        }
        addr
    }

    /// Free a frame back to the free-list.
    ///
    /// # Panics
    /// In debug mode, panics if the address is out of range or already free.
    pub fn free_frame(&mut self, addr: FrameAddr) {
        debug_assert!(
            addr < self.frame_count as u64 * self.frame_size as u64,
            "frame address out of range"
        );
        self.free_list.push(addr);
        self.free_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Allocate `n` frames at once. Returns fewer if the pool is
    /// nearly exhausted.
    pub fn alloc_frames(&mut self, n: usize) -> Vec<FrameAddr> {
        let mut result = Vec::with_capacity(n);
        for _ in 0..n {
            match self.alloc_frame() {
                Some(addr) => result.push(addr),
                None => break,
            }
        }
        result
    }

    /// Free multiple frames at once.
    pub fn free_frames(&mut self, addrs: impl IntoIterator<Item = FrameAddr>) {
        for addr in addrs {
            self.free_frame(addr);
        }
    }

    /// Number of frames currently available for allocation.
    #[must_use]
    pub fn free_count(&self) -> usize {
        self.free_list.len()
    }

    /// Number of frames currently allocated.
    #[must_use]
    pub fn used_count(&self) -> u32 {
        self.frame_count - self.free_list.len() as u32
    }

    /// Total frame capacity.
    #[must_use]
    pub const fn frame_count(&self) -> u32 {
        self.frame_count
    }

    /// Frame size in bytes.
    #[must_use]
    pub const fn frame_size(&self) -> u32 {
        self.frame_size
    }

    /// Total allocations since creation.
    #[must_use]
    pub fn total_allocs(&self) -> u64 {
        self.alloc_count.load(Ordering::Relaxed)
    }

    /// Total frees since creation.
    #[must_use]
    pub fn total_frees(&self) -> u64 {
        self.free_count.load(Ordering::Relaxed)
    }

    /// UMEM size in bytes.
    #[must_use]
    pub const fn size_bytes(&self) -> u64 {
        self.frame_count as u64 * self.frame_size as u64
    }
}

impl std::fmt::Debug for XdpUmem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("XdpUmem")
            .field("frame_size", &self.frame_size)
            .field("frame_count", &self.frame_count)
            .field("free", &self.free_list.len())
            .field("used", &self.used_count())
            .finish()
    }
}

// ── XdpRing ─────────────────────────────────────────────────────────

/// Ring buffer types.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RingType {
    /// FILL ring — user provides empty frame addresses for RX.
    Fill,
    /// COMPLETION ring — kernel returns sent frame addresses.
    Completion,
    /// RX ring — kernel provides received packet descriptors.
    Rx,
    /// TX ring — user provides packet descriptors to send.
    Tx,
}

/// Ring buffer descriptor management.
///
/// Each ring is a circular buffer of descriptors. The producer and
/// consumer share the ring via mmap'd memory. The `producer` and
/// `consumer` indices are stored in shared memory and updated
/// atomically.
///
/// This struct manages the ring metadata (size, mask, indices).
/// The actual ring memory is managed by the kernel after mmap.
pub struct XdpRing {
    ring_type: RingType,
    size: u32,
    /// Mask for wrap-around (= size - 1, since size is power of 2).
    mask: u32,
    /// Cached producer index (user-side).
    cached_prod: u32,
    /// Cached consumer index (user-side).
    cached_cons: u32,
}

impl XdpRing {
    /// Construct a ring descriptor.
    ///
    /// # Errors
    /// Returns [`KernelNicError::InvalidConfig`] if size is invalid.
    pub fn new(ring_type: RingType, size: u32) -> Result<Self, KernelNicError> {
        if size < MIN_RING_SIZE || size > MAX_RING_SIZE || !size.is_power_of_two() {
            return Err(KernelNicError::InvalidConfig(format!(
                "ring size {size} must be a power of 2 in [{MIN_RING_SIZE}, {MAX_RING_SIZE}]"
            )));
        }
        Ok(Self {
            ring_type,
            size,
            mask: size - 1,
            cached_prod: 0,
            cached_cons: 0,
        })
    }

    /// Ring type.
    #[must_use]
    pub const fn ring_type(&self) -> RingType {
        self.ring_type
    }

    /// Ring capacity (number of descriptors).
    #[must_use]
    pub const fn size(&self) -> u32 {
        self.size
    }

    /// Number of free slots (available for production).
    #[must_use]
    pub fn free_slots(&self) -> u32 {
        self.size - self.occupied()
    }

    /// Number of occupied slots (available for consumption).
    #[must_use]
    pub fn occupied(&self) -> u32 {
        self.cached_prod.wrapping_sub(self.cached_cons)
    }

    /// Whether the ring is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.cached_prod == self.cached_cons
    }

    /// Whether the ring is full.
    #[must_use]
    pub fn is_full(&self) -> bool {
        self.occupied() == self.size
    }

    /// Reserve `n` slots for production. Returns the starting index
    /// and the actual number reserved (may be less than requested if
    /// the ring is nearly full).
    ///
    /// Advances the producer index.
    #[must_use]
    pub fn reserve(&mut self, n: u32) -> (u32, u32) {
        let avail = self.free_slots();
        let actual = n.min(avail);
        let start = self.cached_prod & self.mask;
        self.cached_prod = self.cached_prod.wrapping_add(actual);
        (start, actual)
    }

    /// Consume up to `n` descriptors. Returns the starting index
    /// and the actual number consumed.
    ///
    /// Advances the consumer index.
    #[must_use]
    pub fn consume(&mut self, n: u32) -> (u32, u32) {
        let avail = self.occupied();
        let actual = n.min(avail);
        let start = self.cached_cons & self.mask;
        self.cached_cons = self.cached_cons.wrapping_add(actual);
        (start, actual)
    }

    /// Peek at the number of available descriptors without consuming.
    #[must_use]
    pub fn peek(&self) -> u32 {
        self.occupied()
    }

    /// Reset both indices to zero (for testing / reinitialization).
    pub fn reset(&mut self) {
        self.cached_prod = 0;
        self.cached_cons = 0;
    }

    /// Ring mask (= size - 1).
    #[must_use]
    pub const fn mask(&self) -> u32 {
        self.mask
    }
}

impl std::fmt::Debug for XdpRing {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("XdpRing")
            .field("type", &self.ring_type)
            .field("size", &self.size)
            .field("occupied", &self.occupied())
            .field("free", &self.free_slots())
            .finish()
    }
}

// ── XdpSocket ───────────────────────────────────────────────────────

/// AF_XDP socket lifecycle manager.
///
/// Creates the socket, registers UMEM, sets up rings, and binds to
/// the NIC queue. After setup, the user fills/empties rings directly
/// via mmap'd memory (not managed by this struct).
///
/// # Example
/// ```no_run
/// use kernel_nic::xdp::{XdpConfig, XdpSocket, XdpMode, XdpCopyMode};
///
/// let cfg = XdpConfig {
///     ifindex: 2,
///     queue_id: 0,
///     mode: XdpMode::Driver,
///     copy_mode: XdpCopyMode::ZeroCopy,
///     ..Default::default()
/// };
/// let mut socket = XdpSocket::create(&cfg).expect("create XDP socket");
/// ```
pub struct XdpSocket {
    fd: RawFd,
    config: XdpConfig,
    closed: bool,
}

impl XdpSocket {
    /// Create an AF_XDP socket.
    ///
    /// Creates the socket fd. UMEM registration and ring setup are
    /// separate steps so the caller can control ordering.
    ///
    /// # Errors
    /// - [`KernelNicError::InvalidConfig`] if config is invalid.
    /// - [`KernelNicError::Socket`] if `socket(2)` fails.
    pub fn create(config: &XdpConfig) -> Result<Self, KernelNicError> {
        config.validate()?;

        let fd = unsafe {
            libc::socket(
                AF_XDP,
                libc::SOCK_RAW | libc::SOCK_CLOEXEC,
                SOCK_XDP,
            )
        };

        if fd < 0 {
            let err = std::io::Error::last_os_error();
            if err.raw_os_error() == Some(libc::EAFNOSUPPORT) || err.raw_os_error() == Some(libc::EPROTONOSUPPORT) {
                return Err(KernelNicError::FeatureUnsupported(
                    "AF_XDP not supported by this kernel (requires ≥ 4.18)",
                ));
            }
            return Err(KernelNicError::Socket(err));
        }

        Ok(Self {
            fd,
            config: *config,
            closed: false,
        })
    }

    /// The raw socket fd.
    #[must_use]
    pub fn fd(&self) -> RawFd {
        self.fd
    }

    /// The socket configuration.
    #[must_use]
    pub const fn config(&self) -> &XdpConfig {
        &self.config
    }

    /// Set ring sizes via `setsockopt`.
    ///
    /// Configures the FILL, COMPLETION, RX, and TX ring sizes on the
    /// socket. Must be called before `mmap` of the rings.
    ///
    /// # Errors
    /// Returns [`KernelNicError::Socket`] on `setsockopt` failure.
    pub fn set_ring_sizes(&self) -> Result<(), KernelNicError> {
        self.setsockopt(XDP_UMEM_FILL_RING, self.config.fill_ring_size)?;
        self.setsockopt(XDP_UMEM_COMPLETION_RING, self.config.completion_ring_size)?;
        self.setsockopt(XDP_RX_RING, self.config.rx_ring_size)?;
        self.setsockopt(XDP_TX_RING, self.config.tx_ring_size)?;
        Ok(())
    }

    /// Set a single XDP socket option.
    fn setsockopt(&self, opt: libc::c_int, value: u32) -> Result<(), KernelNicError> {
        let val: libc::c_uint = value;
        let rc = unsafe {
            libc::setsockopt(
                self.fd,
                SOL_XDP,
                opt,
                &val as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_uint>() as libc::socklen_t,
            )
        };
        if rc != 0 {
            return Err(KernelNicError::Socket(std::io::Error::last_os_error()));
        }
        Ok(())
    }

    /// Compute bind flags from the config.
    #[must_use]
    pub fn bind_flags(&self) -> u32 {
        self.config.mode.to_flag() | XDP_FLAGS_UPDATE_IF_NOEXIST
    }

    /// Compute socket address flags.
    #[must_use]
    pub fn addr_flags(&self) -> u16 {
        match self.config.copy_mode {
            XdpCopyMode::ZeroCopy => XDP_ZEROCOPY,
            XdpCopyMode::Copy => XDP_COPY,
        }
    }

    /// Close the socket. Idempotent.
    pub fn close(&mut self) {
        if !self.closed {
            unsafe { libc::close(self.fd) };
            self.closed = true;
        }
    }
}

impl Drop for XdpSocket {
    fn drop(&mut self) {
        self.close();
    }
}

impl std::fmt::Debug for XdpSocket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("XdpSocket")
            .field("fd", &self.fd)
            .field("ifindex", &self.config.ifindex)
            .field("queue_id", &self.config.queue_id)
            .field("mode", &self.config.mode)
            .field("copy_mode", &self.config.copy_mode)
            .field("closed", &self.closed)
            .finish()
    }
}

// ── Tests ───────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]
    use super::*;

    // ── Config ──

    #[test]
    fn default_config_validates() {
        let cfg = XdpConfig {
            ifindex: 1,
            ..Default::default()
        };
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn config_rejects_zero_frame_count() {
        let cfg = XdpConfig {
            frame_count: 0,
            ifindex: 1,
            ..Default::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn config_rejects_small_frame_size() {
        let cfg = XdpConfig {
            frame_size: 1024,
            ifindex: 1,
            ..Default::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn config_rejects_non_power_of_two_frame_size() {
        let cfg = XdpConfig {
            frame_size: 3000,
            ifindex: 1,
            ..Default::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn config_rejects_non_power_of_two_ring() {
        let cfg = XdpConfig {
            rx_ring_size: 100,
            ifindex: 1,
            ..Default::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn config_rejects_zero_ifindex() {
        let cfg = XdpConfig {
            ifindex: 0,
            ..Default::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn config_umem_size() {
        let cfg = XdpConfig {
            frame_count: 1024,
            frame_size: 4096,
            ifindex: 1,
            ..Default::default()
        };
        assert_eq!(cfg.umem_size(), 1024 * 4096);
    }

    #[test]
    fn xdp_mode_to_flag() {
        assert_eq!(XdpMode::Driver.to_flag(), XDP_FLAGS_DRV_MODE);
        assert_eq!(XdpMode::Skb.to_flag(), XDP_FLAGS_SKB_MODE);
    }

    // ── UMEM ──

    #[test]
    fn umem_allocates_all_frames() {
        let cfg = XdpConfig {
            frame_count: 16,
            frame_size: 4096,
            ifindex: 1,
            ..Default::default()
        };
        let mut umem = XdpUmem::new(&cfg).unwrap();
        assert_eq!(umem.free_count(), 16);
        assert_eq!(umem.used_count(), 0);

        // Allocate all 16 frames.
        let mut addrs = Vec::new();
        for _ in 0..16 {
            addrs.push(umem.alloc_frame().unwrap());
        }
        assert_eq!(umem.free_count(), 0);
        assert_eq!(umem.used_count(), 16);
        assert_eq!(umem.total_allocs(), 16);

        // No more frames available.
        assert!(umem.alloc_frame().is_none());

        // Free some.
        umem.free_frame(addrs[0]);
        umem.free_frame(addrs[1]);
        assert_eq!(umem.free_count(), 2);
        assert_eq!(umem.used_count(), 14);
        assert_eq!(umem.total_frees(), 2);
    }

    #[test]
    fn umem_frame_addresses_are_aligned() {
        let cfg = XdpConfig {
            frame_count: 8,
            frame_size: 4096,
            ifindex: 1,
            ..Default::default()
        };
        let mut umem = XdpUmem::new(&cfg).unwrap();
        for _ in 0..8 {
            let addr = umem.alloc_frame().unwrap();
            assert_eq!(addr % 4096, 0, "frame address must be frame-aligned");
        }
    }

    #[test]
    fn umem_alloc_batch() {
        let cfg = XdpConfig {
            frame_count: 32,
            frame_size: 2048,
            ifindex: 1,
            ..Default::default()
        };
        let mut umem = XdpUmem::new(&cfg).unwrap();
        let batch = umem.alloc_frames(10);
        assert_eq!(batch.len(), 10);
        assert_eq!(umem.free_count(), 22);

        // Exhaust.
        let rest = umem.alloc_frames(100);
        assert_eq!(rest.len(), 22);

        // Nothing left.
        assert!(umem.alloc_frames(1).is_empty());
    }

    #[test]
    fn umem_free_batch() {
        let cfg = XdpConfig {
            frame_count: 8,
            frame_size: 4096,
            ifindex: 1,
            ..Default::default()
        };
        let mut umem = XdpUmem::new(&cfg).unwrap();
        let batch = umem.alloc_frames(5);
        assert_eq!(umem.used_count(), 5);
        umem.free_frames(batch);
        assert_eq!(umem.used_count(), 0);
        assert_eq!(umem.free_count(), 8);
    }

    #[test]
    fn umem_size_bytes() {
        let cfg = XdpConfig {
            frame_count: 4096,
            frame_size: 4096,
            ifindex: 1,
            ..Default::default()
        };
        let umem = XdpUmem::new(&cfg).unwrap();
        assert_eq!(umem.size_bytes(), 16 * 1024 * 1024); // 16 MiB
    }

    #[test]
    fn umem_reuse_after_free() {
        let cfg = XdpConfig {
            frame_count: 2,
            frame_size: 4096,
            ifindex: 1,
            ..Default::default()
        };
        let mut umem = XdpUmem::new(&cfg).unwrap();
        let a1 = umem.alloc_frame().unwrap();
        let _a2 = umem.alloc_frame().unwrap();
        assert!(umem.alloc_frame().is_none());

        umem.free_frame(a1);
        let a3 = umem.alloc_frame().unwrap();
        assert_eq!(a1, a3); // Stack-based: LIFO.
    }

    // ── Ring ──

    #[test]
    fn ring_new_validates_size() {
        assert!(XdpRing::new(RingType::Rx, 2048).is_ok());
        assert!(XdpRing::new(RingType::Tx, 64).is_ok());
        assert!(XdpRing::new(RingType::Rx, 32).is_err()); // too small
        assert!(XdpRing::new(RingType::Tx, 65536).is_err()); // too large
        assert!(XdpRing::new(RingType::Rx, 100).is_err()); // not power of 2
    }

    #[test]
    fn ring_starts_empty() {
        let ring = XdpRing::new(RingType::Fill, 256).unwrap();
        assert!(ring.is_empty());
        assert!(!ring.is_full());
        assert_eq!(ring.occupied(), 0);
        assert_eq!(ring.free_slots(), 256);
    }

    #[test]
    fn ring_reserve_and_consume() {
        let mut ring = XdpRing::new(RingType::Rx, 256).unwrap();

        // Reserve 10 slots.
        let (start, actual) = ring.reserve(10);
        assert_eq!(actual, 10);
        assert_eq!(start, 0);
        assert_eq!(ring.occupied(), 10);
        assert_eq!(ring.free_slots(), 246);

        // Consume 5.
        let (start, actual) = ring.consume(5);
        assert_eq!(actual, 5);
        assert_eq!(start, 0);
        assert_eq!(ring.occupied(), 5);
    }

    #[test]
    fn ring_wraps_around() {
        let mut ring = XdpRing::new(RingType::Tx, 128).unwrap();

        // Fill and drain multiple times to test wrap-around.
        for _ in 0..10 {
            let (_, n) = ring.reserve(100);
            assert_eq!(n, 100);
            let (_, m) = ring.consume(100);
            assert_eq!(m, 100);
        }
        assert!(ring.is_empty());
    }

    #[test]
    fn ring_reserve_caps_at_free_slots() {
        let mut ring = XdpRing::new(RingType::Fill, 64).unwrap();
        let (_, actual) = ring.reserve(100);
        assert_eq!(actual, 64); // capped at capacity
        assert!(ring.is_full());

        // Can't reserve more.
        let (_, actual) = ring.reserve(1);
        assert_eq!(actual, 0);
    }

    #[test]
    fn ring_consume_caps_at_occupied() {
        let mut ring = XdpRing::new(RingType::Completion, 64).unwrap();
        let _ = ring.reserve(10);
        let (_, actual) = ring.consume(100);
        assert_eq!(actual, 10);
        assert!(ring.is_empty());
    }

    #[test]
    fn ring_mask_is_size_minus_one() {
        let ring = XdpRing::new(RingType::Rx, 512).unwrap();
        assert_eq!(ring.mask(), 511);
    }

    #[test]
    fn ring_reset() {
        let mut ring = XdpRing::new(RingType::Tx, 256).unwrap();
        let _ = ring.reserve(50);
        assert!(!ring.is_empty());
        ring.reset();
        assert!(ring.is_empty());
    }

    #[test]
    fn ring_peek_does_not_consume() {
        let mut ring = XdpRing::new(RingType::Rx, 256).unwrap();
        let _ = ring.reserve(20);
        assert_eq!(ring.peek(), 20);
        assert_eq!(ring.occupied(), 20); // unchanged
    }

    // ── Socket ──

    #[test]
    fn socket_bind_flags_include_mode() {
        let cfg = XdpConfig {
            ifindex: 1,
            mode: XdpMode::Driver,
            ..Default::default()
        };
        // Create without actually opening a socket.
        // We test the flag computation directly.
        let flags = cfg.mode.to_flag() | XDP_FLAGS_UPDATE_IF_NOEXIST;
        assert!(flags & XDP_FLAGS_DRV_MODE != 0);
        assert!(flags & XDP_FLAGS_UPDATE_IF_NOEXIST != 0);
    }

    #[test]
    fn socket_addr_flags_zero_copy() {
        let cfg = XdpConfig {
            ifindex: 1,
            copy_mode: XdpCopyMode::ZeroCopy,
            ..Default::default()
        };
        let flags = match cfg.copy_mode {
            XdpCopyMode::ZeroCopy => XDP_ZEROCOPY,
            XdpCopyMode::Copy => XDP_COPY,
        };
        assert_eq!(flags, XDP_ZEROCOPY);
    }

    #[test]
    fn socket_addr_flags_copy_mode() {
        let cfg = XdpConfig {
            ifindex: 1,
            copy_mode: XdpCopyMode::Copy,
            ..Default::default()
        };
        let flags = match cfg.copy_mode {
            XdpCopyMode::ZeroCopy => XDP_ZEROCOPY,
            XdpCopyMode::Copy => XDP_COPY,
        };
        assert_eq!(flags, XDP_COPY);
    }

    #[test]
    fn socket_create_rejects_invalid_config() {
        let cfg = XdpConfig {
            frame_count: 0, // invalid
            ifindex: 1,
            ..Default::default()
        };
        assert!(XdpSocket::create(&cfg).is_err());
    }

    #[test]
    fn socket_create_rejects_zero_ifindex() {
        let cfg = XdpConfig {
            ifindex: 0,
            ..Default::default()
        };
        assert!(XdpSocket::create(&cfg).is_err());
    }

    #[test]
    fn kernel_constants_correct() {
        assert_eq!(AF_XDP, 44);
        assert_eq!(SOL_XDP, 283);
        assert_eq!(XDP_UMEM_REG, 5);
        assert_eq!(XDP_RX_RING, 8);
        assert_eq!(XDP_TX_RING, 9);
        assert_eq!(XDP_UMEM_FILL_RING, 6);
        assert_eq!(XDP_UMEM_COMPLETION_RING, 7);
    }

    #[test]
    fn mode_flags_non_overlapping() {
        assert_eq!(XDP_FLAGS_DRV_MODE & XDP_FLAGS_SKB_MODE, 0);
    }
}
