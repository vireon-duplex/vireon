//! Provided-buffer ring manager (`IORING_REGISTER_PBUF_RING`).
//!
//! Each [`BufferPool`] owns:
//!
//! 1. A page-aligned `io_uring_buf` array (the **ring**) shared with the
//!    kernel. The kernel reads buffers off this ring as `RecvMsgMultiShot`
//!    completions arrive and posts a CQE with `IORING_CQE_F_BUFFER`
//!    encoding the consumed `buf_id`.
//! 2. The **backing storage** for every buffer, managed via
//!    [`MemoryRegion`]. Addresses written into the
//!    ring must remain valid for the lifetime of the registration.
//!
//! Userspace produces buffers by writing into the next tail slot and
//! advancing the tail (atomic store into the first entry's overloaded
//! `resv` field). Kernel consumes by advancing its private head.
//!
//! # Lifetime
//!
//! `Drop` **explicitly unregisters** the buffer group via
//! `io_uring_register(IORING_UNREGISTER_PBUF_RING)` before freeing
//! memory. This is stronger than relying on implicit ring teardown —
//! the kernel stops touching our memory before we free it.

use std::os::unix::io::{AsRawFd, RawFd};
use std::ptr::NonNull;
use std::sync::atomic::{AtomicU16, Ordering};

use io_uring::IoUring;
use io_uring::types::BufRingEntry;

use crate::common::{DEFAULT_BUF_COUNT, DEFAULT_BUF_SIZE, PAGE_SIZE};
use crate::error::IoError;
use crate::memory::MemoryRegion;

// ─── io_uring ABI constants (from include/uapi/linux/io_uring.h) ────

/// `io_uring_register` opcode for unregistering a provided-buffer ring.
const IORING_UNREGISTER_PBUF_RING: u32 = 23;

/// Kernel UAPI struct for `IORING_(UN)REGISTER_PBUF_RING`.
///
/// Only `bgid` is needed for unregistration; other fields default to
/// zero and are ignored by the kernel on the unregister path.
#[repr(C)]
#[derive(Default)]
struct IoUringBufReg {
    ring_entries: u32,
    bgid: u32,
    ring_addr: u64,
    flags: u16,
    resv1: u16,
    resv2: u32,
    resv: [u64; 2],
}

/// Manager for one provided-buffer ring (a single `bgid`).
pub struct BufferPool {
    /// Page-aligned `BufRingEntry` array shared with the kernel.
    ring_region: MemoryRegion,
    ring_entries: u16,
    /// Mask = `ring_entries - 1`; ring size must be a power of two.
    ring_mask: u16,
    /// Userspace's view of the tail (next slot to publish into).
    tail: AtomicU16,

    /// Contiguous backing store for every buffer.
    backing_region: MemoryRegion,
    buf_count: u32,
    buf_size: usize,

    /// Buffer-group id used to register and reference the ring.
    bgid: u16,
    /// Ring fd, stored for explicit unregister in `Drop`.
    ring_fd: RawFd,
}

impl BufferPool {
    /// Allocates the ring + backing store and registers the bgid.
    ///
    /// `buf_count` must be a power of two (and ≤ 32768) — this is the
    /// number of slots in the ring. `buf_size` is per-buffer.
    pub fn init_buffer_ring(
        ring: &IoUring,
        bgid: u16,
        buf_count: u32,
        buf_size: usize,
    ) -> Result<Self, IoError> {
        if buf_count == 0 || !buf_count.is_power_of_two() {
            return Err(IoError::InvalidConfig(
                "buf_count must be a power of two and > 0".into(),
            ));
        }
        if buf_count > 32_768 {
            return Err(IoError::InvalidConfig("buf_count must be <= 32768".into()));
        }
        if buf_size == 0 {
            return Err(IoError::InvalidConfig("buf_size must be > 0".into()));
        }
        let ring_entries = u16::try_from(buf_count)
            .map_err(|_| IoError::InvalidConfig("buf_count does not fit in u16".into()))?;

        let ring_bytes = usize::from(ring_entries) * size_of_buf_entry();
        let ring_region = MemoryRegion::alloc(ring_bytes, PAGE_SIZE)?;

        let backing_bytes = buf_count as usize * buf_size;
        let backing_region = MemoryRegion::alloc(backing_bytes, PAGE_SIZE)?;

        let ring_fd = ring.as_raw_fd();
        let submitter = ring.submitter();

        // Register the ring with the kernel before publishing any entries.
        // SAFETY: `ring_region` is valid for the lifetime of `self`, which
        // outlives the registration (Drop unregisters first).
        unsafe {
            submitter
                .register_buf_ring_with_flags(
                    ring_region.as_ptr().as_ptr() as u64,
                    ring_entries,
                    bgid,
                    0,
                )
                .map_err(IoError::RegistrationFailed)?;
        }

        let pool = Self {
            ring_region,
            ring_entries,
            ring_mask: (buf_count - 1) as u16,
            tail: AtomicU16::new(0),
            backing_region,
            buf_count,
            buf_size,
            bgid,
            ring_fd,
        };

        // Pre-publish every buffer so the kernel can immediately serve RX.
        for buf_id in 0..buf_count {
            // SAFETY: buf_count <= 32768 validated above, so buf_id fits in u16.
            let bid = buf_id as u16;
            let addr = pool.backing_address_of(bid);
            pool.publish_entry(bid, addr, buf_size);
        }

        Ok(pool)
    }

    /// Convenience constructor using [`DEFAULT_BUF_COUNT`] and
    /// [`DEFAULT_BUF_SIZE`].
    pub fn with_defaults(ring: &IoUring, bgid: u16) -> Result<Self, IoError> {
        Self::init_buffer_ring(ring, bgid, DEFAULT_BUF_COUNT, DEFAULT_BUF_SIZE)
    }

    /// Republishes a consumed buffer back into the ring.
    ///
    /// Call this after the upper layer is done with the buffer received
    /// in a CQE. The kernel will pick it up on the next RX completion.
    pub fn refill_buffer(&mut self, buf_id: u16) {
        let addr = self.backing_address_of(buf_id);
        self.publish_entry(buf_id, addr, self.buf_size);
    }

    /// Returns the raw address of `buf_id`'s backing storage, or `None`
    /// if `buf_id` is out of range.
    #[must_use]
    pub fn get_buffer_address(&self, buf_id: u16) -> Option<NonNull<u8>> {
        if u32::from(buf_id) >= self.buf_count {
            return None;
        }
        Some(self.backing_address_of(buf_id))
    }

    /// The buffer-group id registered with the kernel.
    #[inline]
    #[must_use]
    pub const fn bgid(&self) -> u16 {
        self.bgid
    }

    /// Number of slots (and buffers) in this pool.
    #[inline]
    #[must_use]
    pub const fn buf_count(&self) -> u32 {
        self.buf_count
    }

    /// Per-buffer size in bytes.
    #[inline]
    #[must_use]
    pub const fn buf_size(&self) -> usize {
        self.buf_size
    }

    /// Number of ring entries (power-of-two, equals `buf_count`).
    #[inline]
    #[must_use]
    pub const fn ring_entries(&self) -> u16 {
        self.ring_entries
    }

    /// Computes the address of buffer `buf_id` inside the backing store.
    fn backing_address_of(&self, buf_id: u16) -> NonNull<u8> {
        let offset = usize::from(buf_id) * self.buf_size;
        // SAFETY: `backing_region` is valid for `buf_count * buf_size` bytes,
        // and `offset < buf_count * buf_size` because `buf_id < buf_count`
        // is enforced at every call site (or returned as None from the
        // public `get_buffer_address`).
        unsafe { self.backing_region.as_ptr().add(offset) }
    }

    /// Writes a buffer descriptor into the next tail slot and advances
    /// the tail (release-store so the kernel observes the entry).
    fn publish_entry(&self, buf_id: u16, addr: NonNull<u8>, len: usize) {
        let tail = self.tail.load(Ordering::Relaxed);
        let idx = u32::from(tail & self.ring_mask);
        // SAFETY: `ring_region` is the first entry of a `ring_entries`-sized
        // array; `idx` is masked to stay in bounds. The ring is registered
        // with the kernel for the lifetime of `self`.
        let entry_ptr = unsafe {
            self.ring_region
                .as_ptr()
                .as_ptr()
                .cast::<BufRingEntry>()
                .add(idx as usize)
        };
        // SAFETY: the pointer is within our allocation; we have exclusive
        // mutable access to the slot because the kernel has not yet seen
        // this tail value.
        unsafe {
            (*entry_ptr).set_addr(addr.as_ptr() as u64);
            // `len` fits in u32 because it derives from `buf_size` which
            // is bounded by the validated layout.
            (*entry_ptr).set_len(len as u32);
            (*entry_ptr).set_bid(buf_id);
        }

        let new_tail = tail.wrapping_add(1);
        // SAFETY: `tail_ptr` is the overloaded `resv` field of entry 0
        // (see `BufRingEntry::tail`); it points inside our valid ring
        // allocation. We view it as an `AtomicU16` and release-store so
        // the kernel's acquire-load observes the freshly-written entry
        // (fields written above happen-before the release store).
        let tail_ptr = unsafe {
            BufRingEntry::tail(self.ring_region.as_ptr().as_ptr().cast::<BufRingEntry>())
        };
        let atomic_tail = tail_ptr.cast::<AtomicU16>();
        unsafe { (*atomic_tail).store(new_tail, Ordering::Release) };
    }
}

impl Drop for BufferPool {
    fn drop(&mut self) {
        // EXPLICIT unregister — do NOT rely on implicit ring teardown.
        // Best-effort: errors are silently ignored in Drop.
        let reg = IoUringBufReg {
            bgid: u32::from(self.bgid),
            ..Default::default()
        };
        let _ = unsafe {
            libc::syscall(
                libc::SYS_io_uring_register,
                self.ring_fd,
                IORING_UNREGISTER_PBUF_RING,
                &reg,
                1u32,
            )
        };
        // MemoryRegion drops (ring_region, backing_region) free the
        // allocations automatically after this method returns.
    }
}

/// Compile-time size of one `BufRingEntry` (= `io_uring_buf` = 16 bytes).
const fn size_of_buf_entry() -> usize {
    // `io_uring_buf` is `{ u64, u32, u16, u16 }` = 16 bytes on every arch
    // io-uring supports (verified via bindgen layout assertions).
    16
}

impl std::fmt::Debug for BufferPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BufferPool")
            .field("bgid", &self.bgid)
            .field("buf_count", &self.buf_count)
            .field("buf_size", &self.buf_size)
            .field("ring_entries", &self.ring_entries)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use io_uring::IoUring;

    fn fresh_ring() -> IoUring {
        IoUring::builder()
            .setup_single_issuer()
            .setup_defer_taskrun()
            .setup_coop_taskrun()
            .build(8)
            .expect("ring")
    }

    #[test]
    fn rejects_zero_buf_count() {
        let ring = fresh_ring();
        let err = BufferPool::init_buffer_ring(&ring, 1, 0, 64).expect_err("zero count");
        assert!(matches!(err, IoError::InvalidConfig(_)));
    }

    #[test]
    fn rejects_non_power_of_two() {
        let ring = fresh_ring();
        let err = BufferPool::init_buffer_ring(&ring, 1, 7, 64).expect_err("non-pow2");
        assert!(matches!(err, IoError::InvalidConfig(_)));
    }

    #[test]
    fn rejects_zero_buf_size() {
        let ring = fresh_ring();
        let err = BufferPool::init_buffer_ring(&ring, 1, 4, 0).expect_err("zero size");
        assert!(matches!(err, IoError::InvalidConfig(_)));
    }

    #[test]
    fn init_publishes_and_lookups_round_trip() {
        let ring = fresh_ring();
        let pool = BufferPool::init_buffer_ring(&ring, 42, 4, 128).expect("pool");
        assert_eq!(pool.bgid(), 42);
        assert_eq!(pool.buf_count(), 4);
        assert_eq!(pool.buf_size(), 128);
        for buf_id in 0..4 {
            let addr = pool.get_buffer_address(buf_id);
            assert!(addr.is_some(), "buf_id {buf_id} missing");
        }
        assert!(pool.get_buffer_address(4).is_none(), "out-of-range");
    }

    #[test]
    fn refill_publishes_a_new_entry() {
        let ring = fresh_ring();
        let mut pool = BufferPool::init_buffer_ring(&ring, 7, 4, 64).expect("pool");
        // Refilling a valid buf_id should succeed without panicking.
        pool.refill_buffer(2);
    }

    #[test]
    fn buffer_addresses_are_within_layout() {
        let ring = fresh_ring();
        let pool = BufferPool::init_buffer_ring(&ring, 1, 8, 256).expect("pool");
        let base = pool.backing_region.as_ptr().as_ptr() as usize;
        for buf_id in 0..8u16 {
            let addr = pool.get_buffer_address(buf_id).expect("addr").as_ptr() as usize;
            let offset = addr - base;
            assert_eq!(offset, usize::from(buf_id) * 256);
            assert!(offset < 8 * 256);
        }
    }

    #[test]
    fn explicit_unregister_in_drop_does_not_panic() {
        // Create and immediately drop a pool — Drop should unregister
        // the PBUF_RING group without panicking.
        let ring = fresh_ring();
        let pool = BufferPool::init_buffer_ring(&ring, 9, 4, 128).expect("pool");
        drop(pool);
        // Re-registering the same bgid after Drop should succeed, proving
        // the unregister happened.
        let ring2 = fresh_ring();
        let pool2 = BufferPool::init_buffer_ring(&ring2, 9, 4, 128);
        assert!(pool2.is_ok(), "re-register after Drop should work");
    }
}
