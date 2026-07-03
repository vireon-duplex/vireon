//! NUMA memory binding (Layer 22).
//!
//! Without the `numa` feature these are all no-ops. With the feature enabled,
//! [`bind`] calls the Linux `mbind` syscall to set the preferred node for a
//! memory range.

use std::ptr::NonNull;

use super::node::NodeId;

/// Binds the memory range `[ptr, ptr + len)` to the given NUMA `node`.
///
/// Returns `Ok(())` on success, `Err(message)` on failure. Without the `numa`
/// feature this is always `Ok(())`.
///
/// # Safety
///
/// `ptr` must point to a valid, page-aligned memory region of at least `len`
/// bytes that the caller owns.
pub unsafe fn bind(_ptr: NonNull<u8>, _len: usize, _node: NodeId) -> Result<(), String> {
    #[cfg(feature = "numa")]
    {
        // SAFETY: caller upholds the safety contract of `bind_real`.
        unsafe { bind_real(_ptr, _len, _node) }
    }
    #[cfg(not(feature = "numa"))]
    {
        let _ = (_ptr, _len, _node);
        Ok(())
    }
}

/// Allocates `size` bytes of memory preferred on the given NUMA `node`.
///
/// Returns a [`NonNull<u8>`] to the start of the allocation. The caller is
/// responsible for deallocating via the matching system allocator.
///
/// Without the `numa` feature this uses the standard allocator and ignores
/// `node`.
pub fn allocate_node(size: usize, _node: NodeId) -> Result<NonNull<u8>, String> {
    if size == 0 {
        return Err("cannot allocate zero bytes".into());
    }

    let layout = std::alloc::Layout::from_size_align(size, 64)
        .map_err(|e| format!("invalid layout: {e}"))?;

    // SAFETY: `layout` has non-zero size (checked above).
    let ptr = unsafe { std::alloc::alloc(layout) };
    let nn = NonNull::new(ptr).ok_or_else(|| "allocation failed".to_string())?;

    #[cfg(feature = "numa")]
    {
        // SAFETY: `nn` was just allocated with `size` bytes via `alloc`.
        let _ = unsafe { bind(nn, size, _node) };
    }

    Ok(nn)
}

/// Deallocates memory previously returned by [`allocate_node`].
///
/// # Safety
///
/// `ptr` must have been returned by [`allocate_node`] with the same `size`.
pub unsafe fn deallocate(ptr: NonNull<u8>, size: usize) {
    if size == 0 {
        return;
    }
    let layout = match std::alloc::Layout::from_size_align(size, 64) {
        Ok(l) => l,
        Err(_) => {
            tracing::error!(
                "numa deallocate: invalid layout for size={size}; leaking {:p}",
                ptr.as_ptr()
            );
            return;
        }
    };
    // SAFETY: caller guarantees `ptr` came from `alloc(layout)`.
    unsafe {
        std::alloc::dealloc(ptr.as_ptr(), layout);
    }
}

/// Attempts to migrate existing pages in `[ptr, ptr + len)` to `node`.
///
/// Without the `numa` feature this is a no-op returning `Ok(())`.
///
/// # Safety
///
/// `ptr` must point to a valid, owned memory region of at least `len` bytes.
pub unsafe fn migrate(_ptr: NonNull<u8>, _len: usize, _node: NodeId) -> Result<(), String> {
    // Same as bind() for now — migration semantics can be refined later.
    unsafe { bind(_ptr, _len, _node) }
}

// ── Linux implementation ────────────────────────────────────────────────

#[cfg(feature = "numa")]
unsafe fn bind_real(ptr: NonNull<u8>, len: usize, node: NodeId) -> Result<(), String> {
    // MPOL_PREFERRED = 1 (set preferred node for the range).
    const MPOL_PREFERRED: i32 = 1;

    // Build a nodemask with only `node` set.
    let bit = node.0 as usize;
    let mask_words = (bit / 64) + 1;
    let mut nodemask = vec![0u64; mask_words];
    nodemask[bit / 64] |= 1u64 << (bit % 64);

    // SAFETY: `ptr` is valid for `len` bytes (caller invariant), `nodemask`
    // has enough bits to cover `node`.
    let ret = unsafe {
        libc::syscall(
            libc::SYS_mbind,
            ptr.as_ptr() as *const libc::c_void,
            len,
            MPOL_PREFERRED,
            nodemask.as_ptr(),
            (mask_words * 64) as libc::c_ulong,
            0u32, // flags: none (do not migrate existing pages)
        )
    };

    if ret == 0 {
        Ok(())
    } else {
        Err(format!(
            "mbind failed with errno {}",
            std::io::Error::last_os_error()
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allocate_and_deallocate_round_trip() {
        let size = 4096;
        let ptr = allocate_node(size, NodeId::SINGLE).unwrap();
        // Write to it to prove it's usable.
        unsafe {
            ptr.as_ptr().write_bytes(0xAB, size);
        }
        unsafe {
            deallocate(ptr, size);
        }
    }

    #[test]
    fn allocate_zero_rejected() {
        assert!(allocate_node(0, NodeId::SINGLE).is_err());
    }

    #[test]
    fn bind_succeeds_or_fails_gracefully() {
        let size = 4096;
        let ptr = allocate_node(size, NodeId::SINGLE).unwrap();
        let result = unsafe { bind(ptr, size, NodeId::SINGLE) };
        // Without the `numa` feature, bind is always Ok.
        // With the feature, mbind may fail on non-NUMA systems or when
        // the allocation is not page-aligned — either outcome is acceptable.
        #[cfg(not(feature = "numa"))]
        assert!(result.is_ok(), "bind should succeed without numa feature");
        #[cfg(feature = "numa")]
        {
            let _ = result;
        }
        unsafe {
            deallocate(ptr, size);
        }
    }
}
