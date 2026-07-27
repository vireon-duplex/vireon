//! Buffer validation utilities (Layer 21).
//!
//! [`Validator`] maintains a side table of live slot addresses so that
//! double-free and unknown-pointer conditions can be detected **without**
//! writing into the buffer's data region (which is user-visible). All state
//! lives in a [`HashSet`] keyed by address.

use std::collections::HashSet;
use std::sync::Mutex;

/// Side-table validator for live buffer pointers.
///
/// On every successful acquire [`track`](Self::track) records the slot
/// address; on every release [`release`](Self::release) clears it. A
/// double-free shows up as `release` returning `false` (the pointer was
/// already released), and an unknown pointer similarly returns `false`.
pub struct Validator {
    live: Mutex<HashSet<usize>>,
}

impl Validator {
    /// Creates an empty validator.
    pub fn new() -> Self {
        Self {
            live: Mutex::new(HashSet::new()),
        }
    }

    /// Records `addr` as live.
    ///
    /// Returns `false` if the address was already tracked (double-alloc
    /// indicator — the same slot handed out twice without an intervening
    /// release).
    pub fn track(&self, addr: usize) -> bool {
        lock(&self.live).insert(addr)
    }

    /// Marks `addr` as released.
    ///
    /// Returns `false` if the address was not tracked (double-free or
    /// unknown pointer).
    pub fn release(&self, addr: usize) -> bool {
        lock(&self.live).remove(&addr)
    }

    /// Number of currently-live pointers.
    pub fn count(&self) -> usize {
        lock(&self.live).len()
    }

    /// Removes all tracked pointers.
    pub fn clear(&self) {
        lock(&self.live).clear();
    }
}

impl Default for Validator {
    fn default() -> Self {
        Self::new()
    }
}

/// Locks a mutex, recovering from poison.
fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    match m.lock() {
        Ok(g) => g,
        Err(poisoned) => {
            tracing::error!("validator mutex poisoned; recovering");
            poisoned.into_inner()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn track_then_release_reports_zero() {
        let v = Validator::new();
        assert!(v.track(0x1000));
        assert_eq!(v.count(), 1);
        assert!(v.release(0x1000));
        assert_eq!(v.count(), 0);
    }

    #[test]
    fn double_track_returns_false() {
        let v = Validator::new();
        assert!(v.track(0x1000));
        assert!(!v.track(0x1000));
    }

    #[test]
    fn release_unknown_returns_false() {
        let v = Validator::new();
        assert!(!v.release(0xDEAD));
    }

    #[test]
    fn double_release_returns_false() {
        let v = Validator::new();
        v.track(0x1000);
        assert!(v.release(0x1000));
        assert!(!v.release(0x1000));
    }

    #[test]
    fn clear_removes_all() {
        let v = Validator::new();
        v.track(0x1000);
        v.track(0x2000);
        v.track(0x3000);
        assert_eq!(v.count(), 3);
        v.clear();
        assert_eq!(v.count(), 0);
    }
}
