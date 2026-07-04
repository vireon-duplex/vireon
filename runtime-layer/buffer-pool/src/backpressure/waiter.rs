//! Blocking waiter (Layer 23).
//!
//! [`Waiter`] wraps a [`Condvar`] so that [`BufferPool::get`] can park when
//! backpressure engages and be woken when a release frees capacity.
//!
//! [`BufferPool::get`]: crate::buffer::BufferPool::get

use std::sync::{Condvar, Mutex, MutexGuard};
use std::time::Duration;

/// A parking permit for backpressure.
///
/// The waiter itself does **not** track outstanding bytes — the pool's counters
/// do that. The waiter only provides the `park` / `notify` signalling.
pub struct Waiter {
    flag: Mutex<()>,
    cond: Condvar,
}

impl Waiter {
    /// Creates a new waiter.
    pub fn new() -> Self {
        Self {
            flag: Mutex::new(()),
            cond: Condvar::new(),
        }
    }

    /// Parks the calling thread until [`notify_one`] is called or `timeout`
    /// elapses.
    ///
    /// Returns `true` when notified (or spuriously woken), `false` on
    /// timeout.
    ///
    /// [`notify_one`]: Waiter::notify_one
    pub fn park(&self, timeout: Duration) -> bool {
        let guard = lock(&self.flag);
        match self.cond.wait_timeout(guard, timeout) {
            Ok((_, wt)) => !wt.timed_out(),
            Err(_) => false,
        }
    }

    /// Wakes one waiting thread.
    pub fn notify_one(&self) {
        self.cond.notify_one();
    }

    /// Wakes all waiting threads.
    pub fn notify_all(&self) {
        self.cond.notify_all();
    }
}

impl Default for Waiter {
    fn default() -> Self {
        Self::new()
    }
}

/// Locks a mutex, recovering from poison.
fn lock(flag: &Mutex<()>) -> MutexGuard<'_, ()> {
    match flag.lock() {
        Ok(g) => g,
        Err(poisoned) => {
            tracing::error!("waiter mutex poisoned; recovering");
            poisoned.into_inner()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;
    use std::time::Instant;

    #[test]
    fn park_times_out_when_nobody_notifies() {
        let waiter = Waiter::new();
        let start = Instant::now();
        let notified = waiter.park(Duration::from_millis(50));
        assert!(!notified);
        assert!(start.elapsed() >= Duration::from_millis(40));
    }

    #[test]
    fn notify_wakes_parked_thread() {
        let waiter = Arc::new(Waiter::new());
        let w2 = Arc::clone(&waiter);

        let handle = thread::spawn(move || {
            // Park with a long timeout; should be woken before it expires.
            w2.park(Duration::from_secs(5))
        });

        // Give the parked thread time to enter wait.
        thread::sleep(Duration::from_millis(50));
        waiter.notify_one();

        let notified = handle.join().expect("thread join");
        assert!(notified);
    }

    #[test]
    fn park_returns_after_notify_in_past() {
        let waiter = Waiter::new();
        waiter.notify_one();
        // A past notify does not guarantee immediate return (condvar is not
        // queued), but the timeout ensures we don't block forever.
        let _ = waiter.park(Duration::from_millis(10));
    }
}
