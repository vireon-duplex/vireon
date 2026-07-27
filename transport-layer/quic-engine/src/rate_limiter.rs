//! Per-IP sliding-window rate limiter for new QUIC handshakes.
//!
//! Defends against handshake storms (DoS, misbehaving clients, retry
//! loops) by capping the rate at which a single source IP can initiate
//! new connections. Applied in [`QuicEngine::accept_new`] before
//! `quiche::accept` is called, so rate-limited Initial packets never
//! allocate connection state.
//!
//! ## Algorithm
//!
//! Fixed-window counter per source IP — simpler and lower overhead
//! than a token bucket or sliding log, at the cost of brief
//! burst-tolerance error at window boundaries (≤2× the configured
//! rate for one window per second at worst). For handshake-rate
//! protection this trade-off is correct: the goal is to bound
//! worst-case handshake arrival, not to be precisely fair.
//!
//! ## Memory bounds
//!
//! Each entry is ~48 bytes (`IpAddr` + `Instant` + `u32` + padding).
//! At 1 M distinct IPs (a /8 attacker) the table is ~48 MiB. The
//! GC pass prunes entries whose window is older than
//! `WINDOW_SECS * 2` so a benign client base does not accumulate
//! stale state.

use std::net::IpAddr;
use std::time::{Duration, Instant};

use rustc_hash::FxHashMap;

/// Window duration. One second matches the per-second rate contract
/// implied by `max_per_sec` in [`IpRateLimiter::new`].
const WINDOW_SECS: u64 = 1;

/// GC automatically prunes entries older than this many seconds.
/// Set to 2× the window so a brief lull followed by a burst doesn't
/// drop the entry just before it would have been useful.
const GC_RETENTION_SECS: u64 = 2;

/// Per-IP fixed-window handshake rate limiter.
///
/// Owned by [`QuicEngine`] (one per worker). All mutation happens on
/// the engine's single thread — no locking required.
pub struct IpRateLimiter {
    /// Maximum new handshakes per second per IP. `0` = disabled (always allow).
    max_per_sec: u32,
    /// Per-IP window state.
    windows: FxHashMap<IpAddr, Window>,
}

#[derive(Clone, Copy)]
struct Window {
    /// Start of the current 1-second counting window.
    window_start: Instant,
    /// Connections accepted in the current window.
    count: u32,
}

impl IpRateLimiter {
    /// Construct with the given per-second limit. `0` disables limiting
    /// (cheap fast-path: `check` returns `true` immediately).
    #[must_use]
    pub fn new(max_per_sec: u32) -> Self {
        Self {
            max_per_sec,
            windows: FxHashMap::default(),
        }
    }

    /// Whether the limiter is active. `false` means `check` is a no-op
    /// and no entries are stored.
    #[inline]
    #[must_use]
    pub fn is_enabled(&self) -> bool {
        self.max_per_sec > 0
    }

    /// Returns the configured per-second cap.
    #[inline]
    #[must_use]
    pub const fn max_per_sec(&self) -> u32 {
        self.max_per_sec
    }

    /// Check whether a new handshake from `ip` is permitted. Returns
    /// `true` if allowed, `false` if rate-limited.
    ///
    /// Advances the window for `ip` if it has rolled over, then
    /// increments the in-window counter on a successful check.
    #[inline]
    pub fn check(&mut self, ip: IpAddr, now: Instant) -> bool {
        if self.max_per_sec == 0 {
            return true;
        }
        let w = self.windows.entry(ip).or_insert(Window {
            window_start: now,
            count: 0,
        });
        // Roll the window forward if it's stale.
        if now.duration_since(w.window_start) >= Duration::from_secs(WINDOW_SECS) {
            w.window_start = now;
            w.count = 0;
        }
        if w.count >= self.max_per_sec {
            return false;
        }
        w.count += 1;
        true
    }

    /// Drop entries whose windows are older than [`GC_RETENTION_SECS`].
    /// Call this periodically (e.g. once per second of reactor time)
    /// to bound memory usage under IP churn.
    pub fn gc(&mut self, now: Instant) {
        if self.windows.is_empty() {
            return;
        }
        let cutoff = now
            .checked_sub(Duration::from_secs(GC_RETENTION_SECS))
            .unwrap_or(now);
        // Collect first to avoid mutating during iteration.
        // (FxHashMap's drain_filter would be nicer but is unstable.)
        let stale: Vec<IpAddr> = self
            .windows
            .iter()
            .filter(|&(_, w)| w.window_start < cutoff)
            .map(|(ip, _)| *ip)
            .collect();
        for ip in stale {
            self.windows.remove(&ip);
        }
    }

    /// Current number of tracked IPs (for diagnostics / metrics).
    #[inline]
    #[must_use]
    pub fn tracked_ip_count(&self) -> usize {
        self.windows.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, Ipv6Addr};

    fn ip4(a: u8, b: u8, c: u8, d: u8) -> IpAddr {
        IpAddr::V4(Ipv4Addr::new(a, b, c, d))
    }

    #[test]
    fn disabled_allows_everything() {
        let mut rl = IpRateLimiter::new(0);
        let now = Instant::now();
        assert!(!rl.is_enabled());
        for _ in 0..1_000 {
            assert!(rl.check(ip4(1, 2, 3, 4), now));
        }
        assert_eq!(rl.tracked_ip_count(), 0);
    }

    #[test]
    fn under_limit_allowed() {
        let mut rl = IpRateLimiter::new(5);
        let now = Instant::now();
        for i in 0..5 {
            assert!(rl.check(ip4(10, 0, 0, 1), now), "attempt {i} should pass");
        }
        // 6th in same window → blocked
        assert!(!rl.check(ip4(10, 0, 0, 1), now));
    }

    #[test]
    fn different_ips_independent() {
        let mut rl = IpRateLimiter::new(2);
        let now = Instant::now();
        assert!(rl.check(ip4(10, 0, 0, 1), now));
        assert!(rl.check(ip4(10, 0, 0, 1), now));
        // Same IP again → blocked
        assert!(!rl.check(ip4(10, 0, 0, 1), now));
        // Different IP → independent budget
        assert!(rl.check(ip4(10, 0, 0, 2), now));
        assert!(rl.check(ip4(10, 0, 0, 2), now));
        assert!(!rl.check(ip4(10, 0, 0, 2), now));
    }

    #[test]
    fn ipv6_keyed_independently_from_ipv4() {
        let mut rl = IpRateLimiter::new(1);
        let now = Instant::now();
        let v4 = ip4(10, 0, 0, 1);
        let v6 = IpAddr::V6(Ipv6Addr::LOCALHOST);
        assert!(rl.check(v4, now));
        assert!(rl.check(v6, now));
        // Both exhausted independently
        assert!(!rl.check(v4, now));
        assert!(!rl.check(v6, now));
    }

    #[test]
    fn window_resets_after_one_second() {
        let mut rl = IpRateLimiter::new(3);
        let t0 = Instant::now();
        // Fill the window
        for _ in 0..3 {
            assert!(rl.check(ip4(10, 0, 0, 1), t0));
        }
        assert!(!rl.check(ip4(10, 0, 0, 1), t0));
        // 1.5 seconds later — new window
        let t1 = t0 + Duration::from_millis(1500);
        for _ in 0..3 {
            assert!(rl.check(ip4(10, 0, 0, 1), t1));
        }
        assert!(!rl.check(ip4(10, 0, 0, 1), t1));
    }

    #[test]
    fn gc_drops_stale_entries() {
        let mut rl = IpRateLimiter::new(1);
        let t0 = Instant::now();
        rl.check(ip4(10, 0, 0, 1), t0);
        rl.check(ip4(10, 0, 0, 2), t0);
        rl.check(ip4(10, 0, 0, 3), t0);
        assert_eq!(rl.tracked_ip_count(), 3);

        // GC at t0+3s drops everything (cutoff = t0+1s, all entries have window_start = t0)
        let t1 = t0 + Duration::from_secs(3);
        rl.gc(t1);
        assert_eq!(rl.tracked_ip_count(), 0);

        // GC at t0+1.5s would keep entries seen at t0+1.5s (within retention)
        rl.check(ip4(10, 0, 0, 9), t1);
        rl.gc(t1);
        assert_eq!(rl.tracked_ip_count(), 1);
    }

    #[test]
    fn gc_noop_on_empty() {
        let mut rl = IpRateLimiter::new(1);
        rl.gc(Instant::now()); // must not panic
    }

    #[test]
    fn gc_keeps_recent_entries() {
        let mut rl = IpRateLimiter::new(5);
        let t0 = Instant::now();
        // Recent activity — should survive GC.
        rl.check(ip4(10, 0, 0, 1), t0);
        // GC immediately (cutoff = t0 - 2s, but entry is at t0).
        rl.gc(t0);
        assert_eq!(rl.tracked_ip_count(), 1);
        // Subsequent check continues from prior count.
        assert!(rl.check(ip4(10, 0, 0, 1), t0));
        assert!(rl.check(ip4(10, 0, 0, 1), t0));
        assert!(rl.check(ip4(10, 0, 0, 1), t0));
        assert!(rl.check(ip4(10, 0, 0, 1), t0));
        // Now exhausted (5 in window)
        assert!(!rl.check(ip4(10, 0, 0, 1), t0));
    }

    #[test]
    fn burst_at_window_boundary_bounded() {
        // Document the fixed-window edge case: 5 at end of window + 5 at
        // start of next = 10 in <1s. This is the inherent cost of
        // fixed-window vs sliding-window; acceptable for handshake defense.
        let mut rl = IpRateLimiter::new(5);
        let t0 = Instant::now();
        let t1 = t0 + Duration::from_millis(1001);
        let mut allowed = 0;
        for _ in 0..5 {
            if rl.check(ip4(1, 1, 1, 1), t0) {
                allowed += 1;
            }
        }
        for _ in 0..5 {
            if rl.check(ip4(1, 1, 1, 1), t1) {
                allowed += 1;
            }
        }
        assert_eq!(allowed, 10);
    }
}
