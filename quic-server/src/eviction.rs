//! Memory-pressure-based connection eviction (Layer 23 extension).
//!
//! When the buffer pool enters [`PressureLevel::High`] or above, the
//! eviction module selects the heaviest / idle connections and closes
//! them via QUIC APPLICATION_CLOSE. This frees buffer capacity without
//! rejecting allocations for healthy connections.
//!
//! The victim-selection algorithm is a **pure function** — given the same
//! `(conns, pressure, now_ns, config)` it always returns the same result.
//! All side effects (closing connections, logging) live in the caller
//! ([`crate::stack::VireonStack::evict_connections`]).
//!
//! ## Response ladder
//!
//! ```text
//! Pressure          Action                       Fraction
//! ─────────────────────────────────────────────────────────────
//! High   (75-89%)   Close idle heavy consumers   5% of active
//! Critical (90%+)   Close heaviest regardless    10% of active
//! ```
//!
//! ## Safety guarantees
//!
//! - **Cooldown** — the caller enforces `config.cooldown` between passes.
//! - **Min survivors** — never evict if `conns.len() <= min_survivors`.
//! - **Batch limit** — at most `fraction × conns.len()` per pass.

use std::time::Duration;

use buffer_pool::PressureLevel;

/// QUIC application error code for resource-eviction closes.
///
/// H2 uses `0x00` for no error and `0x01`–`0x0c` for protocol errors.
/// QUIC application error codes are opaque to the transport; `0x0100`
/// (256) sits in a range unlikely to collide with HTTP/3 codes.
pub const EVICTION_ERROR_CODE: u64 = 0x0100;

/// Configuration for the eviction policy.
///
/// Sensible production defaults via [`EvictionConfig::default`]:
/// 5-second cooldown, 5-second idle threshold, 5%/10% fractions,
/// minimum 2 survivors.
#[derive(Clone)]
pub struct EvictionConfig {
    /// Minimum time between eviction passes (prevents thrashing).
    pub cooldown: Duration,
    /// A connection is considered "idle" if `now_ns - last_active_ns`
    /// exceeds this value. Only idle connections are eligible at
    /// [`PressureLevel::High`]; all are eligible at Critical.
    pub high_idle_threshold_ns: u64,
    /// Fraction of active connections to evict at High pressure.
    pub high_evict_fraction: f32,
    /// Fraction of active connections to evict at Critical pressure.
    pub critical_evict_fraction: f32,
    /// Minimum connections that must remain after an eviction pass.
    pub min_survivors: usize,
}

impl Default for EvictionConfig {
    fn default() -> Self {
        Self {
            cooldown: Duration::from_secs(5),
            high_idle_threshold_ns: 5_000_000_000, // 5 s in ns
            high_evict_fraction: 0.05,             // 5%
            critical_evict_fraction: 0.10,         // 10%
            min_survivors: 2,
        }
    }
}

/// Snapshot of one connection for eviction decisions.
///
/// Copied out of [`crate::connection::ConnectionRegistry`] to keep the
/// selection function pure (no borrow of the registry).
#[derive(Clone, Copy, Debug)]
pub struct ConnEvictInfo {
    pub conn_idx: usize,
    pub bytes_recv: u64,
    pub last_active_ns: u64,
}

/// Selects which connections to evict based on the current pressure level.
///
/// Returns `conn_idx` values sorted by eviction priority (biggest memory
/// consumer first). The caller closes each via
/// [`quic_engine::QuicEngine::close_connection`].
///
/// # Algorithm
///
/// 1. If `conns.len() <= min_survivors` → return empty (protect the
///    last few connections regardless of pressure).
/// 2. At **High**: filter to connections idle for longer than
///    `high_idle_threshold_ns`. At **Critical**: no filter.
/// 3. Sort candidates by `bytes_recv` **descending** (biggest consumers
///    first — they hold the most buffer capacity).
/// 4. Take the top `N` where `N = min(conns.len() - min_survivors,
///    floor(conns.len() × fraction))`.
///
/// Returns an empty vec for Low / Moderate pressure (caller should not
/// have invoked this function, but the guard is defensive).
#[must_use]
pub fn select_victims(
    conns: &[ConnEvictInfo],
    pressure: PressureLevel,
    now_ns: u64,
    config: &EvictionConfig,
) -> Vec<usize> {
    if conns.len() <= config.min_survivors {
        return Vec::new();
    }

    let fraction = match pressure {
        PressureLevel::High => config.high_evict_fraction,
        PressureLevel::Critical => config.critical_evict_fraction,
        PressureLevel::Moderate | PressureLevel::Low => return Vec::new(),
    };

    // Candidate set: at High, only idle connections; at Critical, all.
    let mut candidates: Vec<&ConnEvictInfo> = if pressure == PressureLevel::High {
        conns
            .iter()
            .filter(|c| now_ns.saturating_sub(c.last_active_ns) > config.high_idle_threshold_ns)
            .collect()
    } else {
        conns.iter().collect()
    };

    if candidates.is_empty() {
        return Vec::new();
    }

    // Sort by bytes_recv descending — biggest memory consumers first.
    candidates.sort_unstable_by(|a, b| b.bytes_recv.cmp(&a.bytes_recv));

    // Max we can evict without dropping below min_survivors.
    let max_evict = conns.len().saturating_sub(config.min_survivors);
    // Target fraction of total — guarantee at least 1 victim when
    // candidates exist, otherwise small fleets (3 conns × 5% = 0.15)
    // would never trigger eviction under sustained pressure.
    let target = ((conns.len() as f32) * fraction).floor() as usize;
    let n = target.max(1).min(max_evict).min(candidates.len());

    candidates.iter().take(n).map(|c| c.conn_idx).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn info(idx: usize, bytes: u64, active_ns: u64) -> ConnEvictInfo {
        ConnEvictInfo {
            conn_idx: idx,
            bytes_recv: bytes,
            last_active_ns: active_ns,
        }
    }

    const HIGH: PressureLevel = PressureLevel::High;
    const CRITICAL: PressureLevel = PressureLevel::Critical;
    const NOW: u64 = 10_000_000_000; // 10 s

    #[test]
    fn select_victims_empty_returns_empty() {
        let cfg = EvictionConfig::default();
        assert!(select_victims(&[], CRITICAL, NOW, &cfg).is_empty());
    }

    #[test]
    fn select_victims_below_min_survivors_returns_empty() {
        let cfg = EvictionConfig::default(); // min_survivors = 2
        let conns = vec![info(0, 1000, NOW), info(1, 2000, NOW)];
        assert!(select_victims(&conns, CRITICAL, NOW, &cfg).is_empty());
    }

    #[test]
    fn select_high_pressure_filters_idle_connections() {
        let cfg = EvictionConfig::default(); // idle_threshold = 5 s
        // conn 0 active recently (1 s ago), conn 1 idle (10 s ago).
        let conns = vec![
            info(0, 10_000, NOW - 1_000_000_000),
            info(1, 5_000, NOW - 10_000_000_000),
            info(2, 3_000, NOW - 1_000_000_000),
        ];
        let victims = select_victims(&conns, HIGH, NOW, &cfg);
        // Only conn 1 was idle → it's the only candidate.
        assert_eq!(victims, vec![1]);
    }

    #[test]
    fn select_critical_pressure_does_not_filter() {
        let cfg = EvictionConfig::default();
        // All connections are "active" (last_active = NOW). At Critical
        // they should still be eligible — unlike High which filters idle.
        let conns: Vec<ConnEvictInfo> = (0..10)
            .map(|i| info(i, (10 - i) as u64 * 100, NOW))
            .collect();
        let victims = select_victims(&conns, CRITICAL, NOW, &cfg);
        // 10% of 10 = 1 → conn 0 (biggest, 1000 bytes) is evicted.
        assert_eq!(victims.len(), 1);
        assert_eq!(victims[0], 0);
    }

    #[test]
    fn select_sorts_by_bytes_recv_descending() {
        let cfg = EvictionConfig::default();
        let conns: Vec<ConnEvictInfo> = (0..10)
            .map(|i| info(i, (10 - i) as u64 * 100, NOW))
            .collect();
        let victims = select_victims(&conns, CRITICAL, NOW, &cfg);
        // 10% of 10 = 1; biggest is conn 0 (1000 bytes).
        assert_eq!(victims.len(), 1);
        assert_eq!(victims[0], 0);
    }

    #[test]
    fn select_respects_evict_fraction() {
        let cfg = EvictionConfig {
            critical_evict_fraction: 0.30, // 30% of 10 = 3
            ..EvictionConfig::default()
        };
        let conns: Vec<ConnEvictInfo> = (0..10).map(|i| info(i, i as u64 * 100, NOW)).collect();
        let victims = select_victims(&conns, CRITICAL, NOW, &cfg);
        // Sorted desc: 9,8,7,6,... → top 3 = [9, 8, 7].
        assert_eq!(victims.len(), 3);
        assert_eq!(victims, vec![9, 8, 7]);
    }

    #[test]
    fn select_respects_min_survivors() {
        let cfg = EvictionConfig {
            min_survivors: 8,
            critical_evict_fraction: 0.50, // 50% of 10 = 5
            ..EvictionConfig::default()
        };
        let conns: Vec<ConnEvictInfo> = (0..10).map(|i| info(i, i as u64 * 100, NOW)).collect();
        let victims = select_victims(&conns, CRITICAL, NOW, &cfg);
        // max_evict = 10 - 8 = 2, even though fraction says 5.
        assert_eq!(victims.len(), 2);
        assert_eq!(victims, vec![9, 8]);
    }

    #[test]
    fn select_low_pressure_returns_empty() {
        let cfg = EvictionConfig::default();
        let conns = vec![info(0, 1000, NOW), info(1, 2000, NOW), info(2, 3000, NOW)];
        assert!(select_victims(&conns, PressureLevel::Low, NOW, &cfg).is_empty());
        assert!(select_victims(&conns, PressureLevel::Moderate, NOW, &cfg).is_empty());
    }
}
