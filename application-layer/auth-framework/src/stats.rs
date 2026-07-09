//! [`AuthStats`], [`StatsSnapshot`] — atomic counters for observability.

use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug)]
pub struct AuthStats {
    pub auth_attempts_total: AtomicU64,
    pub auth_succeeded: AtomicU64,
    pub auth_failed: AtomicU64,
    pub authz_checks_total: AtomicU64,
    pub authz_allowed: AtomicU64,
    pub authz_denied: AtomicU64,
    pub tokens_issued: AtomicU64,
    pub tokens_validated: AtomicU64,
    pub tokens_revoked: AtomicU64,
    pub rate_limited_count: AtomicU64,
}

impl Default for AuthStats {
    fn default() -> Self {
        Self::new()
    }
}

impl AuthStats {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            auth_attempts_total: AtomicU64::new(0),
            auth_succeeded: AtomicU64::new(0),
            auth_failed: AtomicU64::new(0),
            authz_checks_total: AtomicU64::new(0),
            authz_allowed: AtomicU64::new(0),
            authz_denied: AtomicU64::new(0),
            tokens_issued: AtomicU64::new(0),
            tokens_validated: AtomicU64::new(0),
            tokens_revoked: AtomicU64::new(0),
            rate_limited_count: AtomicU64::new(0),
        }
    }

    pub fn record_auth_attempt(&self) {
        self.auth_attempts_total.fetch_add(1, Ordering::Relaxed);
    }
    pub fn record_auth_success(&self) {
        self.auth_succeeded.fetch_add(1, Ordering::Relaxed);
    }
    pub fn record_auth_failure(&self) {
        self.auth_failed.fetch_add(1, Ordering::Relaxed);
    }
    pub fn record_authz_check(&self, allowed: bool) {
        self.authz_checks_total.fetch_add(1, Ordering::Relaxed);
        if allowed {
            self.authz_allowed.fetch_add(1, Ordering::Relaxed);
        } else {
            self.authz_denied.fetch_add(1, Ordering::Relaxed);
        }
    }
    pub fn record_token_issued(&self) {
        self.tokens_issued.fetch_add(1, Ordering::Relaxed);
    }
    pub fn record_token_validated(&self) {
        self.tokens_validated.fetch_add(1, Ordering::Relaxed);
    }
    pub fn record_token_revoked(&self) {
        self.tokens_revoked.fetch_add(1, Ordering::Relaxed);
    }
    pub fn record_rate_limited(&self) {
        self.rate_limited_count.fetch_add(1, Ordering::Relaxed);
    }

    #[must_use]
    pub fn snapshot(&self) -> StatsSnapshot {
        StatsSnapshot {
            auth_attempts_total: self.auth_attempts_total.load(Ordering::Relaxed),
            auth_succeeded: self.auth_succeeded.load(Ordering::Relaxed),
            auth_failed: self.auth_failed.load(Ordering::Relaxed),
            authz_checks_total: self.authz_checks_total.load(Ordering::Relaxed),
            authz_allowed: self.authz_allowed.load(Ordering::Relaxed),
            authz_denied: self.authz_denied.load(Ordering::Relaxed),
            tokens_issued: self.tokens_issued.load(Ordering::Relaxed),
            tokens_validated: self.tokens_validated.load(Ordering::Relaxed),
            tokens_revoked: self.tokens_revoked.load(Ordering::Relaxed),
            rate_limited_count: self.rate_limited_count.load(Ordering::Relaxed),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StatsSnapshot {
    pub auth_attempts_total: u64,
    pub auth_succeeded: u64,
    pub auth_failed: u64,
    pub authz_checks_total: u64,
    pub authz_allowed: u64,
    pub authz_denied: u64,
    pub tokens_issued: u64,
    pub tokens_validated: u64,
    pub tokens_revoked: u64,
    pub rate_limited_count: u64,
}

impl StatsSnapshot {
    #[must_use]
    pub const fn auth_failure_rate_bps(&self) -> u16 {
        if self.auth_attempts_total == 0 {
            return 0;
        }
        let rate = (self.auth_failed * 10_000) / self.auth_attempts_total;
        if rate > u16::MAX as u64 {
            u16::MAX
        } else {
            rate as u16
        }
    }

    #[must_use]
    pub const fn authz_denial_rate_bps(&self) -> u16 {
        if self.authz_checks_total == 0 {
            return 0;
        }
        let rate = (self.authz_denied * 10_000) / self.authz_checks_total;
        if rate > u16::MAX as u64 {
            u16::MAX
        } else {
            rate as u16
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auth_failure_rate() {
        let stats = AuthStats::new();
        for _ in 0..3 {
            stats.record_auth_attempt();
            stats.record_auth_failure();
        }
        for _ in 0..7 {
            stats.record_auth_attempt();
            stats.record_auth_success();
        }
        // 3/10 = 30% = 3000 bps
        assert_eq!(stats.snapshot().auth_failure_rate_bps(), 3_000);
    }
}
