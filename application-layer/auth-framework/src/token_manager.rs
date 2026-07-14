//! [`TokenManager`] — in-memory token lifecycle management with TTL.
//!
//! Issues opaque tokens identified by a `u64` token hash, stored with
//! an expiry timestamp. Tokens are validated against the store and
//! automatically purged when expired.
//!
//! ```text
//! issue(principal=1)   → token_hash=0xABCDEF, stored with TTL
//! validate(0xABCDEF)   → true (within TTL)
//! revoke(0xABCDEF)     → removed from store
//! validate(0xABCDEF)   → false (revoked)
//! ```

use crate::config::AuthConfig;
use crate::error::AuthError;
use crate::health::HealthReport;
use crate::principal::PrincipalId;
use crate::stats::AuthStats;
use rustc_hash::FxHashMap;
use std::time::{SystemTime, UNIX_EPOCH};

/// Stored token entry.
struct TokenEntry {
    principal: PrincipalId,
    expiry_ns: u64,
}

/// Token manager with in-memory storage and TTL expiry.
///
/// Tokens are `u64` values generated from a monotonically increasing
/// counter mixed with the principal id. Expired tokens are lazily
/// purged on validate/revoke.
pub struct TokenManager {
    cfg: AuthConfig,
    stats: AuthStats,
    tokens: FxHashMap<u64, TokenEntry>,
    next_token: u64,
}

impl std::fmt::Debug for TokenManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TokenManager")
            .field("cfg", &self.cfg)
            .field("active_tokens", &self.tokens.len())
            .finish()
    }
}

/// Simple mixing function to distribute token hashes.
fn mix(a: u64, b: u64) -> u64 {
    let mut x = a.wrapping_add(b.rotate_left(13));
    x ^= x >> 33;
    x = x.wrapping_mul(0xff51_afd7_ed55_8ccd);
    x ^= x >> 33;
    x
}

/// Returns the current time in nanoseconds since UNIX_EPOCH.
fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

impl TokenManager {
    /// Creates a new token manager with the given config.
    #[must_use]
    pub fn new(cfg: AuthConfig) -> Self {
        Self {
            cfg,
            stats: AuthStats::new(),
            tokens: FxHashMap::default(),
            next_token: 1,
        }
    }

    /// Issues a new token for the given principal.
    ///
    /// The token hash is a `u64` derived from an incrementing counter
    /// mixed with the principal id. The token is stored with a TTL
    /// from `cfg.token_ttl_ms`.
    ///
    /// # Errors
    /// Returns [`AuthError::InvalidToken`] if the principal is nil.
    pub fn issue(&mut self, principal: PrincipalId) -> Result<u64, AuthError> {
        if principal.is_nil() {
            return Err(AuthError::InvalidToken("nil principal"));
        }

        let seq = self.next_token;
        self.next_token += 1;
        let token_hash = mix(seq, principal.raw());

        let expiry_ns = now_ns() + self.cfg.token_ttl_ms * 1_000_000;
        self.tokens.insert(
            token_hash,
            TokenEntry {
                principal,
                expiry_ns,
            },
        );

        self.stats.record_token_issued();
        Ok(token_hash)
    }

    /// Validates a token hash against the store.
    ///
    /// Returns `Ok(true)` if the token exists and has not expired.
    /// Expired tokens are lazily purged. Returns `Ok(false)` for
    /// unknown or expired tokens.
    ///
    /// # Errors
    /// Currently never errors; reserved for future shutdown semantics.
    pub fn validate(&mut self, token_hash: u64) -> Result<bool, AuthError> {
        let now = now_ns();
        match self.tokens.get(&token_hash) {
            Some(entry) if entry.expiry_ns > now => {
                self.stats.record_token_validated();
                Ok(true)
            }
            Some(_) => {
                // Expired — purge.
                self.tokens.remove(&token_hash);
                self.stats.record_token_validated();
                Ok(false)
            }
            None => {
                self.stats.record_token_validated();
                Ok(false)
            }
        }
    }

    /// Revokes a token (removes it from the store).
    ///
    /// Idempotent: revoking an unknown token returns `Ok(())`.
    ///
    /// # Errors
    /// Currently never errors.
    pub fn revoke(&mut self, token_hash: u64) -> Result<(), AuthError> {
        if self.tokens.remove(&token_hash).is_some() {
            self.stats.record_token_revoked();
        }
        Ok(())
    }

    /// Returns the number of active (non-purged) tokens.
    #[must_use]
    pub fn active_count(&self) -> usize {
        self.tokens.len()
    }

    /// Purges all expired tokens from the store.
    pub fn purge_expired(&mut self) {
        let now = now_ns();
        self.tokens.retain(|_, entry| entry.expiry_ns > now);
    }

    /// Get the current health report.
    #[must_use]
    pub fn health(&self) -> HealthReport {
        HealthReport::derive(&self.stats.snapshot())
    }

    /// Returns a reference to the stats.
    #[must_use]
    pub fn stats(&self) -> &AuthStats {
        &self.stats
    }
}

// ── Tests ─────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Preset;

    #[test]
    fn token_manager_constructs() {
        let tm = TokenManager::new(Preset::Standard.to_config());
        assert!(tm.health().is_healthy());
        assert_eq!(tm.active_count(), 0);
    }

    #[test]
    fn issue_and_validate() {
        let mut tm = TokenManager::new(Preset::Standard.to_config());
        let token = tm.issue(PrincipalId::new(1)).unwrap();
        assert!(tm.validate(token).unwrap());
        assert_eq!(tm.active_count(), 1);
    }

    #[test]
    fn issue_nil_principal_errors() {
        let mut tm = TokenManager::new(Preset::Standard.to_config());
        assert!(tm.issue(PrincipalId::NIL).is_err());
    }

    #[test]
    fn validate_unknown_returns_false() {
        let mut tm = TokenManager::new(Preset::Standard.to_config());
        assert!(!tm.validate(99999).unwrap());
    }

    #[test]
    fn revoke_removes_token() {
        let mut tm = TokenManager::new(Preset::Standard.to_config());
        let token = tm.issue(PrincipalId::new(1)).unwrap();
        assert!(tm.validate(token).unwrap());

        tm.revoke(token).unwrap();
        assert!(!tm.validate(token).unwrap());
    }

    #[test]
    fn revoke_unknown_is_idempotent() {
        let mut tm = TokenManager::new(Preset::Standard.to_config());
        assert!(tm.revoke(99999).is_ok());
    }

    #[test]
    fn tokens_are_unique() {
        let mut tm = TokenManager::new(Preset::Standard.to_config());
        let t1 = tm.issue(PrincipalId::new(1)).unwrap();
        let t2 = tm.issue(PrincipalId::new(1)).unwrap();
        let t3 = tm.issue(PrincipalId::new(2)).unwrap();
        assert_ne!(t1, t2);
        assert_ne!(t1, t3);
        assert_ne!(t2, t3);
    }

    #[test]
    fn multiple_tokens_same_principal() {
        let mut tm = TokenManager::new(Preset::Standard.to_config());
        let t1 = tm.issue(PrincipalId::new(1)).unwrap();
        let t2 = tm.issue(PrincipalId::new(1)).unwrap();

        assert!(tm.validate(t1).unwrap());
        assert!(tm.validate(t2).unwrap());
        assert_eq!(tm.active_count(), 2);
    }

    #[test]
    fn purge_expired_removes_old_tokens() {
        let mut tm = TokenManager::new(AuthConfig {
            token_ttl_ms: 1, // 1ms TTL
            ..Preset::Standard.to_config()
        });
        let token = tm.issue(PrincipalId::new(1)).unwrap();

        std::thread::sleep(std::time::Duration::from_millis(10));
        tm.purge_expired();
        assert_eq!(tm.active_count(), 0);
        assert!(!tm.validate(token).unwrap());
    }
}
