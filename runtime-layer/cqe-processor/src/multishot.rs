//! Multishot re-arm policy (Layer 9).
//!
//! The kernel multishot RECV posts CQEs until the buffer group runs dry
//! (`-ENOBUFS`, still armed) or the multishot is cancelled/removed
//! (`-ECANCELED` / `!more`). Either way, the CQE Processor must ask the
//! reactor to re-arm at the next submission boundary.
//!
//! Re-arming directly would require posting an SQE — which is forbidden
//! in this module. Instead we record the request and let the reactor
//! poll [`MultishotState::take_pending`].

/// Multishot RX health + re-arm scheduling.
pub struct MultishotState {
    consecutive_enobufs: u32,
    ticks_since_rearm: u32,
    rearm_cooldown: u32,
    rearm_pending: bool,
    rearm_count: u64,
}

/// A pending re-arm request emitted by [`MultishotState::take_pending`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RearmRequest {
    /// Monotonic counter — useful for ordering tests.
    pub seq: u64,
}

impl MultishotState {
    /// Construct with a re-arm cooldown (ticks between scheduled re-arms).
    #[must_use]
    pub const fn new(rearm_cooldown: u32) -> Self {
        Self {
            consecutive_enobufs: 0,
            ticks_since_rearm: 0,
            rearm_cooldown,
            rearm_pending: false,
            rearm_count: 0,
        }
    }

    /// Records an `-ENOBUFS` event. Used for cooldown decisions.
    #[inline]
    pub fn note_enobufs(&mut self) {
        self.consecutive_enobufs = self.consecutive_enobufs.saturating_add(1);
    }

    /// Resets the ENOBUFS counter (called on a successful Receive).
    #[inline]
    pub fn note_success(&mut self) {
        self.consecutive_enobufs = 0;
    }

    /// Schedules a re-arm request if the cooldown window has elapsed.
    /// Returns `true` if a new request was scheduled, `false` if suppressed
    /// by the cooldown. The very first schedule (before any re-arm has ever
    /// been requested) bypasses the cooldown so initial arming is immediate.
    #[inline]
    pub fn schedule_rearm(&mut self) -> bool {
        if self.rearm_pending {
            return false;
        }
        if self.rearm_count > 0 && self.ticks_since_rearm < self.rearm_cooldown {
            return false;
        }
        self.rearm_pending = true;
        self.ticks_since_rearm = 0;
        self.rearm_count = self.rearm_count.saturating_add(1);
        true
    }

    /// Takes the pending re-arm request, if any. Calling this clears the
    /// pending flag so subsequent `schedule_rearm` calls can succeed.
    pub fn take_pending(&mut self) -> Option<RearmRequest> {
        if self.rearm_pending {
            self.rearm_pending = false;
            Some(RearmRequest {
                seq: self.rearm_count,
            })
        } else {
            None
        }
    }

    /// Notifies the state that a drain cycle has completed. Used to age the
    /// cooldown counter.
    #[inline]
    pub fn tick(&mut self) {
        self.ticks_since_rearm = self.ticks_since_rearm.saturating_add(1);
    }

    /// Total re-arms scheduled.
    #[must_use]
    pub fn rearm_count(&self) -> u64 {
        self.rearm_count
    }

    /// `true` if a re-arm is pending.
    #[must_use]
    pub fn is_rearm_pending(&self) -> bool {
        self.rearm_pending
    }
}

impl std::fmt::Debug for MultishotState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MultishotState")
            .field("consecutive_enobufs", &self.consecutive_enobufs)
            .field("ticks_since_rearm", &self.ticks_since_rearm)
            .field("rearm_cooldown", &self.rearm_cooldown)
            .field("rearm_pending", &self.rearm_pending)
            .field("rearm_count", &self.rearm_count)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schedule_respects_cooldown() {
        let mut m = MultishotState::new(/* cooldown */ 2);
        // First request always succeeds (cooldown ticks_since_rearm=0, but
        // initial state allows the first schedule).
        assert!(m.schedule_rearm());
        // Second immediate request suppressed (still pending + cooldown).
        assert!(!m.schedule_rearm());
        // After taking pending, we still need cooldown ticks.
        assert!(m.take_pending().is_some());
        assert!(!m.schedule_rearm());
        m.tick();
        assert!(!m.schedule_rearm());
        m.tick();
        // Now cooldown has elapsed.
        assert!(m.schedule_rearm());
    }

    #[test]
    fn take_pending_clears_flag() {
        let mut m = MultishotState::new(0);
        m.schedule_rearm();
        assert!(m.is_rearm_pending());
        assert!(m.take_pending().is_some());
        assert!(!m.is_rearm_pending());
        assert!(m.take_pending().is_none());
    }

    #[test]
    fn note_enobufs_accumulates() {
        let mut m = MultishotState::new(0);
        m.note_enobufs();
        m.note_enobufs();
        m.note_success();
        // No direct accessor; verify behaviour doesn't panic.
        m.schedule_rearm();
    }
}
