//! [`BackpressureTracker`] — per-session backpressure signal aggregation.
//!
//! See `DESIGN.md` Part 12 (backpressure).
//!
//! # Propagation chain
//!
//! ```text
//! Application ─► Session ─► Stream ─► Transport ─► Runtime ─► IO
//!      ◄────────── ◄────────── ◄────────── ◄────────── ◄─────
//! ```
//!
//! When any layer signals backpressure (e.g. recv buffer full), the
//! upstream layers must also slow down. The tracker aggregates signals
//! per session so the server can query whether a session is blocked
//! before performing work.
//!
//! # Phase 3
//!
//! Functional signal aggregation: `signal()` records a per-source
//! signal, `is_blocked()` reports whether any signal is active, and
//! `blocked_sessions()` enumerates all blocked sessions for batch
//! scheduling.

use rustc_hash::FxHashMap;

use crate::id::SessionId;

/// Source of a backpressure signal.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum BackpressureSource {
    /// Application layer (consumer is slow).
    Application,
    /// Session layer (quota exceeded).
    Session,
    /// Stream layer (flow control).
    Stream,
    /// Transport layer (congestion window).
    Transport,
    /// Runtime layer (reactor saturation).
    Runtime,
    /// IO layer (kernel buffer pressure).
    Io,
}

/// A backpressure signal for a specific session.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BackpressureSignal {
    /// The session under pressure.
    pub session_id: SessionId,
    /// Source of the signal.
    pub source: BackpressureSource,
    /// Recommended pause duration (ns). 0 = "pause indefinitely".
    pub pause_ns: u64,
}

/// Backpressure tracker — aggregates per-session signals from all
/// layers.
pub struct BackpressureTracker {
    signals: FxHashMap<SessionId, Vec<BackpressureSignal>>,
}

impl Default for BackpressureTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl BackpressureTracker {
    /// Construct a new tracker.
    #[must_use]
    pub fn new() -> Self {
        Self {
            signals: FxHashMap::default(),
        }
    }

    /// Records a backpressure signal for a session. If a signal from
    /// the same source already exists, it is replaced.
    pub fn signal(&mut self, sig: BackpressureSignal) {
        let entries = self.signals.entry(sig.session_id).or_default();
        // Replace existing signal from the same source.
        if let Some(existing) = entries.iter_mut().find(|s| s.source == sig.source) {
            *existing = sig;
        } else {
            entries.push(sig);
        }
    }

    /// Removes all signals for `session_id`.
    pub fn clear(&mut self, session_id: SessionId) {
        self.signals.remove(&session_id);
    }

    /// Removes signals from a specific `source` for `session_id`.
    pub fn clear_source(&mut self, session_id: SessionId, source: BackpressureSource) {
        if let Some(entries) = self.signals.get_mut(&session_id) {
            entries.retain(|s| s.source != source);
            if entries.is_empty() {
                self.signals.remove(&session_id);
            }
        }
    }

    /// Whether `session_id` has any active backpressure signal.
    #[must_use]
    pub fn is_blocked(&self, session_id: SessionId) -> bool {
        self.signals.contains_key(&session_id)
    }

    /// Lists all sessions that have at least one active signal.
    #[must_use]
    pub fn blocked_sessions(&self) -> Vec<SessionId> {
        self.signals.keys().copied().collect()
    }

    /// Returns the signals for a session, if any.
    #[must_use]
    pub fn signals_for(&self, session_id: SessionId) -> &[BackpressureSignal] {
        self.signals
            .get(&session_id)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }

    /// Number of sessions with active backpressure.
    #[must_use]
    pub fn len(&self) -> usize {
        self.signals.len()
    }

    /// Whether no sessions are blocked.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.signals.is_empty()
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    fn sig(id: SessionId, source: BackpressureSource) -> BackpressureSignal {
        BackpressureSignal {
            session_id: id,
            source,
            pause_ns: 1_000_000,
        }
    }

    #[test]
    fn source_variants_distinct() {
        assert_ne!(BackpressureSource::Application, BackpressureSource::Session);
        assert_ne!(BackpressureSource::Stream, BackpressureSource::Transport);
        assert_ne!(BackpressureSource::Runtime, BackpressureSource::Io);
    }

    #[test]
    fn signal_construction() {
        let s = BackpressureSignal {
            session_id: SessionId::nil(),
            source: BackpressureSource::Stream,
            pause_ns: 1_000_000,
        };
        assert_eq!(s.pause_ns, 1_000_000);
    }

    #[test]
    fn signal_then_is_blocked() {
        let mut t = BackpressureTracker::new();
        let id = SessionId::from_pair(1, 1);
        assert!(!t.is_blocked(id));
        t.signal(sig(id, BackpressureSource::Stream));
        assert!(t.is_blocked(id));
    }

    #[test]
    fn clear_removes_all_signals() {
        let mut t = BackpressureTracker::new();
        let id = SessionId::from_pair(1, 1);
        t.signal(sig(id, BackpressureSource::Stream));
        t.signal(sig(id, BackpressureSource::Transport));
        t.clear(id);
        assert!(!t.is_blocked(id));
        assert!(t.signals_for(id).is_empty());
    }

    #[test]
    fn clear_source_removes_one() {
        let mut t = BackpressureTracker::new();
        let id = SessionId::from_pair(1, 1);
        t.signal(sig(id, BackpressureSource::Stream));
        t.signal(sig(id, BackpressureSource::Transport));
        t.clear_source(id, BackpressureSource::Stream);
        assert!(t.is_blocked(id));
        assert_eq!(t.signals_for(id).len(), 1);
        assert_eq!(t.signals_for(id)[0].source, BackpressureSource::Transport);
    }

    #[test]
    fn clear_last_source_removes_entry() {
        let mut t = BackpressureTracker::new();
        let id = SessionId::from_pair(1, 1);
        t.signal(sig(id, BackpressureSource::Stream));
        t.clear_source(id, BackpressureSource::Stream);
        assert!(!t.is_blocked(id));
    }

    #[test]
    fn signal_replaces_same_source() {
        let mut t = BackpressureTracker::new();
        let id = SessionId::from_pair(1, 1);
        t.signal(BackpressureSignal {
            session_id: id,
            source: BackpressureSource::Stream,
            pause_ns: 100,
        });
        t.signal(BackpressureSignal {
            session_id: id,
            source: BackpressureSource::Stream,
            pause_ns: 200,
        });
        assert_eq!(t.signals_for(id).len(), 1);
        assert_eq!(t.signals_for(id)[0].pause_ns, 200);
    }

    #[test]
    fn blocked_sessions_lists_all() {
        let mut t = BackpressureTracker::new();
        let id1 = SessionId::from_pair(1, 1);
        let id2 = SessionId::from_pair(2, 2);
        t.signal(sig(id1, BackpressureSource::Stream));
        t.signal(sig(id2, BackpressureSource::Io));
        let mut blocked = t.blocked_sessions();
        blocked.sort();
        assert_eq!(blocked, vec![id1, id2]);
    }

    #[test]
    fn len_and_is_empty() {
        let mut t = BackpressureTracker::new();
        assert!(t.is_empty());
        t.signal(sig(SessionId::nil(), BackpressureSource::Application));
        assert_eq!(t.len(), 1);
        assert!(!t.is_empty());
    }
}
