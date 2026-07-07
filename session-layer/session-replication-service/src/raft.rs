//! [`RaftEngine`] — Phase 1+ skeleton for Raft-based strong
//! consistency replication.
//!
//! See `DESIGN.md` Part 6 (Raft protocol).
//!
//! # Raft Protocol
//!
//! The Raft engine implements leader-based log replication:
//!
//! 1. **Leader Election**: Nodes compete in elections; the winner
//!    becomes leader for a term.
//! 2. **Log Replication**: The leader appends entries and replicates
//!    to followers via `AppendEntries`.
//! 3. **Safety**: An entry is committed once a majority has
//!    acknowledged it.
//!
//! # Phase 0
//!
//! Type + stub only. Phase 2+ adds real consensus via a Raft library
//! (openraft or raft-rs).

use crate::config::ReplicationConfig;
use crate::error::ReplicationError;
use crate::replica::ReplicaId;

/// Phase 1+ Raft consensus engine.
///
/// Provides strong consistency (linearizability) via leader-based
/// log replication. Trades availability for correctness during
/// partitions.
#[allow(dead_code)]
pub struct RaftEngine {
    election_timeout_ms: u64,
    current_term: u64,
    leader_id: Option<ReplicaId>,
}

impl RaftEngine {
    /// Construct a new Raft engine with the given election timeout.
    #[must_use]
    pub fn new(election_timeout_ms: u64) -> Self {
        Self {
            election_timeout_ms,
            current_term: 0,
            leader_id: None,
        }
    }

    /// Construct from a replication config.
    #[must_use]
    pub fn from_config(cfg: &ReplicationConfig) -> Self {
        Self::new(cfg.raft_election_timeout_ms)
    }

    /// Propose a write to the log. The leader replicates to
    /// followers; the entry commits once a quorum acks.
    ///
    /// # Errors
    /// Always returns `Ok(())` in Phase 0; Phase 2+ will return
    /// `Err` on timeout, quorum failure, or leadership loss.
    pub fn propose(&mut self, key: &str, value: &[u8]) -> Result<(), ReplicationError> {
        // Phase 0: stub.
        let _ = (key, value);
        Ok(())
    }

    /// Vote in an election for the given candidate.
    ///
    /// Returns `true` if this node grants its vote.
    ///
    /// # Errors
    /// Always returns `Ok(true)` in Phase 0.
    pub fn vote(&mut self, term: u64, candidate: ReplicaId) -> Result<bool, ReplicationError> {
        // Phase 0: stub — always grant the vote.
        let _ = (term, candidate);
        Ok(true)
    }

    /// Process an `AppendEntries` RPC from the leader.
    ///
    /// # Errors
    /// Always returns `Ok(())` in Phase 0.
    pub fn append_entries(&mut self, term: u64, leader: ReplicaId) -> Result<(), ReplicationError> {
        // Phase 0: stub — update term + leader.
        if term >= self.current_term {
            self.current_term = term;
            self.leader_id = Some(leader);
        }
        Ok(())
    }

    /// The current Raft term.
    #[must_use]
    pub const fn current_term(&self) -> u64 {
        self.current_term
    }

    /// The current leader (if known).
    #[must_use]
    pub const fn leader_id(&self) -> Option<ReplicaId> {
        self.leader_id
    }

    /// The configured election timeout (ms).
    #[must_use]
    pub const fn election_timeout_ms(&self) -> u64 {
        self.election_timeout_ms
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use session_registry::node::NodeId;

    #[test]
    fn raft_engine_new() {
        let engine = RaftEngine::new(1_500);
        assert_eq!(engine.election_timeout_ms(), 1_500);
        assert_eq!(engine.current_term(), 0);
        assert!(engine.leader_id().is_none());
    }

    #[test]
    fn raft_propose_is_stub_ok() {
        let mut engine = RaftEngine::new(1_500);
        assert!(engine.propose("key1", b"value1").is_ok());
    }

    #[test]
    fn raft_vote_grants() {
        let mut engine = RaftEngine::new(1_500);
        let candidate = ReplicaId::from_node(NodeId::new(1));
        let granted = engine.vote(1, candidate).unwrap();
        assert!(granted);
    }

    #[test]
    fn raft_append_entries_updates_leader() {
        let mut engine = RaftEngine::new(1_500);
        let leader = ReplicaId::from_node(NodeId::new(2));
        engine.append_entries(1, leader).unwrap();
        assert_eq!(engine.current_term(), 1);
        assert_eq!(engine.leader_id(), Some(leader));
    }
}
