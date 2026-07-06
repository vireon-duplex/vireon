//! [`SchedulingEvent`] — events the scheduler emits to the
//! observability / session layer.
//!
//! See `DESIGN.md` Part 15 (observability).

use stream_manager::StreamId;

/// Reason a stream was skipped during scheduling.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SkipReason {
    /// Stream has no bytes ready to send.
    EmptyBacklog,
    /// Stream is flow-control blocked (no send credit).
    FlowControlBlocked,
    /// Stream is closed / half-closed (send side terminal).
    ClosedSend,
    /// Stream was dequeued but produced no bytes this tick.
    NoProgress,
}

/// An event emitted by the scheduler during a tick.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SchedulingEvent {
    /// A stream was selected and bytes were scheduled.
    Selected {
        /// The selected stream.
        id: StreamId,
        /// Effective urgency (post-aging).
        effective_urgency: u8,
        /// Bytes scheduled.
        bytes: u64,
    },
    /// A stream was skipped for the given reason.
    Skipped {
        /// The skipped stream.
        id: StreamId,
        /// Why it was skipped.
        reason: SkipReason,
    },
    /// A stream was flow-control blocked.
    Blocked {
        /// The blocked stream.
        id: StreamId,
    },
    /// A stream was aged (promoted to higher urgency).
    Aged {
        /// The aged stream.
        id: StreamId,
        /// Old urgency.
        from_urgency: u8,
        /// New urgency.
        to_urgency: u8,
    },
    /// A stream was starved (waited beyond the threshold without
    /// scheduling).
    Starved {
        /// The starved stream.
        id: StreamId,
        /// Nanoseconds waited.
        waited_ns: u64,
    },
}

impl SchedulingEvent {
    /// The stream id this event refers to.
    #[must_use]
    pub const fn stream_id(&self) -> StreamId {
        match self {
            Self::Selected { id, .. }
            | Self::Skipped { id, .. }
            | Self::Blocked { id, .. }
            | Self::Aged { id, .. }
            | Self::Starved { id, .. } => *id,
        }
    }

    /// Whether the event indicates a liveness problem.
    #[must_use]
    pub const fn is_liveness_issue(&self) -> bool {
        matches!(self, Self::Starved { .. } | Self::Blocked { .. })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stream_id_extractor() {
        let id = StreamId::new_unchecked(4);
        assert_eq!(
            SchedulingEvent::Selected {
                id,
                effective_urgency: 2,
                bytes: 100
            }
            .stream_id(),
            id
        );
        assert_eq!(
            SchedulingEvent::Skipped {
                id,
                reason: SkipReason::FlowControlBlocked
            }
            .stream_id(),
            id
        );
    }

    #[test]
    fn liveness_flagging() {
        let id = StreamId::new_unchecked(0);
        assert!(SchedulingEvent::Starved { id, waited_ns: 100 }.is_liveness_issue());
        assert!(SchedulingEvent::Blocked { id }.is_liveness_issue());
        assert!(
            !SchedulingEvent::Selected {
                id,
                effective_urgency: 0,
                bytes: 100
            }
            .is_liveness_issue()
        );
    }
}
