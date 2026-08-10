//! Egress priority for backpressure scheduling.
//!
//! Priority is derived from the frame's [`MessageType`] (control frames always
//! pre-empt data) and the stream's explicit [`StreamPriority`] (declared at
//! StreamOpen time and decoupled from `DeliveryPolicy`). No wire-format change
//! to data frames — this is a purely local egress scheduling concern.
//!
//! ## Ordering
//!
//! Within a single connection, frames are drained in order
//! Control → Critical → High → Normal → Low. This ensures latency-sensitive
//! audio / control frames are not stuck behind large file-transfer / AI token
//! frames in the pending-sends buffer, while letting callers explicitly demote
//! background work to `Low`.

use send_policy::StreamPriority;
use frame::header::MessageType;

/// Egress priority lane. Lower ordinal = higher priority (drained first).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum Priority {
    /// Control frames: Subscribe, StreamOpen, Resume, Ack, Error.
    /// Always drained first so connection management isn't delayed by
    /// bulk data.
    Control = 0,
    /// Critical user data: audio, tactile feedback. Wins every contention
    /// against other data lanes.
    Critical = 1,
    /// High-priority user data: video, presence, state mutations.
    High = 2,
    /// Default user data lane: chat, AI token streams, RPC.
    Normal = 3,
    /// Background / bulk data: file sync, telemetry. Drained last; may be
    /// starved under sustained higher-lane load (by design).
    Low = 4,
}

impl Priority {
    /// Derives the internal priority from the frame type and the stream's
    /// declared [`StreamPriority`].
    ///
    /// Control frames always get [`Priority::Control`] regardless of the
    /// stream's priority. Data frames inherit the stream's explicit priority;
    /// `None` (unbound / lazy-default stream) maps to [`Priority::Normal`].
    #[inline]
    #[must_use]
    pub fn derive(msg_type: MessageType, priority: Option<StreamPriority>) -> Self {
        if is_control(msg_type) {
            return Self::Control;
        }
        match priority {
            Some(sp) => sp.into(),
            None => Self::Normal,
        }
    }
}

impl From<StreamPriority> for Priority {
    #[inline]
    fn from(sp: StreamPriority) -> Self {
        match sp {
            StreamPriority::Critical => Self::Critical,
            StreamPriority::High => Self::High,
            StreamPriority::Normal => Self::Normal,
            StreamPriority::Low => Self::Low,
        }
    }
}

/// Returns `true` for frame types that manage connection / session state
/// rather than carrying subscriber data.
#[inline]
#[must_use]
fn is_control(msg_type: MessageType) -> bool {
    matches!(
        msg_type,
        MessageType::Handshake
            | MessageType::Subscribe
            | MessageType::Unsubscribe
            | MessageType::StreamOpen
            | MessageType::StreamClose
            | MessageType::StreamReset
            | MessageType::Ack
            | MessageType::Resume
            | MessageType::ResumeOk
            | MessageType::ResumeUnavailable
            | MessageType::Error
            | MessageType::ConsumerGroupJoin
            | MessageType::ConsumerGroupLeave
            | MessageType::ConsumerGroupHeartbeat
            | MessageType::NotifyOffset
            | MessageType::Fetch
            | MessageType::FetchReply
            | MessageType::TopicCreate
            | MessageType::TopicDelete
            | MessageType::StreamPause
            | MessageType::StreamResume
    )
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    #[test]
    fn control_frames_always_control() {
        for mt in [
            MessageType::Subscribe,
            MessageType::StreamOpen,
            MessageType::Resume,
            MessageType::Ack,
            MessageType::Error,
            MessageType::StreamPause,
            MessageType::StreamResume,
        ] {
            assert_eq!(Priority::derive(mt, None), Priority::Control);
            assert_eq!(
                Priority::derive(mt, Some(StreamPriority::Critical)),
                Priority::Control
            );
            assert_eq!(
                Priority::derive(mt, Some(StreamPriority::Low)),
                Priority::Control
            );
        }
    }

    #[test]
    fn explicit_priority_maps_to_lane() {
        assert_eq!(
            Priority::derive(MessageType::Publish, Some(StreamPriority::Critical)),
            Priority::Critical
        );
        assert_eq!(
            Priority::derive(MessageType::Publish, Some(StreamPriority::High)),
            Priority::High
        );
        assert_eq!(
            Priority::derive(MessageType::Publish, Some(StreamPriority::Normal)),
            Priority::Normal
        );
        assert_eq!(
            Priority::derive(MessageType::Publish, Some(StreamPriority::Low)),
            Priority::Low
        );
    }

    #[test]
    fn unbound_data_defaults_to_normal() {
        assert_eq!(
            Priority::derive(MessageType::Publish, None),
            Priority::Normal
        );
    }

    #[test]
    fn ordering_is_control_critical_high_normal_low() {
        assert!(Priority::Control < Priority::Critical);
        assert!(Priority::Critical < Priority::High);
        assert!(Priority::High < Priority::Normal);
        assert!(Priority::Normal < Priority::Low);
    }

    #[test]
    fn from_stream_priority_is_exhaustive() {
        for sp in [
            StreamPriority::Critical,
            StreamPriority::High,
            StreamPriority::Normal,
            StreamPriority::Low,
        ] {
            let p: Priority = sp.into();
            assert!(p >= Priority::Critical && p <= Priority::Low);
        }
    }
}
