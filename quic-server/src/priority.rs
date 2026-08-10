//! Egress priority for backpressure scheduling.
//!
//! Priority is derived from the stream's [`DeliveryPolicy`] (captured at
//! StreamOpen time) and the frame's [`MessageType`]. No wire-format change —
//! this is a purely local egress scheduling concern.
//!
//! ## Ordering
//!
//! Within a single connection, frames are drained in order
//! Control → Realtime → Throughput. This ensures latency-sensitive audio /
//! video / cursor frames are not stuck behind large AI / file-transfer
//! frames in the pending-sends buffer.

use send_policy::DeliveryPolicy;
use frame::header::MessageType;

/// Egress priority lane. Lower ordinal = higher priority (drained first).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum Priority {
    /// Control frames: Subscribe, StreamOpen, Resume, Ack, Error.
    /// Always drained first so connection management isn't delayed by
    /// bulk data.
    Control = 0,
    /// Realtime data: audio, video, cursor, presence. Drained before
    /// Throughput so small latency-sensitive frames aren't stuck behind
    /// large bulk transfers.
    Realtime = 1,
    /// Bulk / throughput data: file transfer, AI token streams, RPC.
    /// Drained last; bounded by the largest per-priority byte cap.
    Throughput = 2,
}

impl Priority {
    /// Derives the priority from the frame type and the stream's delivery
    /// policy.
    ///
    /// Control frames always get [`Priority::Control`] regardless of the
    /// stream's policy. Data frames inherit the stream's latency class:
    /// [`DeliveryPolicy::RealtimeDropOld`] and [`DeliveryPolicy::LatestOnly`]
    /// map to [`Priority::Realtime`]; everything else to
    /// [`Priority::Throughput`].
    #[inline]
    #[must_use]
    pub fn derive(msg_type: MessageType, policy: Option<DeliveryPolicy>) -> Self {
        if is_control(msg_type) {
            return Self::Control;
        }
        match policy {
            Some(DeliveryPolicy::RealtimeDropOld) | Some(DeliveryPolicy::LatestOnly) => Self::Realtime,
            _ => Self::Throughput,
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
        ] {
            assert_eq!(Priority::derive(mt, None), Priority::Control);
            assert_eq!(
                Priority::derive(mt, Some(DeliveryPolicy::RealtimeDropOld)),
                Priority::Control
            );
        }
    }

    #[test]
    fn realtime_policy_maps_to_realtime() {
        assert_eq!(
            Priority::derive(MessageType::Publish, Some(DeliveryPolicy::RealtimeDropOld)),
            Priority::Realtime
        );
        assert_eq!(
            Priority::derive(MessageType::Publish, Some(DeliveryPolicy::LatestOnly)),
            Priority::Realtime
        );
    }

    #[test]
    fn reliable_policies_map_to_throughput() {
        assert_eq!(
            Priority::derive(MessageType::Publish, Some(DeliveryPolicy::ReliableOrdered)),
            Priority::Throughput
        );
        assert_eq!(
            Priority::derive(MessageType::Publish, Some(DeliveryPolicy::ReliableUnordered)),
            Priority::Throughput
        );
        assert_eq!(
            Priority::derive(MessageType::Publish, None),
            Priority::Throughput
        );
    }

    #[test]
    fn ordering_is_control_realtime_throughput() {
        assert!(Priority::Control < Priority::Realtime);
        assert!(Priority::Realtime < Priority::Throughput);
    }
}
