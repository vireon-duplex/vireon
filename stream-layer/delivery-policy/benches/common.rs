//! Shared helpers for delivery-policy benchmarks.

#![allow(clippy::unwrap_used, clippy::expect_used, dead_code)]

use bytes::Bytes;
use delivery_policy::FramePayload;
use frame::header::{FrameFlags, MessageType};

/// Build a `FramePayload` with `len` bytes of payload. Uses `MessageType::TextToken`
/// (the cheapest data-plane type) and `FrameFlags::NONE` so the bench measures
/// policy overhead, not header construction.
#[inline]
pub fn payload(len: usize) -> FramePayload {
    FramePayload::new(
        MessageType::TextToken,
        FrameFlags::NONE,
        0,
        Bytes::from(vec![0u8; len]),
    )
}

/// Realistic payload size for a single audio/video-style frame.
pub const PAYLOAD_SMALL: usize = 128;
