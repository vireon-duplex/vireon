//! Shared constants and protocol opcodes for the `io` crate.
//!
//! Everything here is dependency-free so any module can pull in the numeric
//! bounds without dragging in `io-uring` or `libc`.

/// Default SQ/CQ depth. Generous enough for a 1 M-connection target on a
/// single core once provided-buffer rings absorb the per-recv buffering.
pub const QUEUE_DEPTH: u32 = 1024;

/// Saturation threshold (in bytes outstanding) above which the reactor
/// applies backpressure to producers.
pub const HIGH_WATERMARK: usize = 128 * 1024;

/// Threshold below which the reactor lifts backpressure again.
pub const LOW_WATERMARK: usize = 32 * 1024;

/// Default number of provided buffers per `bgid` (per reactor).
pub const DEFAULT_BUF_COUNT: u32 = 256;

/// Default size of each provided buffer. Covers a 1500-byte UDP MTU with
/// headroom for the `io_uring_recvmsg_out` header and peer-address field.
pub const DEFAULT_BUF_SIZE: usize = 2048;

/// Page size on the target architectures (x86_64, aarch64, …). Used to
/// align provided-buffer ring backing storage and `BufRingEntry` arrays.
pub const PAGE_SIZE: usize = 4096;

/// Wire opcodes emitted by the data plane (QUIC frames carried over UDP).
pub mod data_plane {
    /// Plain text payload token.
    pub const TEXT_TOKEN: u8 = 0x01;
    /// Binary payload token.
    pub const BINARY_TOKEN: u8 = 0x02;
    /// Stream-open request.
    pub const STREAM_OPEN: u8 = 0x03;
    /// Stream-close notification.
    pub const STREAM_CLOSE: u8 = 0x04;
    /// Reliable retransmit marker.
    pub const RETRANSMIT: u8 = 0x05;
    /// Flow-control update.
    pub const FLOW_CONTROL: u8 = 0x06;
    /// Path MTU discovery probe.
    pub const PMTU_PROBE: u8 = 0x07;
    /// Connection migration marker.
    pub const MIGRATION: u8 = 0x08;
    /// Keep-alive heartbeat.
    pub const HEARTBEAT: u8 = 0x09;
}

/// Wire opcodes emitted by the control plane (connection management).
pub mod control_plane {
    /// Open a new stream.
    pub const STREAM_OPEN: u8 = 0x0A;
    /// Close an existing stream.
    pub const STREAM_CLOSE: u8 = 0x0B;
    /// Reset a stream abnormally.
    pub const RESET: u8 = 0x0C;
    /// Acknowledge a control frame.
    pub const ACK: u8 = 0x0D;
    /// Negative acknowledge.
    pub const NACK: u8 = 0x0E;
    /// Drain pending work before shutdown.
    pub const DRAIN: u8 = 0x0F;
}

/// Typed view over a [`data_plane`] opcode byte.
///
/// `TryFrom<u8>` is the validated parser used by the receive path; unknown
/// opcodes yield [`IoError::InvalidConfig`](crate::error::IoError::InvalidConfig).
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataPlane {
    /// [`data_plane::TEXT_TOKEN`].
    TextToken = data_plane::TEXT_TOKEN,
    /// [`data_plane::BINARY_TOKEN`].
    BinaryToken = data_plane::BINARY_TOKEN,
    /// [`data_plane::STREAM_OPEN`].
    StreamOpen = data_plane::STREAM_OPEN,
    /// [`data_plane::STREAM_CLOSE`].
    StreamClose = data_plane::STREAM_CLOSE,
    /// [`data_plane::RETRANSMIT`].
    Retransmit = data_plane::RETRANSMIT,
    /// [`data_plane::FLOW_CONTROL`].
    FlowControl = data_plane::FLOW_CONTROL,
    /// [`data_plane::PMTU_PROBE`].
    PmtuProbe = data_plane::PMTU_PROBE,
    /// [`data_plane::MIGRATION`].
    Migration = data_plane::MIGRATION,
    /// [`data_plane::HEARTBEAT`].
    Heartbeat = data_plane::HEARTBEAT,
}

impl TryFrom<u8> for DataPlane {
    type Error = u8;

    fn try_from(value: u8) -> Result<Self, u8> {
        match value {
            data_plane::TEXT_TOKEN => Ok(Self::TextToken),
            data_plane::BINARY_TOKEN => Ok(Self::BinaryToken),
            data_plane::STREAM_OPEN => Ok(Self::StreamOpen),
            data_plane::STREAM_CLOSE => Ok(Self::StreamClose),
            data_plane::RETRANSMIT => Ok(Self::Retransmit),
            data_plane::FLOW_CONTROL => Ok(Self::FlowControl),
            data_plane::PMTU_PROBE => Ok(Self::PmtuProbe),
            data_plane::MIGRATION => Ok(Self::Migration),
            data_plane::HEARTBEAT => Ok(Self::Heartbeat),
            other => Err(other),
        }
    }
}

/// Typed view over a [`control_plane`] opcode byte.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlPlane {
    /// [`control_plane::STREAM_OPEN`].
    StreamOpen = control_plane::STREAM_OPEN,
    /// [`control_plane::STREAM_CLOSE`].
    StreamClose = control_plane::STREAM_CLOSE,
    /// [`control_plane::RESET`].
    Reset = control_plane::RESET,
    /// [`control_plane::ACK`].
    Ack = control_plane::ACK,
    /// [`control_plane::NACK`].
    Nack = control_plane::NACK,
    /// [`control_plane::DRAIN`].
    Drain = control_plane::DRAIN,
}

impl TryFrom<u8> for ControlPlane {
    type Error = u8;

    fn try_from(value: u8) -> Result<Self, u8> {
        match value {
            control_plane::STREAM_OPEN => Ok(Self::StreamOpen),
            control_plane::STREAM_CLOSE => Ok(Self::StreamClose),
            control_plane::RESET => Ok(Self::Reset),
            control_plane::ACK => Ok(Self::Ack),
            control_plane::NACK => Ok(Self::Nack),
            control_plane::DRAIN => Ok(Self::Drain),
            other => Err(other),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn data_plane_round_trip() {
        for code in [
            data_plane::TEXT_TOKEN,
            data_plane::BINARY_TOKEN,
            data_plane::STREAM_OPEN,
            data_plane::STREAM_CLOSE,
            data_plane::RETRANSMIT,
            data_plane::FLOW_CONTROL,
            data_plane::PMTU_PROBE,
            data_plane::MIGRATION,
            data_plane::HEARTBEAT,
        ] {
            let typed = DataPlane::try_from(code).expect("known opcode");
            assert_eq!(typed as u8, code);
        }
    }

    #[test]
    fn control_plane_round_trip() {
        for code in [
            control_plane::STREAM_OPEN,
            control_plane::STREAM_CLOSE,
            control_plane::RESET,
            control_plane::ACK,
            control_plane::NACK,
            control_plane::DRAIN,
        ] {
            let typed = ControlPlane::try_from(code).expect("known opcode");
            assert_eq!(typed as u8, code);
        }
    }

    #[test]
    fn unknown_opcode_is_rejected() {
        assert!(DataPlane::try_from(0xFF).is_err());
        assert!(ControlPlane::try_from(0x00).is_err());
    }

    #[test]
    fn constants_match_layout() {
        const _: () = assert!(HIGH_WATERMARK > LOW_WATERMARK);
        const _: () = assert!(DEFAULT_BUF_SIZE >= 1500);
        const _: () = assert!(PAGE_SIZE.is_power_of_two());
    }
}
