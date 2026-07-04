//! Typed errors for the `io` crate.
//!
//! Every fallible public operation returns [`IoError`]; no site in non-test
//! code panics (per the workspace lint rule `panic = "deny"`).

/// Errors returned by the io_uring-based IO layer.
#[derive(Debug, thiserror::Error)]
pub enum IoError {
    /// `io_uring_setup(2)` failed — the kernel refused to create the ring.
    #[error("io_uring setup failed: {0}")]
    RingSetup(#[from] std::io::Error),

    /// `io_uring_enter(2)` failed to submit SQEs or wait for CQEs.
    #[error("io_uring submit failed: {0}")]
    Submit(#[source] std::io::Error),

    /// A provided-buffer group ran out of spare buffers for incoming recv
    /// completions. Recovery is to refill the group and re-arm RX.
    #[error("buffer pool exhausted (bgid={bgid})")]
    BufferExhaustion {
        /// Buffer-group id that ran dry.
        bgid: u16,
    },

    /// The kernel reported CQ overflow (`IORING_CQE_F_OVERFLOW` observed
    /// or `cq.koverflow > 0`). With `IORING_SETUP_SQ_AGGRESSIVE_NODROP`
    /// equivalent behaviour this means CQEs were dropped.
    #[error("CQ overflow detected on ring {ring_id}")]
    CqOverflow {
        /// Identifier of the offending ring (reactor core id by convention).
        ring_id: u32,
    },

    /// Caller-supplied configuration is invalid (e.g. zero entries, zero
    /// shards, unsupported flag combination).
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),

    /// Submission was rejected under backpressure (typically `-EBUSY`).
    /// Drain CQEs to free SQ slots and retry.
    #[error("backpressure: submission choked (errno={errno})")]
    Backpressure {
        /// Negated errno value returned by the kernel.
        errno: i32,
    },

    /// The running kernel does not expose a required io_uring feature
    /// (e.g. `IORING_OP_RECVMSG_MULTISHOT` on kernels < 6.0).
    #[error("kernel feature unavailable: {0}")]
    Unsupported(String),

    /// UDP socket setup (`socket(2)`, `setsockopt(2)`, `bind(2)`) failed.
    #[error("socket setup failed: {0}")]
    Socket(#[source] std::io::Error),

    /// The kernel returned a negated errno in a CQE's `res` field.
    /// The caller may map it to a `libc::E*` constant for handling.
    #[error("negated errno from kernel: {0}")]
    Kernel(i32),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_messages_are_informative() {
        let io_err = std::io::Error::from_raw_os_error(libc::EBUSY);
        let msg = IoError::RingSetup(io_err).to_string();
        assert!(
            msg.starts_with("io_uring setup failed:"),
            "unexpected prefix: {msg}"
        );
        assert!(msg.contains("os error 16"), "missing errno: {msg}");
    }

    #[test]
    fn buffer_exhaustion_carries_bgid() {
        let err = IoError::BufferExhaustion { bgid: 7 };
        assert_eq!(err.to_string(), "buffer pool exhausted (bgid=7)");
    }

    #[test]
    fn cq_overflow_carries_ring_id() {
        let err = IoError::CqOverflow { ring_id: 3 };
        assert_eq!(err.to_string(), "CQ overflow detected on ring 3");
    }

    #[test]
    fn invalid_config_string_interpolates() {
        let err = IoError::InvalidConfig("entries must be > 0".into());
        assert_eq!(
            err.to_string(),
            "invalid configuration: entries must be > 0"
        );
    }

    #[test]
    fn backpressure_carries_errno() {
        let err = IoError::Backpressure { errno: libc::EBUSY };
        assert!(err.to_string().contains("16"));
    }

    #[test]
    fn unsupported_lists_feature() {
        let err = IoError::Unsupported("IORING_OP_RECVMSG_MULTISHOT".into());
        assert!(err.to_string().contains("RECVMSG_MULTISHOT"));
    }

    #[test]
    fn kernel_errno_passthrough() {
        let err = IoError::Kernel(-libc::EINVAL);
        assert_eq!(err.to_string(), "negated errno from kernel: -22");
    }
}
