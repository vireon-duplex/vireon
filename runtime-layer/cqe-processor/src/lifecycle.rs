//! Outcome / lifecycle (Layer 11).
//!
//! Each handler returns an [`Outcome`] describing what the dispatcher should
//! record and what follow-up work the reactor (or test) must apply. This
//! keeps handlers decoupled from concrete runtime modules — a handler never
//! calls `SendQueue::on_send_cqe` directly; it emits an event.

/// What a handler wants the dispatcher to do after the handler returns.
#[derive(Clone, Copy, Debug, Default)]
pub struct Outcome {
    /// `true` if the handler recognised the completion variant.
    /// `false` (default) means the variant did not match the handler's
    /// expectation; the dispatcher records it as `skipped`.
    pub consumed: bool,
    /// Request to schedule a follow-up task on the Task Queue. The reactor
    /// inspects [`Outcome::forward`] for the typed event.
    pub schedule_task: bool,
    /// `Some(buf_id)` if a buffer should be returned to the Buffer Pool.
    /// Set by `process_send_zc` when the application has registered the
    /// `id → buf_id` mapping via [`CqeProcessor::register_send`](crate::processor::CqeProcessor::register_send).
    pub return_buffer: Option<u16>,
    /// Forward an event to the Send Queue.
    pub forward: Option<SendQueueEvent>,
    /// Forward an event to the (kernel-side) Timer Wheel.
    pub retire_timer: Option<u64>,
    /// `true` if the multishot RX should be re-armed at the next submission
    /// boundary. Recorded in [`MultishotState`](crate::multishot::MultishotState).
    pub rearm_multishot: bool,
}

impl Outcome {
    /// Construct an outcome that records consumption with no follow-up.
    #[inline]
    #[must_use]
    pub fn consumed() -> Self {
        Self {
            consumed: true,
            ..Self::default()
        }
    }
}

/// Send Queue-bound events emitted by the dispatch handlers.
#[derive(Clone, Copy, Debug)]
pub enum SendQueueEvent {
    /// Forward `on_send_cqe(id)` — first CQE of a SEND_ZC.
    SendCqe(u64),
    /// Forward `on_reclaim_cqe(id)` — second CQE of a SEND_ZC.
    ReclaimCqe(u64),
    /// Forward `on_timeout(id)` — RTO expiry for a packet.
    Timeout(u64),
}
