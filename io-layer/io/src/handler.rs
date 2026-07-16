//! [`IoHandler`] — callback trait for upper-layer packet processing.
//!
//! The reactor calls [`IoHandler::on_datagram`] for every received UDP
//! datagram and [`IoHandler::on_tick`] at the end of each event-loop
//! iteration. Upper layers (e.g. `quic-engine::QuicEngine`) implement this
//! trait to process packets without owning the io_uring ring themselves.
//!
//! # Pattern
//!
//! ```text
//! io_uring CQE ──► Reactor::run_with_handler
//!                      │
//!                      ├─ Receive  ──► handler.on_datagram(data, peer)
//!                      ├─ Timeout  ──► handler.on_timeout(id)
//!                      ├─ SendReclaim ──► handler.on_send_reclaim(id)
//!                      └─ end-of-tick ──► handler.on_tick(ctx)
//! ```
//!
//! `on_tick` receives a [`TickContext`] that exposes the sender for
//! outgoing packets. The handler typically flushes pending sends here.

use std::net::SocketAddr;

use io_uring::IoUring;

use crate::ops::Sender;

/// Callback interface implemented by upper layers.
///
/// Every method has a default no-op implementation except
/// [`on_datagram`](Self::on_datagram) and [`on_tick`](Self::on_tick),
/// which upper layers must implement.
pub trait IoHandler {
    /// Called when a UDP datagram is received.
    ///
    /// `data` is the full UDP payload (after io_uring header stripping).
    /// `peer` is the source address. The slice is only valid for the
    /// duration of this call — the reactor recycles the buffer immediately
    /// after the handler returns.
    ///
    /// The buffer is mutable so the handler can pass it directly to
    /// libraries that require `&mut [u8]` (e.g. `quiche::Connection::recv`),
    /// avoiding an extra copy.
    fn on_datagram(&mut self, data: &mut [u8], peer: SocketAddr);

    /// Called at the end of each event-loop tick.
    ///
    /// The handler should flush pending sends via [`TickContext`].
    /// This is where quiche's `Connection::send()` output is submitted
    /// to the io_uring ring.
    fn on_tick(&mut self, ctx: &mut TickContext);

    /// Called when a kernel-side timeout fires.
    fn on_timeout(&mut self, _id: u64) {}

    /// Called when a zero-copy send's buffer can be recycled
    /// (SEND_ZC notification CQE).
    fn on_send_reclaim(&mut self, _id: u64) {}
}

/// Context passed to [`IoHandler::on_tick`].
///
/// Provides access to the sender for outgoing packet transmission.
/// The ring is also exposed for handlers that need to submit custom SQEs.
pub struct TickContext<'a> {
    /// The io_uring ring — for SQE submission.
    pub ring: &'a mut IoUring,
    /// The UDP sender — for `send_zc` or `send_blocking`.
    pub sender: &'a Sender,
}
