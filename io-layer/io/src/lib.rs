//! Production-grade io_uring IO layer for Vireon.
//!
//! A thread-per-core reactor built on Linux `io_uring` — the kernel
//! boundary for the Vireon QUIC engine. Target: 1 M+ concurrent
//! connections with sub-ms tail latency.
//!
//! # Architecture
//!
//! ```text
//! +-------------------------------------------------------------------+
//! |                       REACTOR (CPU Core N)                        |
//! |  ┌─────────────────────────────────────────────────────────────┐ |
//! |  │                  Event Loop (run())                         │ |
//! |  │   submit SQEs ─── io_uring_submit_and_wait ─── reap CQEs    │ |
//! |  └─────────────────────────────────────────────────────────────┘ |
//! |       │                │                 │              │        |
//! |       ▼                ▼                 ▼              ▼        |
//! |  IoUringCtx        BufferPool        TimerWheel     Metrics      |
//! |  (ring setup,      (PBUF_RING,       (IORING_OP_    (atomic      |
//! |   fixed bufs,       refill)           TIMEOUT +      counters)   |
//! |   sparse files)                       CANCEL)                    |
//! |       │                                                          |
//! |       ▼                                                          |
//! |  Connection (UDP SO_REUSEPORT, recvmsg_multishot, send_zc)       |
//! +-------------------------------------------------------------------+
//! ```
//!
//! # Quick start
//!
//! ```no_run
//! use io::Reactor;
//!
//! let mut reactor = Reactor::init(0, 8080, 0)?;
//! reactor.run()?;
//! # Ok::<(), io::IoError>(())
//! ```
//!
//! # Layer map
//!
//! | Layer | Module | Responsibility |
//! |-------|--------|---------------|
//! | 1 | [`common`] | Constants, protocol opcodes |
//! | 2 | [`error`] | `IoError` thiserror enum |
//! | 3 | [`io_uring_ctx`] | `IoUringCtx` ring setup + registrations |
//! | 4 | [`buffer_pool`] | Provided-buffer ring manager |
//! | 5 | [`timer_wheel`] | Kernel-side io_uring timeouts |
//! | 6 | [`connection`] | UDP socket + multishot RX + send_zc TX |
//! | 7 | [`metrics`] | Atomic counters + snapshot |
//! | 8 | [`reactor`] | Thread-per-core event loop |
//!
//! # Feature flags
//!
//! None yet. A future `prometheus` feature will wire a Prometheus
//! exporter to [`MetricsCollector::snapshot`](metrics::MetricsCollector::snapshot).
//!
//! # Design choices
//!
//! - **Typed `io-uring` v0.7** wrapper. Avoids hand-rolling SQE/CQE
//!   structs; provides `RecvMsgMulti`, `SendZc`, `Timeout`,
//!   `BufRingEntry` helpers.
//! - **Thread-per-core** — no shared state between reactors; each owns a
//!   private ring, buffer pool, and UDP socket bound with `SO_REUSEPORT`.
//! - **`IORING_SETUP_SINGLE_ISSUER | DEFER_TASKRUN | COOP_TASKRUN`** —
//!   halves context-switch cost vs. the legacy task-work path.
//! - **Provided buffer rings** (`IORING_REGISTER_PBUF_RING`) for
//!   zero-copy RX — the kernel posts recv'd datagrams directly into our
//!   pool, no per-SQE buffer pointer.
//! - **`SEND_ZC`** for zero-copy TX with a two-CQE lifecycle (send
//!   status + buffer-reclaim notification).
//!
//! [`common`]: crate::common
//! [`error`]: crate::error
//! [`io_uring_ctx`]: crate::io_uring_ctx
//! [`buffer_pool`]: crate::buffer_pool
//! [`timer_wheel`]: crate::timer_wheel
//! [`connection`]: crate::connection
//! [`metrics`]: crate::metrics
//! [`reactor`]: crate::reactor

#![allow(clippy::module_inception)]

pub mod buffer_pool;
pub mod common;
pub mod connection;
pub mod error;
pub mod io_uring_ctx;
pub mod metrics;
pub mod reactor;
pub mod timer_wheel;

pub use buffer_pool::BufferPool;
pub use common::{ControlPlane, DataPlane};
pub use connection::{Connection, ParsedFrame, TxNotification};
pub use error::IoError;
pub use io_uring_ctx::IoUringCtx;
pub use metrics::{MetricsCollector, MetricsSnapshot};
pub use reactor::Reactor;
pub use timer_wheel::TimerWheel;
