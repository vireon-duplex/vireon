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
//! |  Socket + Receiver + Sender (UDP SO_REUSEPORT, multishot, zc)    |
//! +-------------------------------------------------------------------+
//! ```
//!
//! # Quick start
//!
//! ```no_run
//! use io::Reactor;
//!
//! let mut reactor = Reactor::init(0, 8080, 0, false)?;
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
//! | 3 | [`context`] | `IoUringCtx` ring setup + registrations |
//! | 4 | [`memory`] | Provided-buffer ring manager (`MemoryRegion` + `BufferPool`) |
//! | 5 | [`timer_wheel`] | Kernel-side io_uring timeouts |
//! | 6 | [`socket`] | UDP socket bind + fd ownership |
//! | 7 | [`ops`] | `Receiver` (multishot RX) + `Sender` (send_zc) + SQE builders |
//! | 8 | [`metrics`] | Atomic counters + snapshot |
//! | 9 | [`reactor`] | Thread-per-core event loop |
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
//! [`context`]: crate::context
//! [`memory`]: crate::memory
//! [`timer_wheel`]: crate::timer_wheel
//! [`socket`]: crate::socket
//! [`ops`]: crate::ops
//! [`metrics`]: crate::metrics
//! [`reactor`]: crate::reactor

#![allow(clippy::module_inception)]

pub mod common;
pub mod completion;
pub mod context;
pub mod error;
pub mod features;
pub mod handler;
pub mod memory;
pub mod metrics;
pub mod ops;
pub mod reactor;
pub mod registry;
pub mod socket;
pub mod timer_wheel;

pub use common::{ControlPlane, DataPlane};
pub use completion::{Completion, CompletionDecoder};
pub use context::IoUringCtx;
pub use error::IoError;
pub use features::IoFeatures;
pub use handler::{IoHandler, TickContext};
pub use memory::BufferPool;
pub use memory::MemoryRegion;
pub use metrics::{MetricsCollector, MetricsSnapshot};
pub use ops::{
    DatagramMeta, OpKind, ParsedFrame, Receiver, Sender, decode_id, decode_kind, encode,
};
pub use reactor::Reactor;
pub use registry::IoRegistry;
pub use socket::Socket;
pub use timer_wheel::TimerWheel;
