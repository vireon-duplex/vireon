//! [`Pacer`] — token-bucket pacer integrated with [`send-queue`].
//!
//! Phase 10 — Packet Scheduler (pacing half). See `DESIGN.md` §4.10.
//!
//! Pacing prevents micro-bursts that overwhelm downstream switches.
//! The pacer releases bytes at a rate set by the congestion
//! controller (Phase 5) and burst size derived from the cwnd and
//! RTT. Implementation is a standard token bucket:
//!
//! ```text
//! tokens += (now - last_refill) * rate
//! if tokens > burst_cap { tokens = burst_cap }
//! if tokens >= packet_size { emit packet; tokens -= packet_size }
//! else { hold until next refill }
//! ```
//!
//! Integration with [`send-queue`]: the pacer doesn't directly
//! hold packets — it tells the scheduler "is it OK to emit N bytes
//! right now?" and the scheduler then enqueues into `send-queue`
//! which batches into the next `sendmmsg` / GSO burst.

/// Phase-0 skeleton. Phase 10 populates the bucket arithmetic.
#[derive(Clone, Copy, Debug)]
pub struct Pacer {
    /// Current token count (in bytes).
    tokens: u64,
    /// Maximum token bucket size (burst cap).
    burst_bytes: u64,
    /// Refill rate (bytes per second).
    rate_bytes_per_sec: u64,
    /// Monotonic nanos of last refill.
    last_refill_ns: u64,
}

impl Pacer {
    /// TODO(phase-10): `fn new(rate, burst)` and `fn try_emit(&mut self,
    /// bytes, now_ns) -> bool`.
    #[must_use]
    pub const fn placeholder() -> Self {
        Self {
            tokens: 0,
            burst_bytes: 0,
            rate_bytes_per_sec: 0,
            last_refill_ns: 0,
        }
    }
}

impl Default for Pacer {
    fn default() -> Self {
        Self::placeholder()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn placeholder() {
        let _p = Pacer::placeholder();
        // TODO(phase-10): token-bucket arithmetic, pacing-rate
        // derivation from cwnd/RTT, send-queue integration.
    }
}
