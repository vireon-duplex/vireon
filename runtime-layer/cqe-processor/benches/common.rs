//! Shared helpers for the `cqe-processor` crate benchmark suite.

#![allow(clippy::expect_used, clippy::unwrap_used, dead_code)]

use criterion::BatchSize;

use cqe_processor::CqeProcessor;
use cqe_processor::CqeProcessorBuilder;

use io::completion::Completion;
use io::ops::{OpKind, encode};

// ── Workload constants ────────────────────────────────────────────

/// Synthetic batch sizes swept across benches.
pub const BATCH_SIZES: &[usize] = &[16, 64, 256, 1024];

/// Producer-side fill levels.
pub const FILL_PERCENTS: &[u8] = &[50, 75, 90, 95, 98, 99];

// ── Bench-size helpers ────────────────────────────────────────────

pub const SMALL_BATCH: BatchSize = BatchSize::SmallInput;
pub const LARGE_BATCH: BatchSize = BatchSize::LargeInput;

// ── Processor construction ────────────────────────────────────────

/// Build a processor sized for `workload_size` dispatches.
///
/// `batch_max_cqes` only caps the per-tick `drain_cq` batch — it does
/// **not** limit how many `dispatch_one` calls a bench can issue. We
/// therefore clamp the requested capacity to `MAX_BATCH_CQES` so that
/// large-workload benches (16k, 64k) build successfully while still
/// exercising the dispatch path at the requested depth.
pub fn fresh_processor(workload_size: usize) -> CqeProcessor {
    const MAX_BATCH_CQES: usize = 4096;
    let cap = workload_size.clamp(64, MAX_BATCH_CQES);
    CqeProcessorBuilder::new()
        .batch_max_cqes(cap)
        .build()
        .expect("processor build")
}

// ── Synthetic completion factories ────────────────────────────────

/// Build a synthetic Recv completion with `more = true` so the multishot
/// does not request a re-arm (avoids per-iteration state growth).
pub fn recv_completion(id: u64, len: usize) -> Completion {
    Completion::Receive {
        id,
        buf_id: Some((id & 0xFF) as u16),
        len,
        more: true,
    }
}

/// Build a synthetic Send completion (first CQE of SEND_ZC).
pub fn send_completion(id: u64) -> Completion {
    Completion::Send { id, res: 128 }
}

/// Build a synthetic SendBufferReclaim completion (second CQE of SEND_ZC).
pub fn reclaim_completion(id: u64) -> Completion {
    Completion::SendBufferReclaim { id }
}

/// Build a synthetic Timeout completion.
pub fn timeout_completion(id: u64) -> Completion {
    Completion::Timeout { id }
}

/// Build a synthetic Cancel completion.
pub fn cancel_completion(id: u64) -> Completion {
    Completion::Cancel { id, res: 0 }
}

/// Build the matching user_data for an OpKind.
pub fn user_data_for(kind: OpKind, id: u64) -> u64 {
    encode(kind, id)
}
