//! Shared helpers for the `kernel-nic` crate benchmark suite.

#![allow(clippy::expect_used, clippy::unwrap_used, dead_code)]

use criterion::BatchSize;

pub const SMALL_BATCH: BatchSize = BatchSize::SmallInput;
pub const LARGE_BATCH: BatchSize = BatchSize::LargeInput;
