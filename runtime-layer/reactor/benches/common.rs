//! Shared helpers for reactor benchmarks.
//!
//! Pulled in via `mod common;` from any bench binary that needs
//! shared fixtures. Phase 0 ships an empty module so the bench
//! binaries compile today; helpers will grow as Phase 1+ bench
//! bodies land.

// TODO(phase-1+): shared bench fixtures:
//   - small / medium / large `ReactorConfig` builders
//   - a `NoopDriver` implementing `ReactorDriver` for harness use
//   - a synthetic `SubmissionSink` that records submissions without
//     touching a real io_uring ring
//   - sample CQE / timer / task mixes that drive the loop
