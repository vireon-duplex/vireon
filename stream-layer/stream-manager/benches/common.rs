//! Shared bench helpers for `stream-manager`.
//!
//! Phase 0 ships an empty module so the bench harness compiles.
//! Functional fixtures (synthetic stream tables, random id streams,
//! pre-warmed slabs) land in their respective phases alongside the
//! code they exercise.

#![allow(dead_code)]
