//! A high-performance realtime runtime built on QUIC and `io_uring` for multi-stream, full-duplex communication at massive scale.

// =============================================================================
// Production Safety Lints for Rust
// =============================================================================

// --- Safety: Forbid unsafe code ---
#![deny(unsafe_code)]
// --- Clippy Lint Categories ---
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![warn(clippy::cargo)]
// --- Panic Prevention ---
#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
#![deny(clippy::unimplemented)]
#![deny(clippy::todo)]
#![deny(clippy::unreachable)]
// --- Error Handling ---
#![deny(clippy::unwrap_in_result)]
#![warn(clippy::missing_errors_doc)]
#![warn(clippy::missing_panics_doc)]
// --- Security ---
#![deny(clippy::indexing_slicing)]
#![warn(clippy::integer_division)]
// --- Code Quality ---
#![deny(clippy::mem_forget)]
#![deny(clippy::multiple_inherent_impl)]
#![warn(clippy::clone_on_ref_ptr)]
#![warn(clippy::cognitive_complexity)]
#![warn(clippy::debug_assert_with_mut_call)]
// --- Documentation ---
#![warn(missing_docs)]
#![warn(clippy::missing_docs_in_private_items)]
// --- Rust Standard Lints ---
#![deny(missing_debug_implementations)]
#![deny(missing_copy_implementations)]
#![deny(trivial_casts)]
#![deny(trivial_numeric_casts)]
#![deny(unused_import_braces)]
#![deny(unused_qualifications)]
#![deny(rust_2018_idioms)]
#![deny(nonstandard_style)]
#![deny(dead_code)]
#![allow(clippy::multiple_crate_versions)]
// --- License metadata deferred until a decision is made ---
#![allow(clippy::cargo_common_metadata)]
