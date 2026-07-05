//! Shared bench helpers for quic-engine.
//!
//! Phase 0 ships only the helper skeletons; bench bodies land in
//! Phases 1–14 alongside the subsystem implementations they
//! exercise.

#![allow(clippy::expect_used, clippy::unwrap_used, dead_code)]

use quic_engine::{QuicConfig, QuicEngineBuilder};

/// Construct a default-configured engine for the given reactor.
///
/// TODO(phase-13): replace with a real engine that has wired-up
/// runtime primitives.
pub fn fresh_engine(reactor_id: u32) -> quic_engine::QuicEngine {
    QuicEngineBuilder::from_preset(quic_engine::QuicPreset::BareMetal)
        .reactor_id(reactor_id)
        .build()
        .expect("engine build")
}

/// Construct a config from a preset without building the engine.
pub fn preset_config(preset: quic_engine::QuicPreset) -> QuicConfig {
    preset.to_config()
}
