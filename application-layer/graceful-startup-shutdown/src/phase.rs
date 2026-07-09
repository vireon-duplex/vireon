//! [`StartupPhase`], [`ShutdownPhase`] — phase state machines for
//! graceful startup and shutdown.
//!
//! See `DESIGN.md` Part 5 (phase machine).
//!
//! # Startup Phase Machine
//!
//! Phases progress in order: `Init` → `LoadConfig` → `BindPorts` →
//! `AcceptConnections` → `Ready`. Each phase must complete before
//! the next can begin.
//!
//! # Shutdown Phase Machine
//!
//! Phases progress in order: `Running` → `StopAccepting` →
//! `DrainConnections` → `StopModules` → `FlushState` →
//! `ReleaseResources` → `Complete`.

// ── StartupPhase ────────────────────────────────────────────────

/// Startup phase state machine.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum StartupPhase {
    /// Initial state.
    #[default]
    Init = 0,
    /// Load configuration.
    LoadConfig = 1,
    /// Bind network ports.
    BindPorts = 2,
    /// Begin accepting connections.
    AcceptConnections = 3,
    /// System ready.
    Ready = 4,
}

impl StartupPhase {
    /// Whether the phase is `Ready` (startup complete).
    #[must_use]
    pub const fn is_ready(&self) -> bool {
        matches!(self, Self::Ready)
    }

    /// Whether the phase is active (not Init).
    #[must_use]
    pub const fn is_active(&self) -> bool {
        !matches!(self, Self::Init)
    }

    /// Whether this phase can transition to the target phase.
    #[must_use]
    pub const fn can_transition_to(&self, target: Self) -> bool {
        self.order() + 1 == target.order()
    }

    /// Human-readable name for logs and metrics.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Init => "Init",
            Self::LoadConfig => "LoadConfig",
            Self::BindPorts => "BindPorts",
            Self::AcceptConnections => "AcceptConnections",
            Self::Ready => "Ready",
        }
    }

    /// Numeric order of the phase for comparison.
    #[must_use]
    pub const fn order(&self) -> u8 {
        *self as u8
    }

    /// Get the phase from its raw u8 representation.
    #[must_use]
    pub const fn from_raw(raw: u8) -> Option<Self> {
        match raw {
            0 => Some(Self::Init),
            1 => Some(Self::LoadConfig),
            2 => Some(Self::BindPorts),
            3 => Some(Self::AcceptConnections),
            4 => Some(Self::Ready),
            _ => None,
        }
    }
}

// ── ShutdownPhase ───────────────────────────────────────────────

/// Shutdown phase state machine.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum ShutdownPhase {
    /// System is running (not shutting down).
    #[default]
    Running = 0,
    /// Stop accepting new connections.
    StopAccepting = 1,
    /// Drain existing connections.
    DrainConnections = 2,
    /// Stop application modules.
    StopModules = 3,
    /// Flush state to disk.
    FlushState = 4,
    /// Release system resources.
    ReleaseResources = 5,
    /// Shutdown complete.
    Complete = 6,
}

impl ShutdownPhase {
    /// Whether the phase is `Complete` (shutdown finished).
    #[must_use]
    pub const fn is_complete(&self) -> bool {
        matches!(self, Self::Complete)
    }

    /// Whether the phase is draining (between StopAccepting and DrainConnections).
    #[must_use]
    pub const fn is_draining(&self) -> bool {
        matches!(self, Self::StopAccepting | Self::DrainConnections)
    }

    /// Whether this phase can transition to the target phase.
    #[must_use]
    pub const fn can_transition_to(&self, target: Self) -> bool {
        self.order() + 1 == target.order()
    }

    /// Human-readable name for logs and metrics.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Running => "Running",
            Self::StopAccepting => "StopAccepting",
            Self::DrainConnections => "DrainConnections",
            Self::StopModules => "StopModules",
            Self::FlushState => "FlushState",
            Self::ReleaseResources => "ReleaseResources",
            Self::Complete => "Complete",
        }
    }

    /// Numeric order of the phase for comparison.
    #[must_use]
    pub const fn order(&self) -> u8 {
        *self as u8
    }

    /// Get the phase from its raw u8 representation.
    #[must_use]
    pub const fn from_raw(raw: u8) -> Option<Self> {
        match raw {
            0 => Some(Self::Running),
            1 => Some(Self::StopAccepting),
            2 => Some(Self::DrainConnections),
            3 => Some(Self::StopModules),
            4 => Some(Self::FlushState),
            5 => Some(Self::ReleaseResources),
            6 => Some(Self::Complete),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn startup_phase_order() {
        assert_eq!(StartupPhase::Init.order(), 0);
        assert_eq!(StartupPhase::LoadConfig.order(), 1);
        assert_eq!(StartupPhase::BindPorts.order(), 2);
        assert_eq!(StartupPhase::AcceptConnections.order(), 3);
        assert_eq!(StartupPhase::Ready.order(), 4);
    }

    #[test]
    fn startup_phase_is_ready() {
        assert!(!StartupPhase::Init.is_ready());
        assert!(!StartupPhase::LoadConfig.is_ready());
        assert!(!StartupPhase::BindPorts.is_ready());
        assert!(!StartupPhase::AcceptConnections.is_ready());
        assert!(StartupPhase::Ready.is_ready());
    }

    #[test]
    fn startup_phase_is_active() {
        assert!(!StartupPhase::Init.is_active());
        assert!(StartupPhase::LoadConfig.is_active());
        assert!(StartupPhase::BindPorts.is_active());
        assert!(StartupPhase::AcceptConnections.is_active());
        assert!(StartupPhase::Ready.is_active());
    }

    #[test]
    fn startup_phase_can_transition() {
        assert!(StartupPhase::Init.can_transition_to(StartupPhase::LoadConfig));
        assert!(StartupPhase::LoadConfig.can_transition_to(StartupPhase::BindPorts));
        assert!(StartupPhase::BindPorts.can_transition_to(StartupPhase::AcceptConnections));
        assert!(StartupPhase::AcceptConnections.can_transition_to(StartupPhase::Ready));

        // Cannot skip phases
        assert!(!StartupPhase::Init.can_transition_to(StartupPhase::BindPorts));
        assert!(!StartupPhase::LoadConfig.can_transition_to(StartupPhase::Ready));

        // Cannot transition to self
        assert!(!StartupPhase::Init.can_transition_to(StartupPhase::Init));
    }

    #[test]
    fn startup_phase_as_str() {
        assert_eq!(StartupPhase::Init.as_str(), "Init");
        assert_eq!(StartupPhase::LoadConfig.as_str(), "LoadConfig");
        assert_eq!(StartupPhase::BindPorts.as_str(), "BindPorts");
        assert_eq!(
            StartupPhase::AcceptConnections.as_str(),
            "AcceptConnections"
        );
        assert_eq!(StartupPhase::Ready.as_str(), "Ready");
    }

    #[test]
    fn startup_phase_from_raw() {
        assert_eq!(StartupPhase::from_raw(0), Some(StartupPhase::Init));
        assert_eq!(StartupPhase::from_raw(1), Some(StartupPhase::LoadConfig));
        assert_eq!(StartupPhase::from_raw(2), Some(StartupPhase::BindPorts));
        assert_eq!(
            StartupPhase::from_raw(3),
            Some(StartupPhase::AcceptConnections)
        );
        assert_eq!(StartupPhase::from_raw(4), Some(StartupPhase::Ready));
        assert_eq!(StartupPhase::from_raw(5), None);
        assert_eq!(StartupPhase::from_raw(255), None);
    }

    #[test]
    fn shutdown_phase_order() {
        assert_eq!(ShutdownPhase::Running.order(), 0);
        assert_eq!(ShutdownPhase::StopAccepting.order(), 1);
        assert_eq!(ShutdownPhase::DrainConnections.order(), 2);
        assert_eq!(ShutdownPhase::StopModules.order(), 3);
        assert_eq!(ShutdownPhase::FlushState.order(), 4);
        assert_eq!(ShutdownPhase::ReleaseResources.order(), 5);
        assert_eq!(ShutdownPhase::Complete.order(), 6);
    }

    #[test]
    fn shutdown_phase_is_complete() {
        assert!(!ShutdownPhase::Running.is_complete());
        assert!(!ShutdownPhase::StopAccepting.is_complete());
        assert!(!ShutdownPhase::DrainConnections.is_complete());
        assert!(!ShutdownPhase::StopModules.is_complete());
        assert!(!ShutdownPhase::FlushState.is_complete());
        assert!(!ShutdownPhase::ReleaseResources.is_complete());
        assert!(ShutdownPhase::Complete.is_complete());
    }

    #[test]
    fn shutdown_phase_is_draining() {
        assert!(!ShutdownPhase::Running.is_draining());
        assert!(ShutdownPhase::StopAccepting.is_draining());
        assert!(ShutdownPhase::DrainConnections.is_draining());
        assert!(!ShutdownPhase::StopModules.is_draining());
        assert!(!ShutdownPhase::FlushState.is_draining());
        assert!(!ShutdownPhase::ReleaseResources.is_draining());
        assert!(!ShutdownPhase::Complete.is_draining());
    }

    #[test]
    fn shutdown_phase_can_transition() {
        assert!(ShutdownPhase::Running.can_transition_to(ShutdownPhase::StopAccepting));
        assert!(ShutdownPhase::StopAccepting.can_transition_to(ShutdownPhase::DrainConnections));
        assert!(ShutdownPhase::DrainConnections.can_transition_to(ShutdownPhase::StopModules));
        assert!(ShutdownPhase::StopModules.can_transition_to(ShutdownPhase::FlushState));
        assert!(ShutdownPhase::FlushState.can_transition_to(ShutdownPhase::ReleaseResources));
        assert!(ShutdownPhase::ReleaseResources.can_transition_to(ShutdownPhase::Complete));

        // Cannot skip phases
        assert!(!ShutdownPhase::Running.can_transition_to(ShutdownPhase::DrainConnections));
        assert!(!ShutdownPhase::StopAccepting.can_transition_to(ShutdownPhase::StopModules));

        // Cannot transition to self
        assert!(!ShutdownPhase::Running.can_transition_to(ShutdownPhase::Running));
    }

    #[test]
    fn shutdown_phase_as_str() {
        assert_eq!(ShutdownPhase::Running.as_str(), "Running");
        assert_eq!(ShutdownPhase::StopAccepting.as_str(), "StopAccepting");
        assert_eq!(ShutdownPhase::DrainConnections.as_str(), "DrainConnections");
        assert_eq!(ShutdownPhase::StopModules.as_str(), "StopModules");
        assert_eq!(ShutdownPhase::FlushState.as_str(), "FlushState");
        assert_eq!(ShutdownPhase::ReleaseResources.as_str(), "ReleaseResources");
        assert_eq!(ShutdownPhase::Complete.as_str(), "Complete");
    }

    #[test]
    fn shutdown_phase_from_raw() {
        assert_eq!(ShutdownPhase::from_raw(0), Some(ShutdownPhase::Running));
        assert_eq!(
            ShutdownPhase::from_raw(1),
            Some(ShutdownPhase::StopAccepting)
        );
        assert_eq!(
            ShutdownPhase::from_raw(2),
            Some(ShutdownPhase::DrainConnections)
        );
        assert_eq!(ShutdownPhase::from_raw(3), Some(ShutdownPhase::StopModules));
        assert_eq!(ShutdownPhase::from_raw(4), Some(ShutdownPhase::FlushState));
        assert_eq!(
            ShutdownPhase::from_raw(5),
            Some(ShutdownPhase::ReleaseResources)
        );
        assert_eq!(ShutdownPhase::from_raw(6), Some(ShutdownPhase::Complete));
        assert_eq!(ShutdownPhase::from_raw(7), None);
        assert_eq!(ShutdownPhase::from_raw(255), None);
    }

    #[test]
    fn startup_phase_repr_u8() {
        assert_eq!(StartupPhase::Init as u8, 0);
        assert_eq!(StartupPhase::LoadConfig as u8, 1);
        assert_eq!(StartupPhase::BindPorts as u8, 2);
        assert_eq!(StartupPhase::AcceptConnections as u8, 3);
        assert_eq!(StartupPhase::Ready as u8, 4);
    }

    #[test]
    fn shutdown_phase_repr_u8() {
        assert_eq!(ShutdownPhase::Running as u8, 0);
        assert_eq!(ShutdownPhase::StopAccepting as u8, 1);
        assert_eq!(ShutdownPhase::DrainConnections as u8, 2);
        assert_eq!(ShutdownPhase::StopModules as u8, 3);
        assert_eq!(ShutdownPhase::FlushState as u8, 4);
        assert_eq!(ShutdownPhase::ReleaseResources as u8, 5);
        assert_eq!(ShutdownPhase::Complete as u8, 6);
    }

    #[test]
    fn phases_are_default() {
        assert_eq!(StartupPhase::default(), StartupPhase::Init);
        assert_eq!(ShutdownPhase::default(), ShutdownPhase::Running);
    }
}
