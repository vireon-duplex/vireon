//! [`ModulePhase`] — lifecycle phase state machine for modules.
//!
//! See `DESIGN.md` Part 6 (phase state machine).
//!
//! # State transitions
//!
//! The phase state machine follows these legal transitions:
//!
//! ```text
//!   Registered ──► Initialized ──► Starting ──► Active ──► Stopping ──► Stopped
//!        │                  │             │            │             │
//!        │                  │             │            │             │
//!        └──────────────────┴─────────────┴────────────┴─────────────┘
//!                                    │
//!                                    ▼
//!                                 Failed (from any non-terminal state)
//! ```

// ── ModulePhase ─────────────────────────────────────────────────────

/// Lifecycle phase of a module.
///
/// Phases form a state machine that guides module initialization,
/// startup, and shutdown in a dependency-ordered manner.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum ModulePhase {
    /// Module is registered but not yet initialized.
    #[default]
    Registered = 0,
    /// Module has been initialized (resources allocated).
    Initialized = 1,
    /// Module is starting up (transitioning to active).
    Starting = 2,
    /// Module is active and serving requests.
    Active = 3,
    /// Module is shutting down (graceful stop in progress).
    Stopping = 4,
    /// Module has stopped (resources released).
    Stopped = 5,
    /// Module failed to initialize/start/stop.
    Failed = 6,
}

impl ModulePhase {
    /// Whether the phase is active (serving requests).
    #[must_use]
    pub const fn is_active(self) -> bool {
        matches!(self, Self::Active)
    }

    /// Whether the phase is terminal (no further transitions possible).
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Stopped | Self::Failed)
    }

    /// Whether the phase is running (initialized or later, but not stopped/failed).
    #[must_use]
    pub const fn is_running(self) -> bool {
        matches!(
            self,
            Self::Initialized | Self::Starting | Self::Active | Self::Stopping
        )
    }

    /// Whether a transition to the target phase is allowed.
    #[must_use]
    pub const fn can_transition_to(self, target: Self) -> bool {
        // Use raw() comparison instead of == for derived PartialEq (not const)
        match (self, target) {
            // Forward progress
            (Self::Registered, Self::Initialized) => true,
            (Self::Initialized, Self::Starting) => true,
            (Self::Starting, Self::Active) => true,
            (Self::Active, Self::Stopping) => true,
            (Self::Stopping, Self::Stopped) => true,

            // Any non-terminal can go to Failed
            (src, Self::Failed) if !src.is_terminal() => true,

            // Same phase is always allowed (no-op), including terminal states
            (src, tgt) if src.raw() == tgt.raw() => true,

            // All other transitions are invalid
            _ => false,
        }
    }

    /// Human-readable name for logging and metrics.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Registered => "registered",
            Self::Initialized => "initialized",
            Self::Starting => "starting",
            Self::Active => "active",
            Self::Stopping => "stopping",
            Self::Stopped => "stopped",
            Self::Failed => "failed",
        }
    }

    /// Get the raw u8 value for serialization.
    #[must_use]
    pub const fn raw(self) -> u8 {
        self as u8
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_registered() {
        assert_eq!(ModulePhase::default(), ModulePhase::Registered);
    }

    #[test]
    fn is_active_only_for_active_phase() {
        assert!(ModulePhase::Active.is_active());
        assert!(!ModulePhase::Starting.is_active());
        assert!(!ModulePhase::Stopping.is_active());
        assert!(!ModulePhase::Registered.is_active());
    }

    #[test]
    fn is_terminal_for_stopped_and_failed() {
        assert!(ModulePhase::Stopped.is_terminal());
        assert!(ModulePhase::Failed.is_terminal());
        assert!(!ModulePhase::Active.is_terminal());
        assert!(!ModulePhase::Registered.is_terminal());
    }

    #[test]
    fn is_running_for_non_terminal_non_registered() {
        assert!(ModulePhase::Initialized.is_running());
        assert!(ModulePhase::Starting.is_running());
        assert!(ModulePhase::Active.is_running());
        assert!(ModulePhase::Stopping.is_running());
        assert!(!ModulePhase::Registered.is_running());
        assert!(!ModulePhase::Stopped.is_running());
        assert!(!ModulePhase::Failed.is_running());
    }

    #[test]
    fn forward_progress_transitions_allowed() {
        assert!(ModulePhase::Registered.can_transition_to(ModulePhase::Initialized));
        assert!(ModulePhase::Initialized.can_transition_to(ModulePhase::Starting));
        assert!(ModulePhase::Starting.can_transition_to(ModulePhase::Active));
        assert!(ModulePhase::Active.can_transition_to(ModulePhase::Stopping));
        assert!(ModulePhase::Stopping.can_transition_to(ModulePhase::Stopped));
    }

    #[test]
    fn reverse_transitions_not_allowed() {
        assert!(!ModulePhase::Initialized.can_transition_to(ModulePhase::Registered));
        assert!(!ModulePhase::Active.can_transition_to(ModulePhase::Starting));
        assert!(!ModulePhase::Stopped.can_transition_to(ModulePhase::Stopping));
    }

    #[test]
    fn skip_transitions_not_allowed() {
        assert!(!ModulePhase::Registered.can_transition_to(ModulePhase::Active));
        assert!(!ModulePhase::Registered.can_transition_to(ModulePhase::Starting));
        assert!(!ModulePhase::Initialized.can_transition_to(ModulePhase::Stopped));
    }

    #[test]
    fn any_non_terminal_can_transition_to_failed() {
        assert!(ModulePhase::Registered.can_transition_to(ModulePhase::Failed));
        assert!(ModulePhase::Initialized.can_transition_to(ModulePhase::Failed));
        assert!(ModulePhase::Starting.can_transition_to(ModulePhase::Failed));
        assert!(ModulePhase::Active.can_transition_to(ModulePhase::Failed));
        assert!(ModulePhase::Stopping.can_transition_to(ModulePhase::Failed));
    }

    #[test]
    fn terminal_cannot_transition_to_failed() {
        // Stopped cannot transition to Failed (different phase)
        assert!(!ModulePhase::Stopped.can_transition_to(ModulePhase::Failed));
        // Failed can transition to itself (same-phase no-op is allowed)
        assert!(ModulePhase::Failed.can_transition_to(ModulePhase::Failed));
    }

    #[test]
    fn same_phase_transition_allowed() {
        assert!(ModulePhase::Registered.can_transition_to(ModulePhase::Registered));
        assert!(ModulePhase::Active.can_transition_to(ModulePhase::Active));
        assert!(ModulePhase::Failed.can_transition_to(ModulePhase::Failed));
    }

    #[test]
    fn as_str_returns_correct_name() {
        assert_eq!(ModulePhase::Registered.as_str(), "registered");
        assert_eq!(ModulePhase::Active.as_str(), "active");
        assert_eq!(ModulePhase::Failed.as_str(), "failed");
    }

    #[test]
    fn raw_returns_discriminant_value() {
        assert_eq!(ModulePhase::Registered.raw(), 0);
        assert_eq!(ModulePhase::Initialized.raw(), 1);
        assert_eq!(ModulePhase::Starting.raw(), 2);
        assert_eq!(ModulePhase::Active.raw(), 3);
        assert_eq!(ModulePhase::Stopping.raw(), 4);
        assert_eq!(ModulePhase::Stopped.raw(), 5);
        assert_eq!(ModulePhase::Failed.raw(), 6);
    }

    #[test]
    fn phase_can_be_hashed() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(ModulePhase::Active);
        set.insert(ModulePhase::Starting);
        assert_eq!(set.len(), 2);
        set.insert(ModulePhase::Active); // duplicate
        assert_eq!(set.len(), 2);
    }
}
