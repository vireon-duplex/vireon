//! [`SchedulerHookId`], [`HookTrigger`] — typed identifiers and triggers
//! for scheduler hooks.
//!
//! # Design
//!
//! - `SchedulerHookId`: 64-bit identifier with nil sentinel
//! - `HookTrigger`: Enum of trigger types (pre/post tick, submit, complete, idle, wakeup)

use std::fmt;

// ── SchedulerHookId ─────────────────────────────────────────────────

/// A typed scheduler hook identifier.
///
/// 64-bit identifier with NIL sentinel (value 0).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct SchedulerHookId(u64);

impl SchedulerHookId {
    /// The nil hook ID (value 0).
    pub const NIL: Self = Self(0);

    /// Construct a new hook ID from a raw value.
    #[must_use]
    pub const fn new(id: u64) -> Self {
        Self(id)
    }

    /// The nil hook ID.
    #[must_use]
    pub const fn nil() -> Self {
        Self::NIL
    }

    /// Whether this is the nil hook ID.
    #[must_use]
    pub const fn is_nil(&self) -> bool {
        self.raw() == 0
    }

    /// Raw ID value.
    #[must_use]
    pub const fn raw(&self) -> u64 {
        self.0
    }
}

impl fmt::Display for SchedulerHookId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "HookId({})", self.0)
    }
}

// ── HookTrigger ────────────────────────────────────────────────────

/// When a hook should be invoked.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum HookTrigger {
    /// Before the reactor tick starts.
    PreTick = 0,
    /// After the reactor tick completes.
    PostTick = 1,
    /// Before submitting work to the reactor.
    PreSubmit = 2,
    /// After work completes.
    PostComplete = 3,
    /// When the reactor is idle.
    OnIdle = 4,
    /// When the reactor wakes from idle.
    OnWakeup = 5,
}

impl HookTrigger {
    /// Human-readable name for the trigger.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::PreTick => "pre_tick",
            Self::PostTick => "post_tick",
            Self::PreSubmit => "pre_submit",
            Self::PostComplete => "post_complete",
            Self::OnIdle => "on_idle",
            Self::OnWakeup => "on_wakeup",
        }
    }

    /// Whether this is a pre-tick trigger.
    #[must_use]
    pub const fn is_pre_tick(&self) -> bool {
        matches!(self, Self::PreTick)
    }

    /// Whether this is a post-tick trigger.
    #[must_use]
    pub const fn is_post_tick(&self) -> bool {
        matches!(self, Self::PostTick)
    }

    /// The phase of this trigger ("pre" or "post").
    #[must_use]
    pub const fn phase(&self) -> &'static str {
        match self {
            Self::PreTick | Self::PreSubmit => "pre",
            Self::PostTick | Self::PostComplete => "post",
            Self::OnIdle | Self::OnWakeup => "",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hook_id_new() {
        let id = SchedulerHookId::new(42);
        assert_eq!(id.raw(), 42);
        assert!(!id.is_nil());
    }

    #[test]
    fn hook_id_nil() {
        let nil = SchedulerHookId::nil();
        assert_eq!(nil.raw(), 0);
        assert!(nil.is_nil());
    }

    #[test]
    fn hook_id_is_nil() {
        assert!(SchedulerHookId::nil().is_nil());
        assert!(!SchedulerHookId::new(1).is_nil());
    }

    #[test]
    fn hook_id_copy_and_clone() {
        let id1 = SchedulerHookId::new(123);
        let id2 = id1;
        assert_eq!(id1, id2);
    }

    #[test]
    fn hook_id_display() {
        let id = SchedulerHookId::new(42);
        assert_eq!(format!("{}", id), "HookId(42)");
    }

    #[test]
    fn hook_trigger_as_str() {
        assert_eq!(HookTrigger::PreTick.as_str(), "pre_tick");
        assert_eq!(HookTrigger::PostTick.as_str(), "post_tick");
        assert_eq!(HookTrigger::PreSubmit.as_str(), "pre_submit");
        assert_eq!(HookTrigger::PostComplete.as_str(), "post_complete");
        assert_eq!(HookTrigger::OnIdle.as_str(), "on_idle");
        assert_eq!(HookTrigger::OnWakeup.as_str(), "on_wakeup");
    }

    #[test]
    fn hook_trigger_is_pre_tick() {
        assert!(HookTrigger::PreTick.is_pre_tick());
        assert!(!HookTrigger::PostTick.is_pre_tick());
        assert!(!HookTrigger::OnIdle.is_pre_tick());
    }

    #[test]
    fn hook_trigger_is_post_tick() {
        assert!(HookTrigger::PostTick.is_post_tick());
        assert!(!HookTrigger::PreTick.is_post_tick());
        assert!(!HookTrigger::OnIdle.is_post_tick());
    }

    #[test]
    fn hook_trigger_phase() {
        assert_eq!(HookTrigger::PreTick.phase(), "pre");
        assert_eq!(HookTrigger::PostTick.phase(), "post");
        assert_eq!(HookTrigger::PreSubmit.phase(), "pre");
        assert_eq!(HookTrigger::PostComplete.phase(), "post");
        assert_eq!(HookTrigger::OnIdle.phase(), "");
        assert_eq!(HookTrigger::OnWakeup.phase(), "");
    }

    #[test]
    fn hook_trigger_copy_and_clone() {
        let trigger1 = HookTrigger::PreTick;
        let trigger2 = trigger1;
        assert_eq!(trigger1, trigger2);
    }

    #[test]
    fn hook_trigger_hash() {
        // Ensure HookTrigger derives Hash.
        let _ = std::collections::HashSet::from([
            HookTrigger::PreTick,
            HookTrigger::PostTick,
            HookTrigger::OnIdle,
        ]);
    }
}
