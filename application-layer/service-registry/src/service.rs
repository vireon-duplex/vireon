//! [`ServiceId`], [`ServiceState`], [`ServiceInfo`] — service
//! identification and lifecycle primitives.
//!
//! See `DESIGN.md` Part 2 (service model) and Part 5 (lifecycle).

// ── ServiceId ─────────────────────────────────────────────────────

/// Application-level service identifier. `0` is reserved as nil
/// (unassigned). u64 supports up to ~18 quintillion services,
/// well beyond any single deployment.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ServiceId(u64);

impl ServiceId {
    /// The nil sentinel — "no service".
    pub const NIL: Self = Self(0);

    /// Construct from a raw u64. Returns `NIL` if `id == 0` is
    /// passed; callers should check `is_nil()`.
    #[must_use]
    pub const fn new(id: u64) -> Self {
        Self(id)
    }

    /// The nil sentinel.
    #[must_use]
    pub const fn nil() -> Self {
        Self::NIL
    }

    /// Whether this is the nil sentinel.
    #[must_use]
    pub const fn is_nil(self) -> bool {
        self.0 == 0
    }

    /// The raw u64 value.
    #[must_use]
    pub const fn raw(self) -> u64 {
        self.0
    }
}

impl Default for ServiceId {
    fn default() -> Self {
        Self::NIL
    }
}

impl std::fmt::Display for ServiceId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "service#{}", self.0)
    }
}

// ── ServiceState ───────────────────────────────────────────────────

/// Service lifecycle state.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum ServiceState {
    /// Service is registered but not yet active.
    #[default]
    Registered,
    /// Service is active and accepting requests.
    Active,
    /// Service is draining existing connections (graceful shutdown).
    Draining,
    /// Service is unregistered (terminal state).
    Unregistered,
}

impl ServiceState {
    /// Whether the service is in an active state (can accept requests).
    #[must_use]
    pub const fn is_active(self) -> bool {
        matches!(self, Self::Active)
    }

    /// Whether the service is draining (graceful shutdown in progress).
    #[must_use]
    pub const fn is_draining(self) -> bool {
        matches!(self, Self::Draining)
    }

    /// Whether the service can accept new requests.
    #[must_use]
    pub const fn can_accept_requests(self) -> bool {
        matches!(self, Self::Active)
    }

    /// Human-readable name for metrics labels.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Registered => "registered",
            Self::Active => "active",
            Self::Draining => "draining",
            Self::Unregistered => "unregistered",
        }
    }

    /// Whether a transition from `self` to `target` is legal.
    #[must_use]
    pub const fn can_transition_to(self, target: Self) -> bool {
        use ServiceState as S;
        match (self, target) {
            (S::Registered, S::Active | S::Unregistered) => true,
            (S::Active, S::Draining | S::Unregistered) => true,
            (S::Draining, S::Unregistered) => true,
            (S::Unregistered, _) => false,
            _ => false,
        }
    }
}

// ── ServiceInfo ────────────────────────────────────────────────────

/// Static + dynamic info about a service.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServiceInfo {
    /// Service identifier.
    pub id: ServiceId,
    /// Service name (fixed-size byte array + length prefix).
    pub name: [u8; 64],
    /// Name length (bytes).
    pub name_len: u8,
    /// Current lifecycle state.
    pub state: ServiceState,
    /// Number of endpoints registered.
    pub endpoint_count: u8,
    /// Nanoseconds since epoch when the service was registered.
    pub registered_ns: u64,
}

impl ServiceInfo {
    /// Construct a new service info with minimal state.
    #[must_use]
    pub const fn new(id: ServiceId) -> Self {
        Self {
            id,
            name: [0u8; 64],
            name_len: 0,
            state: ServiceState::Registered,
            endpoint_count: 0,
            registered_ns: 0,
        }
    }

    /// Builder-style: set the service name (truncates to 63 bytes).
    #[must_use]
    pub fn with_name(mut self, name: &str) -> Self {
        let bytes = name.as_bytes();
        let len = bytes.len().min(63) as u8;
        self.name[..len as usize].copy_from_slice(&bytes[..len as usize]);
        self.name_len = len;
        self
    }

    /// Get the service name as a string slice.
    #[must_use]
    pub fn name_str(&self) -> &str {
        // SAFETY: We only write valid UTF-8 to name.
        let slice = &self.name[..self.name_len as usize];
        unsafe { std::str::from_utf8_unchecked(slice) }
    }

    /// Whether the service is available for new requests.
    #[must_use]
    pub const fn is_available(&self) -> bool {
        matches!(self.state, ServiceState::Active) && self.endpoint_count > 0
    }
}

impl Default for ServiceInfo {
    fn default() -> Self {
        Self::new(ServiceId::nil())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn service_id_nil_sentinel() {
        assert!(ServiceId::NIL.is_nil());
        assert!(ServiceId::nil().is_nil());
        assert!(!ServiceId::new(1).is_nil());
        assert_eq!(ServiceId::default(), ServiceId::NIL);
    }

    #[test]
    fn service_id_raw_roundtrips() {
        let id = ServiceId::new(42);
        assert_eq!(id.raw(), 42);
    }

    #[test]
    fn service_id_display() {
        assert_eq!(format!("{}", ServiceId::new(7)), "service#7");
        assert_eq!(format!("{}", ServiceId::NIL), "service#0");
    }

    #[test]
    fn service_id_ordering() {
        assert!(ServiceId::new(1) < ServiceId::new(2));
    }

    #[test]
    fn service_state_transitions() {
        use ServiceState as S;
        assert!(S::Registered.can_transition_to(S::Active));
        assert!(S::Active.can_transition_to(S::Draining));
        assert!(S::Draining.can_transition_to(S::Unregistered));
        assert!(!S::Unregistered.can_transition_to(S::Registered));
        assert!(!S::Active.can_transition_to(S::Registered));
    }

    #[test]
    fn service_state_predicates() {
        assert!(ServiceState::Active.is_active());
        assert!(ServiceState::Active.can_accept_requests());
        assert!(!ServiceState::Draining.is_active());
        assert!(!ServiceState::Draining.can_accept_requests());
        assert!(ServiceState::Draining.is_draining());
        assert!(!ServiceState::Active.is_draining());
    }

    #[test]
    fn service_state_as_str() {
        assert_eq!(ServiceState::Active.as_str(), "active");
        assert_eq!(ServiceState::Draining.as_str(), "draining");
        assert_eq!(ServiceState::Unregistered.as_str(), "unregistered");
    }

    #[test]
    fn service_info_new_defaults() {
        let info = ServiceInfo::new(ServiceId::new(1));
        assert_eq!(info.id, ServiceId::new(1));
        assert_eq!(info.state, ServiceState::Registered);
        assert_eq!(info.endpoint_count, 0);
        assert_eq!(info.name_len, 0);
    }

    #[test]
    fn service_info_with_name() {
        let info = ServiceInfo::new(ServiceId::new(1)).with_name("my-service");
        assert_eq!(info.name_str(), "my-service");
        assert_eq!(info.name_len, 10);
    }

    #[test]
    fn service_info_name_truncates() {
        let long_name = "a".repeat(100);
        let info = ServiceInfo::new(ServiceId::new(1)).with_name(&long_name);
        assert_eq!(info.name_len, 63);
        assert_eq!(info.name_str().len(), 63);
    }

    #[test]
    fn service_info_is_available() {
        let mut info = ServiceInfo::new(ServiceId::new(1));
        assert!(!info.is_available()); // Registered + no endpoints

        info.state = ServiceState::Active;
        assert!(!info.is_available()); // Active but no endpoints

        info.endpoint_count = 1;
        assert!(info.is_available()); // Active + endpoints
    }

    #[test]
    fn service_info_default() {
        let info = ServiceInfo::default();
        assert_eq!(info.id, ServiceId::nil());
        assert_eq!(info.state, ServiceState::Registered);
    }

    #[test]
    fn service_info_const_new() {
        const INFO: ServiceInfo = ServiceInfo::new(ServiceId::new(42));
        assert_eq!(INFO.id, ServiceId::new(42));
    }
}
