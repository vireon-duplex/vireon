//! [`AuthorizationEngine`] — RBAC authorization with role storage,
//! inheritance resolution, and permission checking.
//!
//! Stores role definitions and per-principal role assignments. When
//! `authorize()` is called, resolves the principal's roles (including
//! inherited roles) and checks if any role grants the requested permission.
//!
//! ```text
//! register_role(admin_role with permission "read:data")
//! assign_role(principal=1, role=admin)
//! authorize(principal=1, permission="read:data")  → Ok(())
//! authorize(principal=2, permission="read:data")  → Err(PermissionDenied)
//! ```

use crate::config::AuthConfig;
use crate::error::AuthError;
use crate::health::HealthReport;
use crate::permission::Permission;
use crate::principal::PrincipalId;
use crate::role::Role;
use crate::stats::AuthStats;
use rustc_hash::FxHashMap;

/// RBAC authorization engine.
///
/// Stores role definitions (`role_id → Role`) and principal role
/// assignments (`PrincipalId → Vec<role_id>`). Permission checks
/// traverse direct and inherited roles.
pub struct AuthorizationEngine {
    #[allow(dead_code)]
    cfg: AuthConfig,
    stats: AuthStats,
    /// Role definitions: role_id → Role.
    roles: FxHashMap<u16, Role>,
    /// Principal → assigned role IDs.
    assignments: FxHashMap<PrincipalId, Vec<u16>>,
}

impl std::fmt::Debug for AuthorizationEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthorizationEngine")
            .field("cfg", &self.cfg)
            .field("registered_roles", &self.roles.len())
            .field("assigned_principals", &self.assignments.len())
            .finish()
    }
}

impl AuthorizationEngine {
    /// Creates a new authorization engine.
    #[must_use]
    pub fn new(cfg: AuthConfig) -> Self {
        Self {
            cfg,
            stats: AuthStats::new(),
            roles: FxHashMap::default(),
            assignments: FxHashMap::default(),
        }
    }

    /// Registers a role definition.
    ///
    /// Re-registering an existing role ID replaces the definition.
    pub fn register_role(&mut self, role: Role) {
        self.roles.insert(role.id, role);
    }

    /// Assigns a role to a principal.
    ///
    /// If the principal already has the role, this is a no-op.
    /// If the role ID is not registered, returns an error.
    ///
    /// # Errors
    /// Returns [`AuthError::PermissionDenied`] if the role is not registered.
    pub fn assign_role(&mut self, principal: PrincipalId, role_id: u16) -> Result<(), AuthError> {
        if !self.roles.contains_key(&role_id) {
            return Err(AuthError::PermissionDenied("unknown role"));
        }
        let roles = self.assignments.entry(principal).or_default();
        if !roles.contains(&role_id) {
            roles.push(role_id);
        }
        Ok(())
    }

    /// Checks if a principal has a specific permission.
    ///
    /// Resolves all roles assigned to the principal (including
    /// inherited roles up to 4 levels deep) and checks if any role
    /// grants the permission.
    ///
    /// # Errors
    /// Returns [`AuthError::PermissionDenied`] if the principal does
    /// not have the permission. Returns [`AuthError::UnknownPrincipal`]
    /// if the principal has no role assignments.
    pub fn authorize(
        &mut self,
        principal: PrincipalId,
        permission: Permission,
    ) -> Result<(), AuthError> {
        let role_ids = match self.assignments.get(&principal) {
            Some(r) if !r.is_empty() => r.clone(),
            _ => {
                self.stats.record_authz_check(false);
                return Err(AuthError::UnknownPrincipal(principal));
            }
        };

        // Collect all permissions from direct and inherited roles.
        let mut visited = rustc_hash::FxHashSet::default();
        for &role_id in &role_ids {
            if self.check_permission_recursive(role_id, permission, &mut visited) {
                self.stats.record_authz_check(true);
                return Ok(());
            }
        }

        self.stats.record_authz_check(false);
        Err(AuthError::PermissionDenied(
            "no matching permission in assigned roles",
        ))
    }

    /// Recursively checks if a role (or its inherited roles) grants
    /// the permission. Visited set prevents cycles.
    fn check_permission_recursive(
        &self,
        role_id: u16,
        permission: Permission,
        visited: &mut rustc_hash::FxHashSet<u16>,
    ) -> bool {
        if !visited.insert(role_id) {
            return false; // Already visited — cycle detected.
        }

        let Some(role) = self.roles.get(&role_id) else {
            return false;
        };

        if role.has_permission(permission) {
            return true;
        }

        // Check inherited roles.
        for i in 0..role.inherit_count as usize {
            if self.check_permission_recursive(role.inherits[i], permission, visited) {
                return true;
            }
        }

        false
    }

    /// Checks if a principal has a specific role (directly or inherited).
    ///
    /// # Errors
    /// Returns [`AuthError::UnknownPrincipal`] if the principal has
    /// no assignments.
    pub fn check_role(&mut self, principal: PrincipalId, role_id: u16) -> Result<bool, AuthError> {
        let Some(role_ids) = self.assignments.get(&principal) else {
            return Err(AuthError::UnknownPrincipal(principal));
        };

        // Direct assignment.
        if role_ids.contains(&role_id) {
            return Ok(true);
        }

        // Check inherited roles.
        let mut visited = rustc_hash::FxHashSet::default();
        for &direct_id in role_ids {
            if self.check_role_recursive(direct_id, role_id, &mut visited) {
                return Ok(true);
            }
        }

        Ok(false)
    }

    fn check_role_recursive(
        &self,
        current: u16,
        target: u16,
        visited: &mut rustc_hash::FxHashSet<u16>,
    ) -> bool {
        if !visited.insert(current) {
            return false;
        }
        let Some(role) = self.roles.get(&current) else {
            return false;
        };
        for i in 0..role.inherit_count as usize {
            let inherited = role.inherits[i];
            if inherited == target {
                return true;
            }
            if self.check_role_recursive(inherited, target, visited) {
                return true;
            }
        }
        false
    }

    /// Returns the number of registered roles.
    #[must_use]
    pub fn role_count(&self) -> usize {
        self.roles.len()
    }

    /// Returns the number of principals with role assignments.
    #[must_use]
    pub fn assigned_principal_count(&self) -> usize {
        self.assignments.len()
    }

    /// Get the current health report.
    #[must_use]
    pub fn health(&self) -> HealthReport {
        HealthReport::derive(&self.stats.snapshot())
    }

    /// Returns a reference to the stats.
    #[must_use]
    pub fn stats(&self) -> &AuthStats {
        &self.stats
    }
}

// ── Tests ─────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Preset;

    fn make_perm(resource: u64, action: u64) -> Permission {
        Permission::new(resource, action)
    }

    #[test]
    fn authorizer_constructs() {
        let authz = AuthorizationEngine::new(Preset::Standard.to_config());
        assert!(authz.health().is_healthy());
    }

    #[test]
    fn register_and_assign_then_authorize() {
        let mut authz = AuthorizationEngine::new(Preset::Standard.to_config());

        let mut role = Role::new(1, 100);
        role.add_permission(make_perm(1, 1)); // resource=1, action=read
        authz.register_role(role);
        authz.assign_role(PrincipalId::new(1), 1).unwrap();

        assert!(
            authz
                .authorize(PrincipalId::new(1), make_perm(1, 1))
                .is_ok()
        );
    }

    #[test]
    fn authorize_denies_missing_permission() {
        let mut authz = AuthorizationEngine::new(Preset::Standard.to_config());

        let mut role = Role::new(1, 100);
        role.add_permission(make_perm(1, 1));
        authz.register_role(role);
        authz.assign_role(PrincipalId::new(1), 1).unwrap();

        assert!(
            authz
                .authorize(PrincipalId::new(1), make_perm(1, 2))
                .is_err()
        );
    }

    #[test]
    fn authorize_unknown_principal_errors() {
        let mut authz = AuthorizationEngine::new(Preset::Standard.to_config());
        let result = authz.authorize(PrincipalId::new(99), make_perm(1, 1));
        assert!(matches!(result, Err(AuthError::UnknownPrincipal(_))));
    }

    #[test]
    fn role_inheritance() {
        let mut authz = AuthorizationEngine::new(Preset::Standard.to_config());

        // Base role has "read" permission.
        let mut base = Role::new(1, 200);
        base.add_permission(make_perm(1, 1)); // read
        authz.register_role(base);

        // Admin role inherits base and adds "write".
        let mut admin = Role::new(2, 300);
        admin.add_permission(make_perm(1, 2)); // write
        admin.add_inherited(1); // inherits base
        authz.register_role(admin);

        // Assign admin to principal.
        authz.assign_role(PrincipalId::new(1), 2).unwrap();

        // Should have both read (inherited) and write (direct).
        assert!(
            authz
                .authorize(PrincipalId::new(1), make_perm(1, 1))
                .is_ok()
        ); // read
        assert!(
            authz
                .authorize(PrincipalId::new(1), make_perm(1, 2))
                .is_ok()
        ); // write
    }

    #[test]
    fn assign_unknown_role_errors() {
        let mut authz = AuthorizationEngine::new(Preset::Standard.to_config());
        assert!(authz.assign_role(PrincipalId::new(1), 999).is_err());
    }

    #[test]
    fn check_role_direct_and_inherited() {
        let mut authz = AuthorizationEngine::new(Preset::Standard.to_config());

        let mut base = Role::new(1, 200);
        base.add_permission(make_perm(1, 1));
        authz.register_role(base);

        let mut admin = Role::new(2, 300);
        admin.add_inherited(1);
        authz.register_role(admin);

        authz.assign_role(PrincipalId::new(1), 2).unwrap();

        assert!(authz.check_role(PrincipalId::new(1), 2).unwrap()); // direct
        assert!(authz.check_role(PrincipalId::new(1), 1).unwrap()); // inherited
        assert!(!authz.check_role(PrincipalId::new(1), 99).unwrap()); // not assigned
    }

    #[test]
    fn multiple_roles_cumulative() {
        let mut authz = AuthorizationEngine::new(Preset::Standard.to_config());

        let mut reader = Role::new(1, 100);
        reader.add_permission(make_perm(1, 1));
        authz.register_role(reader);

        let mut writer = Role::new(2, 200);
        writer.add_permission(make_perm(1, 2));
        authz.register_role(writer);

        authz.assign_role(PrincipalId::new(1), 1).unwrap();
        authz.assign_role(PrincipalId::new(1), 2).unwrap();

        assert!(
            authz
                .authorize(PrincipalId::new(1), make_perm(1, 1))
                .is_ok()
        );
        assert!(
            authz
                .authorize(PrincipalId::new(1), make_perm(1, 2))
                .is_ok()
        );
    }

    #[test]
    fn cycle_detection_in_inheritance() {
        let mut authz = AuthorizationEngine::new(Preset::Standard.to_config());

        // A inherits B, B inherits A — cycle.
        let mut a = Role::new(1, 100);
        a.add_permission(make_perm(1, 1));
        a.add_inherited(2);
        authz.register_role(a);

        let mut b = Role::new(2, 200);
        b.add_inherited(1);
        authz.register_role(b);

        authz.assign_role(PrincipalId::new(1), 1).unwrap();

        // Should not hang, should still find the permission via role A.
        assert!(
            authz
                .authorize(PrincipalId::new(1), make_perm(1, 1))
                .is_ok()
        );
    }
}
