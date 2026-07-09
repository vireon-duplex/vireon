# Auth Framework

Authentication and authorization framework for the Vireon QUIC runtime.

## Overview

The Auth Framework provides:

- **Authentication**: Pluggable credential verification
- **Authorization**: RBAC/ABAC support
- **Token Management**: Issue, validate, and revoke tokens
- **Role Management**: Define and assign roles
- **Permission Checking**: Fine-grained access control

## Quick Start

```rust
use auth_framework::{
    AuthenticationPipeline, AuthorizationEngine,
    Credential, CredentialType, Permission, PrincipalId
};
use auth_framework::config::Preset;

// Create auth pipeline
let cfg = Preset::Standard.to_config();
let mut auth = AuthenticationPipeline::new(cfg)?;

// Authenticate
let cred = Credential::new(CredentialType::Password, 123, 0);
let token_hash = auth.authenticate(PrincipalId::new(1), cred)?;

// Authorize
let mut authz = AuthorizationEngine::new(cfg);
let perm = Permission::new(1, 2);
authz.authorize(PrincipalId::new(1), perm)?;
```

## Architecture

```
Auth Request → [Auth Framework] → Auth Decision
                  ↓
           ├─ AuthenticationPipeline (verify credentials)
           ├─ AuthorizationEngine (check permissions)
           ├─ TokenManager (issue/validate/revoke)
           ├─ RoleStore (role definitions)
           └─ PermissionStore (access control rules)
```

## Key Concepts

### Principals

Principals are entities that can be authenticated:

- `User`: Human users
- `Service`: Machine services
- `Agent`: Automated agents
- `Anonymous`: Unauthenticated

### Credentials

Supported credential types:

- **Password**: Traditional password authentication
- **Token**: Bearer token authentication
- **Certificate**: X.509 client certificates
- **ApiKey**: API key authentication
- **OAuth**: OAuth2 flows

### Permissions

Permissions are resource-action pairs:

```rust
let perm = Permission::new(resource_hash, action_hash);
```

### Roles

Roles group permissions:

```rust
let mut role = Role::new(1, 100);  // id, name_hash
role.add_permission(perm);
role.add_inherited(2);  // inherit from role 2
```

## Configuration

### Presets

Pre-configured settings:

```rust
let cfg = Preset::Permissive.to_config();    // High limits
let cfg = Preset::Standard.to_config();      // Balanced
let cfg = Preset::Strict.to_config();        // RBAC + ABAC
let cfg = Preset::HighSecurity.to_config();  // Maximum security
```

### Custom Configuration

```rust
use auth_framework::AuthFrameworkBuilder;

let cfg = AuthFrameworkBuilder::new()
    .token_ttl_ms(7_200_000)
    .max_auth_attempts(5)
    .rate_limit_per_minute(20)
    .enable_rbac(true)
    .enable_abac(true)
    .build()?;
```

## Authentication

### Credential Verification

```rust
let cred = Credential::new(CredentialType::Password, password_hash, now_ns);
match auth.authenticate(principal_id, cred) {
    Ok(token_hash) => println!("Authenticated: {}", token_hash),
    Err(AuthError::AuthenticationFailed(msg)) => println!("Failed: {}", msg),
    Err(AuthError::RateLimited) => println!("Too many attempts"),
    Err(e) => println!("Error: {}", e),
}
```

## Authorization

### Permission Checking

```rust
let perm = Permission::new(resource_hash, action_hash);
match authz.authorize(principal_id, perm) {
    Ok(()) => println!("Permission granted"),
    Err(AuthError::PermissionDenied(msg)) => println!("Denied: {}", msg),
    Err(e) => println!("Error: {}", e),
}
```

### Role Checking

```rust
match authz.check_role(principal_id, role_id) {
    Ok(has_role) => println!("Has role: {}", has_role),
    Err(e) => println!("Error: {}", e),
}
```

## Token Management

### Token Lifecycle

```rust
// Issue
let token_hash = token_manager.issue(principal_id)?;

// Validate
match token_manager.validate(token_hash) {
    Ok(true) => println!("Token valid"),
    Ok(false) => println!("Token invalid"),
    Err(e) => println!("Error: {}", e),
}

// Revoke
token_manager.revoke(token_hash)?;
```

## Observability

### Statistics

```rust
let stats = auth.stats().snapshot();
println!("Auth attempts: {}", stats.auth_attempts_total);
println!("Success rate: {}%", 100 - stats.auth_failure_rate_bps() as u64 / 100);
```

### Health Reporting

```rust
let report = auth.health();
match report.state {
    HealthState::Healthy => println!("All systems nominal"),
    HealthState::Degraded => println!("Investigate during business hours"),
    HealthState::Critical => println!("Page on-call immediately"),
}
```

## Error Handling

All operations return `Result<T, AuthError>`:

```rust
pub enum AuthError {
    AuthenticationFailed(&'static str),
    InvalidToken(&'static str),
    TokenExpired,
    PermissionDenied(&'static str),
    UnknownPrincipal(PrincipalId),
    RateLimited,
    InvalidCredential(&'static str),
    SessionRevoked,
    ShuttingDown,
}
```

Error classification predicates:

```rust
error.is_fatal()      // Abort, don't retry
error.is_retryable()  // Back off and retry
error.is_authz()      // Authorization failure
```

## Performance

### Targets

- Authentication: < 10ms p95 latency
- Authorization: < 1ms p95 latency
- Token validation: < 1ms p95 latency
- Throughput: 10K auth requests/second

## Limitations

- Permission sets limited to 16 permissions
- Role inheritance limited to 4 roles
- Token manager skeleton in Phase 0
- No credential rotation in Phase 0

## Dependencies

- **configuration-system**: Configuration management
- **session-manager**: Session identification

## License

Licensed under the same terms as the Vireon project.

## See Also

- [DESIGN.md](DESIGN.md) - Detailed design documentation
- [Session Manager](../../session-layer/session-manager/) - Session orchestration
- [Service Registry](../service-registry/) - Service discovery
