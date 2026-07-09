# Auth Framework Design Document

## 1. Overview

The Auth Framework provides authentication, authorization, and token management for the Vireon runtime. It supports multiple credential types, role-based access control (RBAC), attribute-based access control (ABAC), and comprehensive audit logging.

## 2. Architecture

### 2.1 System Model

The auth framework operates as a policy decision point:

```
Auth Request → [Auth Framework] → Auth Decision
                  ↓
           ├─ AuthenticationPipeline (verify credentials)
           ├─ AuthorizationEngine (check permissions)
           ├─ TokenManager (issue/validate/revoke)
           ├─ RoleStore (role definitions)
           └─ PermissionStore (access control rules)
```

### 2.2 Key Properties

- **Pluggable Auth**: Support for multiple credential types
- **Strong Consistency**: Auth decisions are always consistent
- **High Performance**: Sub-millisecond auth decisions
- **Comprehensive Auditing**: Full logging of auth events

## 3. Principal Model

### 3.1 Principal Identification

Principals are identified by a 64-bit `PrincipalId`:

- `0`: Reserved as nil sentinel
- `1-2^56-1`: Valid principal identifiers

```rust
pub struct PrincipalId(u64);
```

### 3.2 Principal Types

```rust
pub enum PrincipalType {
    User,        // Human users
    Service,     // Machine services
    Agent,       // Automated agents
    Anonymous,   // Unauthenticated
}
```

## 4. Credential Model

### 4.1 Credential Types

```rust
pub enum CredentialType {
    Password,     // Traditional password auth
    Token,        // Bearer tokens
    Certificate,  // X.509 client certificates
    ApiKey,       // API key authentication
    OAuth,        // OAuth2 flows
}
```

### 4.2 Credential Validation

Credentials store a hash of the secret data:

```rust
pub struct Credential {
    pub cred_type: CredentialType,
    pub data_hash: u64,    // Hash of credential data
    pub issued_ns: u64,    // Issuance timestamp
}
```

## 5. Permission Model

### 5.1 Permission Structure

Permissions are resource-action pairs:

```rust
pub struct Permission {
    pub resource_hash: u64,  // Hash of resource identifier
    pub action_hash: u64,    // Hash of action identifier
}
```

### 5.2 Permission Sets

Fixed-size sets for efficient storage:

```rust
pub struct PermissionSet {
    permissions: [Permission; 16],
    count: u8,
}
```

## 6. Role Model

### 6.1 Role Definition

```rust
pub struct Role {
    pub id: u16,
    pub name_hash: u64,
    pub permissions: PermissionSet,
    pub inherits: [u16; 4],
    pub inherit_count: u8,
}
```

### 6.2 Role Assignment

```rust
pub struct RoleAssignment {
    pub principal: PrincipalId,
    pub role_id: u16,
    pub assigned_ns: u64,
}
```

## 7. Authentication Pipeline

### 7.1 Authentication Flow

```
Credential → Verification → Principal → Token
```

### 7.2 Failure Handling

- Max attempts: Configurable (default 3)
- Rate limiting: Per-IP and per-principal
- Account lockout: After max failed attempts

## 8. Authorization Engine

### 8.1 RBAC

Role-based access control checks:

1. Load principal's role assignments
2. Collect role permissions (including inherited)
3. Check if requested permission is granted

### 8.2 ABAC

Attribute-based access control (Phase 2+):

- Resource attributes (owner, sensitivity, tags)
- Principal attributes (department, clearance level)
- Environmental attributes (time, location)

## 9. Token Management

### 9.1 Token Lifecycle

```
Issue → Validate → Refresh → Revoke
```

### 9.2 Token Types

- **Session Tokens**: Short-lived (minutes to hours)
- **API Tokens**: Long-lived (days to months)
- **Refresh Tokens**: Used to obtain new session tokens

## 10. Configuration

### 10.1 Presets

```rust
pub enum Preset {
    Permissive,    // High limits, long TTL
    Standard,      // Balanced security
    Strict,        // RBAC + ABAC enabled
    HighSecurity,  // Maximum security
}
```

### 10.2 Configuration Structure

```rust
pub struct AuthConfig {
    pub token_ttl_ms: u64,
    pub max_auth_attempts: u8,
    pub rate_limit_per_minute: u32,
    pub enable_rbac: bool,
    pub enable_abac: bool,
    pub min_password_length: u8,
}
```

## 11. Error Handling

### 11.1 Error Taxonomy

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

### 11.2 Error Classification

- **Fatal**: `InvalidToken`, `InvalidCredential`, `ShuttingDown`
- **Retryable**: `AuthenticationFailed`, `RateLimited`
- **AuthZ**: `PermissionDenied`, `SessionRevoked`

## 12. Statistics and Observability

### 12.1 Key Metrics

```rust
pub struct AuthStats {
    pub auth_attempts_total: AtomicU64,
    pub auth_succeeded: AtomicU64,
    pub auth_failed: AtomicU64,
    pub authz_checks_total: AtomicU64,
    pub authz_allowed: AtomicU64,
    pub authz_denied: AtomicU64,
    pub tokens_issued: AtomicU64,
    pub tokens_validated: AtomicU64,
    pub tokens_revoked: AtomicU64,
    pub rate_limited_count: AtomicU64,
}
```

### 12.2 Derived Metrics

- `auth_failure_rate_bps`: Failed auth attempts as percentage
- `authz_denial_rate_bps`: Denied authz requests as percentage

## 13. Health Monitoring

### 13.1 Health States

- **Healthy**: All systems nominal
- **Degraded**: Elevated failure rates
- **Critical**: Severe failure or denial rates

### 13.2 Thresholds

- Auth failure rate: > 5% degraded, > 20% critical
- AuthZ denial rate: > 10% degraded, > 30% critical

## 14. Security Considerations

### 14.1 Credential Storage

- Never store raw credentials
- Use salted hashes for passwords
- Rotate credentials regularly

### 14.2 Token Security

- Sign tokens with asymmetric keys
- Validate signatures on every use
- Implement token revocation lists

## 15. Performance

### 15.1 Performance Targets

- Authentication: < 10ms p95 latency
- Authorization: < 1ms p95 latency
- Token validation: < 1ms p95 latency
- Throughput: 10K auth requests/second

### 15.2 Optimization Techniques

- Fixed-size data structures
- Lock-free statistics
- Permission caching
- Token validation caching

## 16. Phase 1+ Roadmap

### 16.1 Phase 1 (Next Sprint)

- Real authentication logic
- Role store implementation
- Token issuance and validation
- Statistics integration

### 16.2 Phase 2 (Future)

- ABAC implementation
- Token refresh flow
- Credential rotation
- Audit logging

### 16.3 Phase 3+ (Future)

- Multi-factor authentication
- OAuth2/OpenID Connect
- Certificate-based auth
- Advanced token claims

## 17. Dependencies

### 17.1 Internal Dependencies

- **configuration-system**: Configuration management
- **session-manager**: Session identification

### 17.2 External Dependencies

- **thiserror**: Error derivation
- **tracing**: Structured logging
- **criterion**: Benchmarking (dev-only)

## 18. Compatibility

### 18.1 Rust Version

Minimum: Rust 2024 edition
Target: Stable Rust 1.85+

### 18.2 Platform Support

- **Linux**: Primary target
- **macOS**: Development target
- **Windows**: Community support

## 19. Limitations

### 19.1 Known Limitations

- Permission sets limited to 16 permissions
- Role inheritance limited to 4 roles
- No credential rotation in Phase 0
- No audit logging in Phase 0

### 19.2 Future Enhancements

- Dynamic permission set sizes
- Deeper role inheritance
- Automated credential rotation
- Comprehensive audit trails

## 20. References

### 20.1 Design Inspiration

- OAuth 2.0 Authorization Framework
- X.509 Certificate Authentication
- NIST RBAC Model
- AWS IAM Policy Model

### 20.2 Standards

- RFC 6749 (OAuth 2.0)
- RFC 7519 (JSON Web Token)
- RFC 5280 (X.509 PKI)
- NIST SP 800-162 (RBAC)
