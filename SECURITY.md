# Security Policy — Vireon

Security requirements, vulnerability reporting, and threat model. Supplements
[SKILLS.md §13](./SKILLS.md).

## 1. Reporting vulnerabilities

**Do NOT open public issues for security bugs.** Report privately to the
maintainers with:

- description + affected component
- minimal repro steps
- impact (privesc, data leak, DoS)
- proof of concept (if any)
- suggested fix (optional)

Response SLA: acknowledge < 24h · triage < 3 business days · initial assessment
< 7 business days · fix < 30 days (critical: < 7 days) · coordinated disclosure
after fix + 90-day grace period.

## 2. Supported versions

| Version | Supported |
|---|---|
| `0.x` (latest `main`) | yes (active development) |
| `< 0.1.0` | no |

## 3. Required controls

| # | Control |
|---|---|
| 1 | Input validation on all publish/subscribe/control requests |
| 2 | Authentication (TLS client certs / tokens / SASL-style) before produce/consume |
| 3 | Authorization — per-stream ACLs (who may publish / subscribe / create) |
| 4 | Rate limiting & per-connection quotas (message rate, connection caps) |
| 5 | TLS 1.3 in transit; encryption at rest for durable storage |
| 6 | Secrets management — never in Git (Vault / cloud KMS) |
| 7 | Audit log for admin actions, auth events, ACL changes |

## 4. Authentication & authorization

| Item | Choice |
|---|---|
| Transport auth | TLS 1.3 (mTLS for service-to-service) |
| Credentials | Short-lived signed tokens / client certificates |
| Authorization | Per-stream ACLs (publish / subscribe / admin) |
| Never | Plaintext credentials, MD5/SHA1, unauthenticated produce/consume in prod |

## 5. Secrets management

Never commit `.env`, keys, certificates. Production: Vault / cloud Secrets
Manager, rotated automatically. Staging: encrypted K8s Secrets. Local dev:
`.env` (gitignored).

## 6. Dependency security

| Tool | CI behavior |
|---|---|
| `cargo audit` | fail on any CVE |
| `cargo deny` | fail on license/advisory/ban violations |
| Dependabot | weekly PRs; security PRs prioritized |

## 7. Threat model (broker)

| # | Threat | Mitigation |
|---|---|---|
| 1 | **Unauthorized publish/subscribe** | Authn + per-stream ACLs; deny by default. |
| 2 | **Message replay** | Monotonic offsets; idempotent producers; redelivery only within at-least-once semantics. |
| 3 | **Poison / oversized messages** | Max-message-size limits; reject early; quarantine unparseable payloads. |
| 4 | **Unbounded consumer / slow consumer** | Bounded channels + backpressure; per-connection quotas; drop/reject policy under load. |
| 5 | **Resource exhaustion / DoS** | Connection caps, rate limiting, circuit breakers on storage; flow control credits. |
| 6 | **Credential / token theft** | Short TTL tokens; rotation; revocation; mTLS pinning. |
| 7 | **Data leak at rest** | Encryption at rest; least-privilege access; retention/expiry enforcement. |

## 8. Incident response

Follow [OPERATIONS.md](./OPERATIONS.md) §6:
`Detect → Contain (rotate secrets, revoke tokens) → Eradicate → Recover → Postmortem` (blameless, < 5 business days).
