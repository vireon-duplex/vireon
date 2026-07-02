# Deployment — Vireon

Build, release, deploy, and rollback procedures. No exceptions for production.

## 1. Environments

| Env | Purpose | Branch |
|---|---|---|
| local | Dev machine | feature branches |
| dev | CI testing | `develop` |
| staging | Pre-prod validation | release candidates |
| production | Live | `main` tags |

## 2. Build (Docker)

Multi-stage only; final image distroless (Alpine/Debian-slim as fallback). See
[`Dockerfile`](./Dockerfile).

```dockerfile
FROM rust:1.90-slim AS builder
# ... build the `vireon` binary in release
FROM gcr.io/distroless/cc-debian12 AS runtime
COPY --from=builder /app/target/release/vireon /app/vireon
USER nonroot:nonroot
ENTRYPOINT ["/app/vireon"]
```

Expose the broker's client port(s) once the transport is selected
([ARCHITECTURE.md](./ARCHITECTURE.md) §3). For now the process exposes its
HTTP control/health port.

## 3. CI/CD pipeline

```text
Lint → Test → Audit → Build → Docker → Deploy
```

| Stage | Command | On failure |
|---|---|---|
| Lint | `cargo fmt --check`, `cargo clippy -D warnings` | stop |
| Test | `cargo test --workspace` | stop |
| Audit | `cargo audit`, `cargo deny check` | stop |
| Build | `cargo build --release` | stop |
| Docker | build, tag, sign (cosign), push | stop |
| Deploy | rollout to env | rollback + alert |

## 4. Image tagging

| Tag | Purpose | Immutable |
|---|---|---|
| `<git-sha>` | traceability | yes |
| `v<semver>` | release | yes |
| `latest` | newest stable | no (rolling) |

- SHA and semver tags are **never** overwritten.
- Production images **must** be signed with cosign.
- Only CI-built images are deployable.

## 5. Kubernetes

Each deployment needs: `Deployment`, `Service`, `ConfigMap` (non-secret),
`Secret` (sensitive), `HPA`, `PDB`, `NetworkPolicy`. QUIC/UDP handling is
deferred until transport is chosen.

Minimal pod security: `runAsNonRoot: true`, `runAsUser: 65532`,
`fsGroup: 65532`. Health: `/health/live`, `/health/ready`.

## 6. Migrations

- Forward-only; reviewed before production; tested on staging first.
- Reversible via documented rollback plan (backup + reversal).
- Never hand-edit production schemas.

## 7. Configuration & secrets

| Type | Storage |
|---|---|
| Non-secret | ConfigMap (`config/*.toml`, log level, pool sizes) |
| Secret | Kubernetes Secret / Vault / cloud KMS |

Never bake secrets into images; inject at runtime. All secrets encrypted at rest
and in transit (TLS 1.3).

## 8. Release & rollback

**Release:** tag `vX.Y.Z` → CI builds + signs → staging smoke test → approval →
prod (canary/blue-green/rolling) → monitor 30 min.

**Rollback:** `kubectl rollout undo deployment/vireon`; revert migration only if
its schema change caused the failure; verify health; postmortem if customer
impact.

## 9. Pre-production checklist

- [ ] tests, clippy, audit, deny all pass
- [ ] rollback plan documented
- [ ] dashboards + alerts configured
- [ ] load test completed
- [ ] image signed with cosign
- [ ] health endpoints responding
