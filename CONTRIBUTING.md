# Contributing — Vireon

Process, coding standards, and workflow for all contributors (human and AI).
> Follow [ARCHITECTURE.md](./ARCHITECTURE.md) and [SKILLS.md](./SKILLS.md).

## 1. Prerequisites & quick start

| Tool | Notes |
|---|---|
| **Rust (stable)** | Edition 2024; `rustup component add rustfmt clippy` |
| **Docker** (optional) | For containerized builds / infra |

```bash
git clone https://github.com/pinyoothotaboot/Vireon.git
cd Vireon
cargo run
```

> Transport and storage backends are not yet chosen. When they land, any
> required dev services (e.g. a storage engine) will be added to
> `docker-compose.yml` and documented here.

## 2. Architecture

Clean Architecture with the broker domain at the center. See
[ARCHITECTURE.md](./ARCHITECTURE.md). Keep `domain` free of infra deps;
implement transport/storage behind port traits.

## 3. Branch model

```text
main         # production-ready
feature/*    # new features
fix/*        # bug fixes
hotfix/*     # urgent prod fixes
```

Keep branches short-lived; rebase before opening a PR.

## 4. Quality gates (must pass)

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test
cargo audit        # known CVEs
cargo deny check   # licenses, advisories, bans
```

## 5. Commit convention

Follow [`.gitmessage`](./.gitmessage) exactly. The `why` block is mandatory.

```text
<type>(<scope>): <short summary>

why:     what:     impact:     refs:
```

Types: `feat`, `fix`, `refactor`, `test`, `docs`, `chore`.
Rules: stage by filename (never `git add -A`); never commit secrets; never
`--no-verify`; on hook failure make a **new** commit (no `--amend`).

## 6. Pull requests

- [ ] `cargo fmt --check`, `cargo clippy -D warnings`, `cargo test` pass.
- [ ] `cargo audit` + `cargo deny` pass.
- [ ] No `unwrap`/`expect`/`panic!` outside tests/bootstrap.
- [ ] Docs updated; linked to an issue where applicable.
- [ ] PR title follows `<type>(<scope>): <description>`; squash-merge preferred.

## 7. Code review focus

Correctness, security (input validation, auth, no secrets), architecture
(layer boundaries, thin handlers), error handling, tests, performance (no
blocking on Tokio, bounded channels), readability.

## 8. Reporting issues / security

- **Bugs:** open a GitHub issue with repro steps, expected vs actual, environment.
- **Security:** do **not** open a public issue — see [SECURITY.md](./SECURITY.md).
