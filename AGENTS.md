# AGENTS.md — Rules for AI coding agents

Authoritative operating rules for any AI agent (Claude Code, Copilot, Cursor…)
working in this repo. If `AGENTS.md` conflicts with another instruction,
**`AGENTS.md` wins.**

## 1. Read first

Before editing any file, read (in order; skip silently if missing):

1. [ARCHITECTURE.md](./ARCHITECTURE.md) — design, layers, broker domain model.
2. [SKILLS.md](./SKILLS.md) — engineering standards.
3. This file.
4. [`.gitmessage`](./.gitmessage) — commit template.
5. [CONTRIBUTING.md](./CONTRIBUTING.md) — workflow.

## 2. Code generation rules

| # | Rule |
|---|---|
| 1 | **No panics** in non-test code — no `unwrap()`/`expect()`/`panic!()`; use `?` + typed errors. |
| 2 | **Respect layers** — domain has zero infra deps; application depends on domain only; infrastructure implements ports. |
| 3 | **Transport/storage are ports** (see ARCHITECTURE §3) — never hard-code a backend. |
| 4 | **Never block Tokio** — `tokio::time::sleep`, `spawn_blocking` for CPU work. |
| 5 | **Bounded channels only** in production paths — propagate backpressure. |
| 6 | **Match existing style** — read surrounding code before generating. |
| 7 | **Run quality gates** before considering a code task done: `cargo fmt` + `cargo clippy -D warnings`. |

```rust
// OK
let msg = log.append(stream, &msg).await?;
// WRONG
let msg = log.append(stream, &msg).await.unwrap();
```

## 3. Forbidden actions

| # | Forbidden | Reason |
|---|---|---|
| 1 | `git add -A` / `git add .` | Stages unintended files — add by filename. |
| 2 | Committing secrets (`.env`, keys) | Security violation. |
| 3 | `git commit --no-verify` | Bypasses hooks — never unless explicitly requested. |
| 4 | `git commit --amend` after a hook failure | Fix and make a **new** commit. |
| 5 | `git push` without explicit user request | Agents never push on their own. |
| 6 | Creating files not requested / adding unused deps | Scope creep / build bloat. |
| 7 | Infra logic in `domain/` | Violates Clean Architecture. |
| 8 | Unbounded channels in prod paths | Hides backpressure until OOM. |

## 4. Commit workflow (only when the user explicitly asks)

```text
1. Verify changes   →  re-read edits, drop debug/temp code
2. Quality gates    →  cargo fmt && cargo clippy -D warnings && cargo test
3. Stage selectively→  git add <specific files>
4. Write commit     →  follow .gitmessage exactly (why/what/impact/refs)
5. Verify           →  git status (clean tree)
```

Never commit proactively. The `why` block is mandatory.

## 5. Testing expectations

- Unit-test **all** domain logic (pure, fast, deterministic).
- Minimum **80%** coverage; integration tests exercise storage/transport ports.
- Every production bug gets a regression test before the fix ships.

## 6. Documentation expectations

- Every public item has a `///` doc comment describing **what**, not how.
- Wrap product/CamelCase names in backticks (e.g. `Vireon`) — `clippy::doc_markdown` is denied.

## 7. Scope discipline & communication

- Do only what is asked; no surprise refactors, renames, or comments.
- Note out-of-scope issues in your response — do not fix them silently.
- Be concise: lead with the action, then findings; no trailing summary.
- Use absolute file paths.
