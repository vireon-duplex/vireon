---
name: Bug Report
about: Report a bug to help us improve Vireon
title: "[bug] "
labels: bug
assignees: ''
---

## Describe the bug

A clear and concise description of what the bug is.

## To Reproduce

Steps to reproduce the behavior:

1. Start server with config `...`
2. Run client code `...`
3. See error

## Expected behavior

What you expected to happen.

## Actual behavior

What actually happened (include logs, error messages).

## Environment

- Vireon version: `cargo run -- -V` or git commit hash
- OS: (e.g. Ubuntu 24.04, macOS 15.2)
- Rust version: `rustc --version`
- CPU governor: `cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor`
- Server config (TOML or CLI flags, redact secrets):

## Logs

```
Paste relevant server/client logs here. Use RUST_LOG=debug for verbose output.
```

## Additional context

Add any other context about the problem here.
