#!/usr/bin/env bash
#
# bench.sh — Criterion baseline runner & regression comparator for vireon.
#
# Wraps `cargo bench` with named baselines so perf changes can be A/B'd
# across the 196 existing Criterion benches without relying on a shared
# `main` baseline (which Criterion overwrites by default).
#
# Usage:
#   scripts/bench.sh save    [BASELINE] [CRATE...]   Run benches, save as baseline
#   scripts/bench.sh compare [BASELINE] [CRATE...]   Run benches, compare vs baseline
#   scripts/bench.sh list                            List saved baselines
#   scripts/bench.sh diff   BASELINE_A BASELINE_B    Compare two saved baselines (no run)
#   scripts/bench.sh help                           Show this help
#
# BASELINE defaults to "current". CRATE args are forwarded to `cargo bench`
# as `-p <crate>` selectors (omit to run the whole workspace).
#
# Examples:
#   scripts/bench.sh save main                       # snapshot current branch
#   scripts/bench.sh compare main -p pubsub-engine   # compare local vs main
#   scripts/bench.sh diff main feature-x             # two saved baselines
#   scripts/bench.sh save before-g13                 # before a refactor
#   # ...make changes...
#   scripts/bench.sh compare before-g13              # see regression/improvement
#
# Caveats:
#   - Bench numbers are only meaningful on quiet, dedicated hardware.
#     GitHub Actions runners are noisy — CI baselines are informational.
#   - Baselines live under target/criterion/ (gitignored). `cargo clean`
#     wipes them; re-`save` to restore.
#   - Comparing across CPU models / kernel versions is meaningless; each
#     baseline records host metadata in meta.json to make this visible.

set -euo pipefail

SCRIPT_NAME="$(basename "${BASH_SOURCE[0]}")"
WORKSPACE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
META_DIR="$WORKSPACE_ROOT/target/criterion-baselines"

die() { echo "error: $*" >&2; exit 1; }

usage() {
  # Print the comment block at the top of this file.
  sed -n '2,/^set -euo/p' "${BASH_SOURCE[0]}" \
    | grep -E '^#(\s|$)' \
    | sed 's/^# \{0,1\}//'
  exit 0
}

# Returns the CPU model string (best-effort, Linux-only).
cpu_model() {
  if [[ -r /proc/cpuinfo ]]; then
    awk -F: '/^model name/ {gsub(/^ +/, "", $2); print $2; exit}' /proc/cpuinfo
  else
    echo "unknown"
  fi
}

# Writes meta.json next to a baseline so a saved bench is traceable to a
# git SHA + host configuration. Lives outside target/criterion/ to survive
# Criterion's own baseline metadata (which doesn't record git state).
write_meta() {
  local baseline="$1"
  local dir="$META_DIR/$baseline"
  mkdir -p "$dir"
  local sha branch
  sha="$(cd "$WORKSPACE_ROOT" && git rev-parse --short HEAD 2>/dev/null || echo unknown)"
  branch="$(cd "$WORKSPACE_ROOT" && git rev-parse --abbrev-ref HEAD 2>/dev/null || echo unknown)"
  cat > "$dir/meta.json" <<EOF
{
  "baseline": "$baseline",
  "git_sha": "$sha",
  "git_branch": "$branch",
  "hostname": "$(hostname 2>/dev/null || echo unknown)",
  "kernel": "$(uname -r 2>/dev/null || echo unknown)",
  "cpu": "$(cpu_model)",
  "timestamp": "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
}
EOF
  echo "[$SCRIPT_NAME] meta written → $dir/meta.json"
}

# Discovers (crate, bench_name) pairs via `cargo metadata`.
# Filters to packages in BENCH_CRATES (or all workspace packages if empty).
# Prints one "crate<TAB>bench" line per pair.
discover_benches() {
  BENCH_CRATES_FILTER="${BENCH_CRATES[*]}" \
    python3 -c '
import json, os, subprocess, sys
out = subprocess.run(
    ["cargo", "metadata", "--no-deps", "--format-version", "1"],
    capture_output=True, text=True, check=True,
)
data = json.loads(out.stdout)
filt = os.environ.get("BENCH_CRATES_FILTER", "").split()
filt_set = set(filt) if filt else None
for pkg in data["packages"]:
    if filt_set is not None and pkg["name"] not in filt_set:
        continue
    for t in pkg["targets"]:
        if t["kind"] == ["bench"]:
            print(f"{pkg[\"name\"]}\t{t[\"name\"]}")
'
}

# Runs each Criterion bench individually via `cargo bench -p X --bench Y`.
# Per-bench isolation matters because:
#   (a) `cargo bench -p X --benches` also runs the lib's default-harness
#       bench mode, which rejects Criterion's `--save-baseline` flag and
#       aborts the whole run on the first target.
#   (b) Workspace benches occasionally fail to compile independently of
#       one another (pre-existing stubs, feature flags). One broken bench
#       must not scrap the entire baseline.
run_benches() {
  local -a baseline_flag=("$@")
  local pairs
  if ! pairs="$(discover_benches)"; then
    die "cargo metadata failed"
  fi
  if [[ -z "$pairs" ]]; then
    if (( ${#BENCH_CRATES[@]} == 0 )); then
      die "no bench targets found in workspace"
    else
      die "no bench targets found in: ${BENCH_CRATES[*]}"
    fi
  fi

  local total; total=$(wc -l <<<"$pairs")
  echo "[$SCRIPT_NAME] $total bench target(s) queued"
  local idx=0 failed=0
  while IFS=$'\t' read -r crate bench; do
    idx=$((idx + 1))
    echo "[$SCRIPT_NAME] [$idx/$total] cargo bench -p $crate --bench $bench"
    if ! (cd "$WORKSPACE_ROOT" \
          && cargo bench -p "$crate" --bench "$bench" -- "${baseline_flag[@]}"); then
      echo "[$SCRIPT_NAME]   ⚠ bench failed (continuing)" >&2
      failed=$((failed + 1))
    fi
  done <<< "$pairs"
  if (( failed > 0 )); then
    echo "[$SCRIPT_NAME] done with $failed/$total failures" >&2
  fi
}

cmd_save() {
  local baseline="${1:-current}"; shift || true
  BENCH_CRATES=("$@")
  echo "[$SCRIPT_NAME] saving baseline '$baseline' (${#BENCH_CRATES[@]} crate filter(s))"
  write_meta "$baseline"
  run_benches --save-baseline "$baseline"
}

cmd_compare() {
  local baseline="${1:-current}"; shift || true
  if [[ ! -d "$META_DIR/$baseline" ]]; then
    echo "[$SCRIPT_NAME] warning: no meta dir for '$baseline'. Comparison may fail if Criterion has no saved baseline of that name." >&2
  fi
  BENCH_CRATES=("$@")
  echo "[$SCRIPT_NAME] comparing against baseline '$baseline' (${#BENCH_CRATES[@]} crate filter(s))"
  run_benches --baseline "$baseline"
}

cmd_list() {
  if [[ ! -d "$META_DIR" ]]; then
    echo "[$SCRIPT_NAME] no baselines saved yet ($META_DIR absent)"
    return
  fi
  local found=0
  for d in "$META_DIR"/*/; do
    [[ -d "$d" ]] || continue
    found=1
    local name; name="$(basename "$d")"
    local meta="$d/meta.json"
    if [[ -r "$meta" ]]; then
      local sha host ts
      sha=$(grep -oE '"git_sha":\s*"[^"]+"' "$meta"     | sed -E 's/.*"([^"]+)"$/\1/')
      host=$(grep -oE '"hostname":\s*"[^"]+"' "$meta"   | sed -E 's/.*"([^"]+)"$/\1/')
      ts=$(grep -oE '"timestamp":\s*"[^"]+"' "$meta"    | sed -E 's/.*"([^"]+)"$/\1/')
      printf '  %-20s  sha=%-10s  host=%-16s  %s\n' "$name" "$sha" "$host" "$ts"
    else
      printf '  %-20s  (no meta.json)\n' "$name"
    fi
  done
  (( found )) || echo "[$SCRIPT_NAME] no baselines saved yet"
}

cmd_diff() {
  local a="${1:-}"; local b="${2:-}"
  [[ -n "$a" && -n "$b" ]] || die "diff needs two baseline names: diff BASELINE_A BASELINE_B"

  # critcmp is the standard tool for comparing two saved Criterion baselines.
  if command -v critcmp >/dev/null 2>&1; then
    echo "[$SCRIPT_NAME] critcmp: '$a' vs '$b'"
    critcmp "$a" "$b"
  else
    cat >&2 <<EOF
[$SCRIPT_NAME] critcmp not installed; cannot diff without re-running benches.

Install it:

    cargo install critcmp

then re-run:

    scripts/bench.sh diff $a $b

Alternatively, run `scripts/bench.sh compare $a` to re-bench against '$a'.
EOF
    exit 2
  fi
}

main() {
  local cmd="${1:-help}"
  shift || true
  case "$cmd" in
    save)         cmd_save "$@";;
    compare)      cmd_compare "$@";;
    list|ls)      cmd_list;;
    diff)         cmd_diff "$@";;
    help|-h|--help) usage;;
    *)            die "unknown command '$cmd'. Try 'scripts/bench.sh help'.";;
  esac
}

main "$@"
