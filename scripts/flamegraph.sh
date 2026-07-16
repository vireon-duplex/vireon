#!/usr/bin/env bash
# =============================================================================
# flamegraph.sh — Profile the vireon QUIC server under realistic bench load.
# =============================================================================
#
# WHAT THIS DOES
#   Builds quic-server + bench-harness with the `flamegraph` cargo profile
#   (full optimizations + debug symbols, not stripped), starts the server in
#   the background, attaches `perf record` to its PID while bench-harness
#   generates traffic, then renders the folded stacks into an SVG flamegraph.
#
# REQUIREMENTS
#   - Linux with `perf` installed
#   - Userspace perf access:  sudo sysctl kernel.perf_event_paranoid=1
#   - A folded-stack renderer: `inferno-flamegraph` (preferred) or `flamegraph.pl`
#       cargo install inferno      # installs inferno-flamegraph
#
# USAGE
#   ./scripts/flamegraph.sh [--duration 15] [--conns 64] [--mode single] [--port 4433]
#
# AD-HOC SINGLE-PROCESS PROFILING (no load orchestration)
#   For profiling bench-harness itself or any single binary:
#     cargo flamegraph --profile flamegraph -p bench-harness -- -c 64 -d 10
#
# =============================================================================

set -euo pipefail

# ── Defaults ────────────────────────────────────────────────────────────────
DURATION=15
CONNS=64
MODE="single"
PORT=4433
BIND="127.0.0.1"
TARGET="server"
CERT="${CERT:-cert.pem}"
KEY="${KEY:-key.pem}"
OUT="${OUT:-profiles/flamegraph.svg}"
KEEP_DATA=true

# ── Parse args ───────────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case "$1" in
        --duration) DURATION="$2"; shift 2 ;;
        --conns)    CONNS="$2";    shift 2 ;;
        --mode)     MODE="$2";     shift 2 ;;
        --port)     PORT="$2";     shift 2 ;;
        --target)   TARGET="$2";   shift 2 ;;
        --cert)     CERT="$2";     shift 2 ;;
        --key)      KEY="$2";      shift 2 ;;
        --out)      OUT="$2";      shift 2 ;;
        --no-data)  KEEP_DATA=false; shift ;;
        -h|--help)
            sed -n '2,/^# =\+/p' "$0" | sed 's/^# \?//'
            exit 0 ;;
        *) echo "unknown arg: $1" >&2; exit 2 ;;
    esac
done

# ── Resolve repo root (script lives in <root>/scripts/) ─────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$ROOT_DIR"

# ── Color helpers ───────────────────────────────────────────────────────────
if [[ -t 1 ]]; then
    C_B="\033[1;36m"; C_G="\033[1;32m"; C_Y="\033[1;33m"; C_R="\033[1;31m"; C_0="\033[0m"
else
    C_B=""; C_G=""; C_Y=""; C_R=""; C_0=""
fi
info()  { printf "${C_B}==>${C_0} %s\n" "$*"; }
ok()    { printf "${C_G}  ✓${C_0} %s\n" "$*"; }
warn()  { printf "${C_Y}  !${C_0} %s\n" "$*"; }
die()    { printf "${C_R}  ✗${C_0} %s\n" "$*" >&2; exit 1; }

# ── Prerequisite checks ────────────────────────────────────────────────────
info "checking prerequisites"

command -v perf >/dev/null 2>&1 || die "perf not found. Install: sudo apt install linux-tools-common linux-tools-\$(uname -r)"

# perf_event_paranoid: 3+ blocks userspace sampling. 1 or 0 is what we need.
PARANOID="$(cat /proc/sys/kernel/perf_event_paranoid 2>/dev/null || echo 3)"
if [[ "$PARANOID" -gt 1 ]]; then
    warn "perf_event_paranoid=$PARANOID — perf needs userspace access."
    die "run: sudo sysctl kernel.perf_event_paranoid=1  (then re-run this script)"
fi
ok "perf_event_paranoid=$PARANOID (userspace sampling allowed)"

# Renderer: prefer inferno-flamegraph (Rust, no Perl needed), fall back to flamegraph.pl.
RENDERER=""
if command -v inferno-flamegraph >/dev/null 2>&1; then
    RENDERER="inferno"
    ok "renderer: inferno-flamegraph"
elif command -v flamegraph.pl >/dev/null 2>&1; then
    RENDERER="perl"
    ok "renderer: flamegraph.pl"
else
    warn "no folded-stack renderer found."
    die "install one:  cargo install inferno    (preferred)  — or —  install flamegraph.pl (Perl)"
fi

# ── Build ────────────────────────────────────────────────────────────────────
info "building quic-server + bench-harness (profile=flamegraph, this may take a moment)"
cargo build --profile flamegraph -p quic-server -p bench-harness
ok "build complete"

# The binary location depends on whether .cargo/config.toml sets an explicit
# `target` triple (nested: target/<triple>/<profile>/) or not (flat: target/<profile>/).
# Probe both layouts.
BIN_DIR=""
for d in "$ROOT_DIR/target/flamegraph" \
         "$ROOT_DIR/target/x86_64-unknown-linux-gnu/flamegraph"; do
    if [[ -x "$d/quic-server" ]]; then BIN_DIR="$d"; break; fi
done
[[ -n "$BIN_DIR" ]] || die "quic-server binary not found after build"
[[ -x "$BIN_DIR/loadtest" ]] || die "loadtest binary not found at $BIN_DIR/loadtest"
ok "binaries in $BIN_DIR"

# ── Profiling the SERVER under load ─────────────────────────────────────────
# Put perf.data next to the SVG (same dir + basename) so they stay paired.
OUT_ABS="$(cd "$ROOT_DIR" && mkdir -p "$(dirname "$OUT")" && realpath "$OUT")"
PERF_DATA="${OUT_ABS%.svg}.data"
SERVER_LOG="$(mktemp)"
SERVER_PID=""

cleanup() {
    # Kill server if still running.
    if [[ -n "$SERVER_PID" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
    rm -f "$SERVER_LOG"
    info "cleanup done"
}
trap cleanup EXIT

info "starting quic-server on ${BIND}:${PORT} (mode=${MODE}, echo=on)"
RUST_LOG="warn" "$BIN_DIR/quic-server" \
    --cert "$CERT" --key "$KEY" \
    --port "$PORT" --bind "$BIND" \
    --mode "$MODE" --echo \
    >"$SERVER_LOG" 2>&1 &
SERVER_PID=$!

# Wait for the server to bind. Poll the port for up to 10s.
info "waiting for server to bind…"
for _ in $(seq 1 100); do
    if kill -0 "$SERVER_PID" 2>/dev/null; then
        # Server process alive — check if port is listening.
        if ss -lun 2>/dev/null | grep -q ":${PORT} " ; then
            ok "server listening (pid=$SERVER_PID)"
            break
        fi
    else
        die "server exited early. Log:"$'\n'"$(cat "$SERVER_LOG")"
    fi
    sleep 0.1
done
if ! kill -0 "$SERVER_PID" 2>/dev/null; then
    die "server failed to start. Log:"$'\n'"$(cat "$SERVER_LOG")"
fi

# ── Record + apply load concurrently ────────────────────────────────────────
info "recording ${DURATION}s under load (${CONNS} conns)…"
# perf attaches to the server PID; `-- sleep N` bounds the recording window.
perf record -F 997 --call-graph dwarf -p "$SERVER_PID" -o "$PERF_DATA" -- sleep "$DURATION" &
PERF_PID=$!

# Give perf a moment to arm, then fire the load generator.
sleep 1
"$BIN_DIR/loadtest" --target "${BIND}:${PORT}" --conns "$CONNS" --duration "$DURATION" || \
    warn "bench-harness reported errors (profile may still be usable)"

# Wait for perf to finish sampling.
wait "$PERF_PID" || true
ok "perf recording complete"

# Stop the server cleanly.
kill "$SERVER_PID" 2>/dev/null || true
wait "$SERVER_PID" 2>/dev/null || true
SERVER_PID=""

# ── Render SVG ───────────────────────────────────────────────────────────────
info "generating ${OUT_ABS}"
case "$RENDERER" in
    inferno)
        perf script -i "$PERF_DATA" | inferno-flamegraph --title "vireon quic-server" > "$OUT_ABS"
        ;;
    perl)
        perf script -i "$PERF_DATA" | flamegraph.pl --title "vireon quic-server" > "$OUT_ABS"
        ;;
esac

if [[ "$KEEP_DATA" == "true" ]]; then
    ok "outputs:"
    printf "    SVG:      ${C_B}%s${C_0}\n" "$OUT_ABS"
    printf "    raw data: ${C_B}%s${C_0}\n" "$PERF_DATA"
    printf "\n  interactive TUI:  ${C_B}perf report -i %s${C_0}\n" "$PERF_DATA"
    printf "  text dump:        ${C_B}perf report -i %s --stdio | less${C_0}\n\n" "$PERF_DATA"
else
    rm -f "$PERF_DATA"
    ok "flamegraph written to ${OUT_ABS}"
    printf "\n  open with:  ${C_B}%s${C_0}\n\n" "$OUT_ABS"
fi
