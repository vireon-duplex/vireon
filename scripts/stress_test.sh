#!/bin/bash
# stress_test.sh — comprehensive server testing
#
# Tests: port reuse (ghost prevention), all SDK examples, throughput
# consistency. Runs in single + multi-worker modes.
#
# Usage: bash scripts/stress_test.sh
set -euo pipefail

PORT=44331
BIN="./target/x86_64-unknown-linux-gnu/release/quic-server"
CERT="cert.pem"
KEY="key.pem"
SERVER_LOG="/tmp/vireon-stress-server.log"
PASS=0
FAIL=0
SKIP=0

# ── Colors ──────────────────────────────────────────────────────────
G='\033[0;32m'; R='\033[0;31m'; Y='\033[0;33m'; C='\033[0;36m'; N='\033[0m'

ok()   { echo -e "${G}PASS${N} $1"; PASS=$((PASS + 1)); }
fail() { echo -e "${R}FAIL${N} $1"; FAIL=$((FAIL + 1)); }
skip() { echo -e "${Y}SKIP${N} $1"; SKIP=$((SKIP + 1)); }
info() { echo -e "${C}──── $1 ────${N}"; }

# ── Helpers ─────────────────────────────────────────────────────────

count_ghosts() {
    ss -lun 2>/dev/null | { grep ":$PORT" || true; } | wc -l
}

wait_port_bound() {
    for i in $(seq 1 50); do
        if [ "$(ss -lun 2>/dev/null | { grep ":$PORT" || true; } | wc -l)" -gt 0 ]; then
            return 0
        fi
        sleep 0.1
    done
    return 1
}

start_server() {
    local mode="$1" workers="${2:-1}"
    local extra_args=""
    if [ "$mode" = "multi" ]; then
        extra_args="--workers $workers"
    fi
    "$BIN" \
        --cert "$CERT" --key "$KEY" \
        --bind 127.0.0.1 --port "$PORT" \
        --mode "$mode" $extra_args \
        > "$SERVER_LOG" 2>&1 &
    SERVER_PID=$!
    if ! wait_port_bound; then
        echo "ERROR: server did not bind to port $PORT within 5s"
        cat "$SERVER_LOG"
        return 1
    fi
    sleep 1  # extra init time
}

stop_server() {
    if [ -z "${SERVER_PID:-}" ]; then return 0; fi
    kill -INT "$SERVER_PID" 2>/dev/null || return 0
    for i in $(seq 1 50); do
        kill -0 "$SERVER_PID" 2>/dev/null || return 0
        sleep 0.1
    done
    echo "WARN: server did not exit in 5s, SIGKILL"
    kill -9 "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
}

run_example() {
    local name="$1"; shift
    local timeout_secs="${EXAMPLE_TIMEOUT:-120}"
    VIREON_ADDR="127.0.0.1:$PORT" \
        timeout "$timeout_secs" \
        cargo run -p vireon-sdk --release --example "$name" 2>&1
}

# ── Scenario 1: Port Reuse Stress ──────────────────────────────────

scenario1() {
    info "Scenario 1: Port Reuse Stress (ghost prevention)"
    local initial_ghosts
    initial_ghosts=$(count_ghosts)
    echo "Initial ghost count on port $PORT: $initial_ghosts"

    for mode in single multi; do
        for cycle in 1 2 3 4 5; do
            start_server "$mode" 4
            stop_server
            sleep 0.3
            local g
            g=$(count_ghosts)
            if [ "$g" -le "$initial_ghosts" ]; then
                ok "S1 [$mode] cycle $cycle: ghosts=$g (≤ initial $initial_ghosts)"
            else
                fail "S1 [$mode] cycle $cycle: ghosts=$g (> initial $initial_ghosts)"
                echo "  Server log tail:"
                tail -5 "$SERVER_LOG" | sed 's/^/  /'
            fi
        done
    done
}

# ── Scenario 2: SDK Examples ───────────────────────────────────────

scenario2() {
    info "Scenario 2: All SDK Examples (functional correctness)"

    for mode in single multi; do
        info "Mode: $mode"

        # ── s07: HoL congestion ──
        start_server "$mode" 4
        if S07_WORKERS=4 run_example s07_hol_congestion > /tmp/s07-$mode.log 2>&1; then
            ok "S2 [$mode] s07_hol_congestion"
        else
            fail "S2 [$mode] s07_hol_congestion (exit $?)"
            tail -10 /tmp/s07-$mode.log | sed 's/^/  /'
        fi
        stop_server; sleep 0.3

        # ── s09: reconnect ──
        start_server "$mode" 4
        if run_example s09_reconnect > /tmp/s09-$mode.log 2>&1; then
            ok "S2 [$mode] s09_reconnect"
        else
            fail "S2 [$mode] s09_reconnect (exit $?)"
            tail -10 /tmp/s09-$mode.log | sed 's/^/  /'
        fi
        stop_server; sleep 0.3

        # ── s11: ordering ──
        start_server "$mode" 4
        if run_example s11_ordering > /tmp/s11-$mode.log 2>&1; then
            ok "S2 [$mode] s11_ordering"
        else
            fail "S2 [$mode] s11_ordering (exit $?)"
            tail -10 /tmp/s11-$mode.log | sed 's/^/  /'
        fi
        stop_server; sleep 0.3

        # ── s12: full limit ──
        start_server "$mode" 4
        if S12_FRAMES=500 S12_FANOUT=4 run_example s12_full_limit > /tmp/s12-$mode.log 2>&1; then
            ok "S2 [$mode] s12_full_limit"
        else
            fail "S2 [$mode] s12_full_limit (exit $?)"
            tail -10 /tmp/s12-$mode.log | sed 's/^/  /'
        fi
        stop_server; sleep 0.3

        # ── s13: aggregate throughput ──
        start_server "$mode" 4
        if S13_PUBS=2 S13_STREAMS=2 S13_FRAME_SIZE=8192 S13_DURATION=3 \
               run_example s13_aggregate_throughput > /tmp/s13-$mode.log 2>&1; then
            ok "S2 [$mode] s13_aggregate_throughput"
        else
            fail "S2 [$mode] s13_aggregate_throughput (exit $?)"
            tail -10 /tmp/s13-$mode.log | sed 's/^/  /'
        fi
        stop_server; sleep 0.3

        # ── s14: pool multiplex ──
        start_server "$mode" 4
        if S14_POOL=4 S14_FRAMES=5000 run_example s14_pool_multiplex > /tmp/s14-$mode.log 2>&1; then
            ok "S2 [$mode] s14_pool_multiplex"
        else
            fail "S2 [$mode] s14_pool_multiplex (exit $?)"
            tail -10 /tmp/s14-$mode.log | sed 's/^/  /'
        fi
        stop_server; sleep 0.3
    done
}

# ── Scenario 3: Throughput Consistency ─────────────────────────────

scenario3() {
    info "Scenario 3: Throughput Consistency (s13 × 3)"
    local results=()

    for run in 1 2 3; do
        start_server single 1
        local log="/tmp/s13-consistency-$run.log"
        if S13_PUBS=2 S13_STREAMS=4 S13_FRAME_SIZE=8192 S13_DURATION=3 \
               run_example s13_aggregate_throughput > "$log" 2>&1; then
            # Extract MiB/s from output
            local mibs
            mibs=$(grep -oP '[\d.]+\s+MiB/s' "$log" | grep -oP '[\d.]+' | tail -1 || echo "0")
            echo "  Run $run: $mibs MiB/s"
            results+=("$mibs")
            ok "S3 run $run: $mibs MiB/s"
        else
            fail "S3 run $run (exit $?)"
            tail -10 "$log" | sed 's/^/  /'
            results+=("0")
        fi
        stop_server; sleep 0.3
    done

    # Check variance
    if [ "${#results[@]}" -ge 2 ]; then
        local best=0 worst=999999
        for r in "${results[@]}"; do
            local ri=$(echo "$r" | awk '{printf "%.1f", $1}')
            if awk "BEGIN{exit !($ri > $best)}"; then best=$ri; fi
            if awk "BEGIN{exit !($ri < $worst)}"; then worst=$ri; fi
        done
        if awk "BEGIN{exit !($worst < $best * 0.5)}"; then
            fail "S3 variance: best=$best worst=$worst MiB/s (>50% drop)"
        else
            ok "S3 variance acceptable: best=$best worst=$worst MiB/s"
        fi
    fi
}

# ── Main ───────────────────────────────────────────────────────────

echo "============================================================"
echo "  Vireon Stress Test"
echo "  Port:     $PORT"
echo "  Binary:   $BIN"
echo "  Ghosts:   $(count_ghosts) (initial)"
echo "============================================================"
echo

# Verify binary exists
if [ ! -f "$BIN" ]; then
    echo "ERROR: binary not found at $BIN"
    echo "Run: cargo build -p quic-server --release --target x86_64-unknown-linux-gnu"
    exit 1
fi

# Ensure no leftover server on port
if [ "$(count_ghosts)" -gt 0 ]; then
    echo "WARN: port $PORT already has $(count_ghosts) socket(s) — proceeding anyway"
fi

scenario1
echo
scenario2
echo
scenario3

echo
echo "============================================================"
echo "  Results: ${G}$PASS passed${N}, ${R}$FAIL failed${N}, ${Y}$SKIP skipped${N}"
echo "  Final ghost count: $(count_ghosts)"
echo "============================================================"

if [ "$FAIL" -gt 0 ]; then
    exit 1
fi
