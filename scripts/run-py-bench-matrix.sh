#!/usr/bin/env bash
# Python SDK bench × vireon.conf matrix — fresh server per mode.
#
# Mirrors the Rust bench-harness matrix (run-bench-matrix.sh) but
# drives scripts/python-scenarios/bench.py instead of the `loadtest`
# binary.
#
# Stream mode uses a batch-and-drain pattern that avoids Python event
# loop contention. This allows publish throughput to exceed 1 GiB/s.
# Recv throughput is limited by the PyO3 FFI bridge (~15-20K recv/s).
#
# Modes exercised per variant:
#   stream-1k    — stream mode, 1KB payloads (message-rate + bandwidth)
#   stream-16k   — stream mode, 16KB payloads (bandwidth)
#   stream-64k   — stream mode, 64KB payloads (max bandwidth)
#   broadcast    — 1 pub + 3 subs, fan-out delivery
#   group        — 1 pub + 3 group members, round-robin delivery
#
# PASS criteria: exit code 0 AND (for broadcast/group) duplicates == 0.
set -u

ROOT=/home/pinyoo/Rust_Projects/vireon
SRV=$ROOT/target/x86_64-unknown-linux-gnu/release/quic-server
BENCH=$ROOT/scripts/python-scenarios/bench.py
VENV=$ROOT/bindings/python/.venv/bin/activate
CONF_DIR=/tmp/vireon-conf-test
RUN_ID=$$
BENCH_DUR=5    # seconds per bench run
BENCH_SUBS=3   # subscribers/members for broadcast/group

cd "$ROOT" || exit 1

# Kill any lingering server processes to avoid ghost-socket interference
pkill -f "quic-server" 2>/dev/null
sleep 3

# Ensure the vireon Python module is built and installed in the venv
if [ ! -f "$VENV" ]; then
    echo "ERROR: venv not found at $VENV — run maturin develop first" >&2
    exit 1
fi
# shellcheck disable=SC1090
source "$VENV"
python3 -c "import vireon" 2>/dev/null || {
    echo "ERROR: vireon module not importable — run 'maturin develop' in bindings/python" >&2
    exit 1
}

# Test cases: each is "mode:payload" except broadcast/group which use 1KB
STREAM_TESTS=("stream:1024" "stream:16384" "stream:65536")
FANOUT_TESTS=("broadcast:1024" "group:1024")

RESULTS=()

free_port() {
    python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()'
}

free_port_clean() {
    for _ in $(seq 1 50); do
        local p
        p=$(free_port)
        if ! ss -uln 2>/dev/null | grep -q ":$p "; then
            echo "$p"
            return 0
        fi
    done
    echo "ERROR: no clean port found after 50 tries" >&2
    return 1
}

wait_ready() {
    local port=$1
    for _ in $(seq 1 75); do
        if ss -uln 2>/dev/null | grep -q ":$port "; then
            return 0
        fi
        sleep 0.2
    done
    return 1
}

start_server() {
    local conf=$1 port=$2
    $SRV --config "$conf" --port "$port" > "$CONF_DIR/srv-$RUN_ID-$port.log" 2>&1 &
    local pid=$!
    if ! wait_ready "$port"; then
        echo "FAIL: server didn't bind $port" >&2
        return 1
    fi
    echo "$pid"
}

stop_pid() {
    local pid=$1
    kill -INT "$pid" 2>/dev/null
    for _ in $(seq 1 50); do
        kill -0 "$pid" 2>/dev/null || return 0
        sleep 0.1
    done
    kill -9 "$pid" 2>/dev/null
}

# extract_metric LOG TEST_ID
# Parses bench.py output. Returns "pub_rate | recv_rate | delivery info"
extract_metric() {
    local log=$1 test_id=$2
    local mode payload
    IFS=':' read -r mode payload <<< "$test_id"

    local pub_rate recv_rate
    pub_rate=$(grep -oP '^publish throughput:\s+\K[0-9.]+ (GiB/s|MiB/s)' "$log" 2>/dev/null | head -1)
    recv_rate=$(grep -oP '^recv throughput:\s+\K[0-9.]+ (GiB/s|MiB/s)' "$log" 2>/dev/null | head -1)
    local del_rate
    del_rate=$(grep -oP '^delivery throughput:\s+\K[0-9.]+ (GiB/s|MiB/s)' "$log" 2>/dev/null | head -1)

    case "$mode" in
        stream)
            local pub recv
            pub=$(grep -oP '^published:\s+\K[0-9]+' "$log" 2>/dev/null | head -1)
            recv=$(grep -oP 'received:\s+\K[0-9]+' "$log" 2>/dev/null | head -1)
            echo "pub ${pub_rate:-?} | recv ${recv_rate:-?} | pub=${pub:-?} frames recv=${recv:-?} frames"
            ;;
        broadcast)
            local fan dup
            fan=$(grep -oP 'fanout multiplier\s+\K[0-9.]+x' "$log" 2>/dev/null | head -1)
            dup=$(grep -oP '^  duplicates\s+\K[0-9]+' "$log" 2>/dev/null | head -1)
            echo "pub ${pub_rate:-?} | delivery ${del_rate:-?} | fanout ${fan:-?} | dup=${dup:-0}"
            ;;
        group)
            local dup bal
            dup=$(grep -oP '^  duplicates\s+\K[0-9]+' "$log" 2>/dev/null | head -1)
            bal=$(grep 'balance:' "$log" 2>/dev/null | tail -1 | sed 's/.*balance: //')
            echo "pub ${pub_rate:-?} | delivery ${del_rate:-?} | dup=${dup:-0} | bal ${bal:-?}"
            ;;
    esac
}

# run_test VARIANT TEST_ID ADDR SUBS DUR
run_test() {
    local variant=$1 test_id=$2 addr=$3 subs=$4 dur=$5
    local mode payload
    IFS=':' read -r mode payload <<< "$test_id"
    local label="${mode}-${payload}"
    local log="$CONF_DIR/py-bench-${RUN_ID}-${variant}-${label}.log"
    local start=$(date +%s.%N)
    RUST_LOG=error timeout $((dur + 20)) \
        python3 "$BENCH" --addr "$addr" --mode "$mode" --subs "$subs" \
            --payload "$payload" --duration "$dur" \
        > "$log" 2>&1
    local rc=$?
    local end=$(date +%s.%N)
    local elapsed=$(awk "BEGIN{printf \"%.1f\", $end-$start}")
    local metric=$(extract_metric "$log" "$test_id")

    if [ $rc -ne 0 ]; then
        echo "FAIL(rc=$rc) ${elapsed}s | ${metric}"
        return
    fi
    if [[ "$mode" == "broadcast" || "$mode" == "group" ]]; then
        local dups=$(grep -oP '^  duplicates\s+\K[0-9]+' "$log" 2>/dev/null | head -1)
        dups=${dups:-0}
        if [ "$dups" -ne 0 ]; then
            echo "FAIL(dup=$dups) ${elapsed}s | ${metric}"
            return
        fi
    fi
    echo "PASS ${elapsed}s | ${metric}"
}

subs_for_mode() {
    case "$1" in
        stream:*) echo 1;;
        broadcast:*|group:*) echo "$BENCH_SUBS";;
    esac
}

# ── Per-test runner helpers (fresh server each time) ─────────────

run_single_test() {
    local variant=$1 conf=$2 test_id=$3
    local subs; subs=$(subs_for_mode "$test_id")
    local mode payload
    IFS=':' read -r mode payload <<< "$test_id"
    local label="${mode}-${payload}"
    local port; port=$(free_port_clean)
    local pid; pid=$(start_server "$conf" "$port")
    if [ -z "$pid" ]; then
        printf "  %-18s subs=%-2d  %s\n" "$label" "$subs" "FAIL(server)"
        RESULTS+=("$variant|$label|FAIL(server)")
        return
    fi
    local res; res=$(run_test "$variant" "$test_id" "127.0.0.1:$port" "$subs" "$BENCH_DUR")
    printf "  %-18s subs=%-2d  %s\n" "$label" "$subs" "$res"
    RESULTS+=("$variant|$label|$res")
    stop_pid "$pid"
    sleep 1
}

write_cluster_conf() {
    local file=$1 mode=$2 workers=$3 peers=$4
    cat > "$file" <<EOF
bind = "127.0.0.1"
cert = "$CONF_DIR/cert.pem"
key = "$CONF_DIR/key.pem"
mode = "$mode"
workers = $workers
skip_crc = true
log_format = "text"
metrics_port = 0
[wal_sync]
mode = "never"
[cluster]
node_id = 0
peers = "$peers"
replication_factor = 2
EOF
}

run_cluster_test() {
    local variant=$1 mode_v=$2 workers=$3 test_id=$4
    local subs; subs=$(subs_for_mode "$test_id")
    local mode payload
    IFS=':' read -r mode payload <<< "$test_id"
    local label="${mode}-${payload}"
    local Q1 Q2 Q3 C1 C2 C3
    Q1=$(free_port_clean); Q2=$(free_port_clean); Q3=$(free_port_clean)
    C1=$(free_port_clean); C2=$(free_port_clean); C3=$(free_port_clean)
    local PEERS="1=127.0.0.1:$C1,2=127.0.0.1:$C2,3=127.0.0.1:$C3"
    local conf="$CONF_DIR/py-bench-cluster-$RUN_ID-$variant-$label.conf"
    write_cluster_conf "$conf" "$mode_v" "$workers" "$PEERS"

    local CPIDS=()
    for N in 1 2 3; do
        case $N in 1) PORT=$Q1;; 2) PORT=$Q2;; 3) PORT=$Q3;; esac
        $SRV --config "$conf" --cluster-node-id "$N" --port "$PORT" \
            > "$CONF_DIR/py-bench-cl-$RUN_ID-$variant-$label-$N.log" 2>&1 &
        CPIDS+=($!)
    done
    local READY=0
    for PORT in $Q1 $Q2 $Q3; do
        wait_ready "$PORT" && READY=$((READY+1))
    done
    if [ "$READY" -ne 3 ]; then
        printf "  %-18s subs=%-2d  %s\n" "$label" "$subs" "FAIL(cluster $READY/3)"
        RESULTS+=("$variant|$label|FAIL(cluster)")
        for p in "${CPIDS[@]}"; do stop_pid "$p"; done
        return
    fi
    sleep 1
    local res; res=$(run_test "$variant" "$test_id" "127.0.0.1:$Q1" "$subs" "$BENCH_DUR")
    printf "  %-18s subs=%-2d  %s\n" "$label" "$subs" "$res"
    RESULTS+=("$variant|$label|$res")
    for p in "${CPIDS[@]}"; do stop_pid "$p"; done
    sleep 1
}

ALL_TESTS=("${STREAM_TESTS[@]}" "${FANOUT_TESTS[@]}")

# ── Main ─────────────────────────────────────────────────────────

echo "═══════════════════════════════════════════════════════════════"
echo "  vireon-python bench × vireon.conf matrix (run_id=$RUN_ID)"
echo "  Fresh server per test — no zombie-connection carryover"
echo "  Duration: ${BENCH_DUR}s/test  Subscribers: ${BENCH_SUBS}"
echo "  Tests: stream(1K/16K/64K) + broadcast(1K) + group(1K)"
echo "═══════════════════════════════════════════════════════════════"

echo
echo "▶ [1/4] single.conf  (mode=single, workers=1)"
for t in "${ALL_TESTS[@]}"; do
    run_single_test "single" "$CONF_DIR/single.conf" "$t"
done

echo
echo "▶ [2/4] multi.conf   (mode=multi, workers=4)"
for t in "${ALL_TESTS[@]}"; do
    run_single_test "multi" "$CONF_DIR/multi.conf" "$t"
done

echo
echo "▶ [3/4] single-cluster.conf  (single + 3-node cluster, RF=2)"
for t in "${ALL_TESTS[@]}"; do
    run_cluster_test "sc" "single" 1 "$t"
done

echo
echo "▶ [4/4] multi-cluster.conf   (multi 2w + 3-node cluster, RF=2)"
for t in "${ALL_TESTS[@]}"; do
    run_cluster_test "mc" "multi" 2 "$t"
done

# ── Summary ─────────────────────────────────────────────────────
echo
echo "═══════════════════════════════════════════════════════════════"
echo "  Summary"
echo "═══════════════════════════════════════════════════════════════"
pass=0; fail=0; skip=0
for r in "${RESULTS[@]}"; do
    if [[ "$r" == *"PASS"* ]]; then
        pass=$((pass+1))
    elif [[ "$r" == *"SKIP"* ]]; then
        skip=$((skip+1))
    else
        fail=$((fail+1))
        echo "  ✗ $r"
    fi
done
echo
echo "  Passed: $pass   Skipped: $skip   Failed: $fail"
echo "═══════════════════════════════════════════════════════════════"
