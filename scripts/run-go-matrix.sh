#!/usr/bin/env bash
# Go SDK × vireon.conf matrix — fresh server per scenario.
#
# Mirrors run-py-matrix.sh but drives the Go example programs under
# bindings/go/examples/ via `go run`. Each scenario verifies correctness
# (delivery semantics, ordering, consumer-group round-robin).
set -u

ROOT=/home/pinyoo/Rust_Projects/vireon
SRV=$ROOT/target/x86_64-unknown-linux-gnu/release/quic-server
GO_DIR=$ROOT/bindings/go
CONF_DIR=/tmp/vireon-conf-test
RUN_ID=$$

cd "$ROOT" || exit 1

# Kill any lingering server processes to avoid ghost-socket interference.
pkill -f "quic-server" 2>/dev/null
sleep 3

# Locate Go (may be installed in ~/go-sdk for users without system Go).
GO_BIN=${GO_BIN:-}
if [ -z "$GO_BIN" ]; then
    if command -v go >/dev/null 2>&1; then
        GO_BIN=$(command -v go)
    elif [ -x "$HOME/go-sdk/go/bin/go" ]; then
        GO_BIN="$HOME/go-sdk/go/bin/go"
    else
        echo "ERROR: go binary not found (set GO_BIN or install Go)" >&2
        exit 1
    fi
fi

# Pre-build the examples once (matrix reuses the compiled binaries).
( cd "$GO_DIR" && "$GO_BIN" build ./... ) || {
    echo "ERROR: go build failed" >&2
    exit 1
}

SCENARIOS=(quickstart hol_congestion ordering consumer_group pool_multiplex)

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

# go_metric LOG SCENARIO — extract a concise metric line.
go_metric() {
    local log=$1 ex=$2
    case "$ex" in
        quickstart)
            grep -o 'all [0-9]* delivery policies verified' "$log" 2>/dev/null | tail -1
            ;;
        hol_congestion)
            local heavy light total
            heavy=$(grep -oP 'heaviest stream deliver=\K[0-9]+' "$log" 2>/dev/null | tail -1)
            light=$(grep -oP 'lightest=\K[0-9]+' "$log" 2>/dev/null | tail -1)
            total=$(grep -oP 'total=\K[0-9]+' "$log" 2>/dev/null | tail -1)
            echo "heavy=${heavy:-?}msgs light=${light:-?}msgs total=${total:-?}msgs"
            ;;
        ordering)
            local recv gaps dup
            recv=$(grep -oP 'received:\s+\K[0-9]+(?=/)' "$log" 2>/dev/null | tail -1)
            gaps=$(grep -oP 'gaps:\s+\K[0-9]+' "$log" 2>/dev/null | tail -1)
            dup=$(grep -oP 'duplicates:\s+\K[0-9]+' "$log" 2>/dev/null | tail -1)
            echo "recv=${recv:-?}msgs gaps=${gaps:-?} dup=${dup:-?}"
            ;;
        consumer_group)
            local del bal
            del=$(grep -oP 'delivered:\s+\K[0-9]+/[0-9]+' "$log" 2>/dev/null | tail -1)
            bal=$(grep -oP 'balance:\s+\Kg0=[0-9]+ g1=[0-9]+ g2=[0-9]+' "$log" 2>/dev/null | tail -1 | sed 's/=\([0-9]\+\)/=\1msgs/g')
            echo "delivered=${del:-?}msgs bal=${bal:-?}"
            ;;
        pool_multiplex)
            local recv tput
            recv=$(grep -oP 'received \K[0-9]+/[0-9]+' "$log" 2>/dev/null | tail -1)
            tput=$(grep -oP 'throughput:\s+\K[0-9]+ msg/s' "$log" 2>/dev/null | tail -1)
            echo "recv=${recv:-?}msgs | tput ${tput:-?}"
            ;;
    esac
}

# run_scenario VARIANT SCENARIO ADDR
run_scenario() {
    local variant=$1 ex=$2 addr=$3
    local log="$CONF_DIR/go-${RUN_ID}-${variant}-${ex}.log"
    local start; start=$(date +%s.%N)
    ( cd "$GO_DIR" && VIREON_ADDR="$addr" timeout 90 \
        "$GO_BIN" run "./examples/$ex/" ) \
        > "$log" 2>&1
    local rc=$?
    local end; end=$(date +%s.%N)
    local elapsed; elapsed=$(awk "BEGIN{printf \"%.1f\", $end-$start}")
    local metric; metric=$(go_metric "$log" "$ex")
    if [ $rc -eq 0 ]; then
        echo "PASS ${elapsed}s | ${metric}"
    else
        echo "FAIL(rc=$rc) ${elapsed}s | ${metric}"
    fi
}

# run_single_scenario VARIANT CONF SCENARIO
run_single_scenario() {
    local variant=$1 conf=$2 ex=$3
    local port; port=$(free_port_clean)
    local pid; pid=$(start_server "$conf" "$port")
    if [ -z "$pid" ]; then
        printf "  %-30s %s\n" "$ex" "FAIL(server start)"
        RESULTS+=("$variant|$ex|FAIL(server)")
        return
    fi
    local res; res=$(run_scenario "$variant" "$ex" "127.0.0.1:$port")
    printf "  %-30s %s\n" "$ex" "$res"
    RESULTS+=("$variant|$ex|$res")
    stop_pid "$pid"
    sleep 0.5
}

# write_cluster_conf FILE MODE WORKERS PEERS
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

# run_cluster_scenario VARIANT MODE WORKERS SCENARIO
run_cluster_scenario() {
    local variant=$1 mode=$2 workers=$3 ex=$4
    local Q1 Q2 Q3 C1 C2 C3
    Q1=$(free_port_clean); Q2=$(free_port_clean); Q3=$(free_port_clean)
    C1=$(free_port_clean); C2=$(free_port_clean); C3=$(free_port_clean)
    local PEERS="1=127.0.0.1:$C1,2=127.0.0.1:$C2,3=127.0.0.1:$C3"
    local conf="$CONF_DIR/go-cluster-$RUN_ID-$variant-$ex.conf"
    write_cluster_conf "$conf" "$mode" "$workers" "$PEERS"

    local CPIDS=()
    for N in 1 2 3; do
        case $N in 1) PORT=$Q1;; 2) PORT=$Q2;; 3) PORT=$Q3;; esac
        $SRV --config "$conf" --cluster-node-id "$N" --port "$PORT" \
            > "$CONF_DIR/go-cl-$RUN_ID-$variant-$ex-$N.log" 2>&1 &
        CPIDS+=($!)
    done
    local READY=0
    for PORT in $Q1 $Q2 $Q3; do
        wait_ready "$PORT" && READY=$((READY+1))
    done
    if [ "$READY" -ne 3 ]; then
        printf "  %-30s %s\n" "$ex" "FAIL(cluster bind $READY/3)"
        RESULTS+=("$variant|$ex|FAIL(cluster)")
        for p in "${CPIDS[@]}"; do stop_pid "$p"; done
        return
    fi
    sleep 1
    local res; res=$(run_scenario "$variant" "$ex" "127.0.0.1:$Q1")
    printf "  %-30s %s\n" "$ex" "$res"
    RESULTS+=("$variant|$ex|$res")
    for p in "${CPIDS[@]}"; do stop_pid "$p"; done
    sleep 0.5
}

# ── Main ─────────────────────────────────────────────────────────

echo "═══════════════════════════════════════════════════════════════"
echo "  vireon-go SDK × vireon.conf matrix (run_id=$RUN_ID)"
echo "  Fresh server per scenario — no zombie-connection carryover"
echo "═══════════════════════════════════════════════════════════════"

echo
echo "▶ [1/4] single.conf  (mode=single, workers=1)"
for ex in "${SCENARIOS[@]}"; do
    run_single_scenario "single" "$CONF_DIR/single.conf" "$ex"
done

echo
echo "▶ [2/4] multi.conf   (mode=multi, workers=4)"
for ex in "${SCENARIOS[@]}"; do
    run_single_scenario "multi" "$CONF_DIR/multi.conf" "$ex"
done

echo
echo "▶ [3/4] single-cluster.conf  (single + 3-node cluster, RF=2)"
for ex in "${SCENARIOS[@]}"; do
    run_cluster_scenario "sc" "single" 1 "$ex"
done

echo
echo "▶ [4/4] multi-cluster.conf   (multi 2w + 3-node cluster, RF=2)"
for ex in "${SCENARIOS[@]}"; do
    run_cluster_scenario "mc" "multi" 2 "$ex"
done

# ── Summary ─────────────────────────────────────────────────────
echo
echo "═══════════════════════════════════════════════════════════════"
echo "  Summary"
echo "═══════════════════════════════════════════════════════════════"
pass=0; fail=0
for r in "${RESULTS[@]}"; do
    if [[ "$r" == *"PASS"* ]]; then
        pass=$((pass+1))
    else
        fail=$((fail+1))
        echo "  ✗ $r"
    fi
done
echo
echo "  Passed: $pass   Failed: $fail"
echo "═══════════════════════════════════════════════════════════════"
