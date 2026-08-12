#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════════
#  Vireon — System Install Script
#
#  Installs quic-server as a systemd service (nginx/redis-style):
#    /usr/local/bin/quic-server          — binary
#    /etc/vireon/vireon.conf             — config (preserved on upgrade)
#    /etc/vireon/certs/                  — TLS certs
#    /var/lib/vireon/wal/                — WAL data
#    /var/log/vireon/                    — logs
#    /etc/systemd/system/vireon.service  — systemd unit
#
#  Usage:
#    sudo ./scripts/install.sh                  # install
#    sudo ./scripts/install.sh --gen-cert       # install + generate dev cert
#    sudo ./scripts/install.sh --uninstall      # remove (prompts for data)
#
#  After install:
#    sudo systemctl enable --now vireon
#    sudo systemctl status vireon
# ═══════════════════════════════════════════════════════════════════
set -euo pipefail

# ── Constants ────────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

BINARY_NAME="quic-server"
INSTALL_BIN="/usr/local/bin/${BINARY_NAME}"
CONFIG_DIR="/etc/vireon"
CONFIG_FILE="${CONFIG_DIR}/vireon.conf"
CERTS_DIR="${CONFIG_DIR}/certs"
DATA_DIR="/var/lib/vireon/wal"
LOG_DIR="/var/log/vireon"
SERVICE_SRC="${REPO_ROOT}/packaging/vireon.service"
SERVICE_DST="/etc/systemd/system/vireon.service"
CONF_SRC="${REPO_ROOT}/packaging/vireon.conf"
USERNAME="vireon"

# ── Colors ───────────────────────────────────────────────────────────
G='\033[0;32m'; Y='\033[0;33m'; R='\033[0;31m'; N='\033[0m'
ok()   { echo -e "${G}✓${N} $*"; }
warn() { echo -e "${Y}⚠${N} $*"; }
err()  { echo -e "${R}✗${N} $*" >&2; }

# ── Flags ────────────────────────────────────────────────────────────
UNINSTALL=false
GEN_CERT=false
for arg in "$@"; do
    case "$arg" in
        --uninstall|-u) UNINSTALL=true ;;
        --gen-cert)     GEN_CERT=true ;;
        --help|-h)
            sed -n '2,20p' "$0"
            exit 0
            ;;
        *) err "Unknown flag: $arg"; exit 1 ;;
    esac
done

# ── Root check ───────────────────────────────────────────────────────
if [[ "$(id -u)" -ne 0 ]]; then
    err "Run as root (use sudo)."
    exit 1
fi

# ═══════════════════════════════════════════════════════════════════
#  UNINSTALL
# ═══════════════════════════════════════════════════════════════════
if $UNINSTALL; then
    echo "Uninstalling vireon..."

    # Stop + disable service
    if systemctl is-active --quiet vireon 2>/dev/null; then
        systemctl stop vireon
        ok "stopped vireon service"
    fi
    if systemctl is-enabled --quiet vireon 2>/dev/null; then
        systemctl disable vireon
        ok "disabled vireon service"
    fi

    # Remove binary + service
    rm -f "$INSTALL_BIN"
    rm -f "$SERVICE_DST"
    systemctl daemon-reload
    ok "removed binary + service file"

    # Prompt for config/data removal
    echo
    if [[ -d "$CONFIG_DIR" ]] || [[ -d "$DATA_DIR" ]]; then
        read -rp "Remove config ($CONFIG_DIR) and data ($DATA_DIR)? [y/N] " confirm
        if [[ "$confirm" =~ ^[Yy]$ ]]; then
            rm -rf "$CONFIG_DIR" "$DATA_DIR" "$LOG_DIR"
            ok "removed config + data + log dirs"
        else
            warn "kept config + data (manual cleanup: rm -rf $CONFIG_DIR $DATA_DIR $LOG_DIR)"
        fi
    fi

    # Remove user (best-effort — don't fail if it has running processes)
    if id "$USERNAME" &>/dev/null; then
        if userdel "$USERNAME" 2>/dev/null; then
            ok "removed user '$USERNAME'"
        else
            warn "could not remove user '$USERNAME' (has running processes?). Remove manually: userdel $USERNAME"
        fi
    fi

    echo
    ok "uninstall complete"
    exit 0
fi

# ═══════════════════════════════════════════════════════════════════
#  INSTALL
# ═══════════════════════════════════════════════════════════════════
echo "Installing vireon..."

# ── Step 1: Create system user ──────────────────────────────────────
if id "$USERNAME" &>/dev/null; then
    ok "user '$USERNAME' already exists"
else
    useradd --system --no-create-home --shell /usr/sbin/nologin "$USERNAME"
    ok "created system user '$USERNAME'"
fi

# ── Step 2: Locate + install binary ─────────────────────────────────
BINARY=""
for candidate in \
    "${REPO_ROOT}/target/x86_64-unknown-linux-gnu/release/${BINARY_NAME}" \
    "${REPO_ROOT}/target/release/${BINARY_NAME}"; do
    if [[ -x "$candidate" ]]; then
        BINARY="$candidate"
        break
    fi
done

if [[ -z "$BINARY" ]]; then
    err "binary not found. Build it first:"
    echo "  cargo build --release -p quic-server" >&2
    exit 1
fi

install -m 755 "$BINARY" "$INSTALL_BIN"
ok "installed binary → $INSTALL_BIN"

# ── Step 3: Create directories ──────────────────────────────────────
mkdir -p "$CERTS_DIR" "$DATA_DIR" "$LOG_DIR"
chown -R "${USERNAME}:${USERNAME}" "$DATA_DIR" "$LOG_DIR"
chown -R root:"${USERNAME}" "$CONFIG_DIR"
chmod 750 "$CONFIG_DIR" "$CERTS_DIR"
ok "created directories: $CONFIG_DIR, $DATA_DIR, $LOG_DIR"

# ── Step 4: Install config (preserve existing) ──────────────────────
if [[ -f "$CONFIG_FILE" ]]; then
    warn "config already exists — kept: $CONFIG_FILE"
else
    install -m 640 -o root -g "${USERNAME}" "$CONF_SRC" "$CONFIG_FILE"
    ok "installed config → $CONFIG_FILE"
fi

# ── Step 5: Install systemd unit ────────────────────────────────────
install -m 644 "$SERVICE_SRC" "$SERVICE_DST"
systemctl daemon-reload
ok "installed systemd unit → $SERVICE_DST"

# ── Step 6: Generate self-signed cert (optional) ────────────────────
if $GEN_CERT; then
    if [[ -f "$CERTS_DIR/cert.pem" ]]; then
        warn "cert already exists — skipped generation"
    elif ! command -v openssl &>/dev/null; then
        warn "openssl not found — install it and generate certs manually"
    else
        openssl req -x509 -newkey rsa:2048 \
            -keyout "$CERTS_DIR/key.pem" \
            -out "$CERTS_DIR/cert.pem" \
            -days 365 -nodes \
            -subj "/CN=localhost" 2>/dev/null
        chown root:"${USERNAME}" "$CERTS_DIR/cert.pem" "$CERTS_DIR/key.pem"
        chmod 640 "$CERTS_DIR/cert.pem" "$CERTS_DIR/key.pem"
        ok "generated self-signed cert (dev only) → $CERTS_DIR"
    fi
else
    if [[ ! -f "$CERTS_DIR/cert.pem" ]]; then
        warn "no TLS cert found. Generate one:"
        echo "  sudo ./scripts/install.sh --gen-cert"
        echo "  # or place your own cert.pem + key.pem in $CERTS_DIR/"
    fi
fi

# ── Done ────────────────────────────────────────────────────────────
echo
ok "install complete"
echo
echo "  Start:      sudo systemctl enable --now vireon"
echo "  Status:     sudo systemctl status vireon"
echo "  Config:     $CONFIG_FILE"
echo "  Logs:       journalctl -u vireon -f"
echo "  Metrics:    http://localhost:9090/metrics"
