#!/usr/bin/env bash
# setup_ec2.sh — prepare Amazon DocumentDB Prism on a fresh Amazon Linux 2023 EC2 instance.
#
# Installs system + Python dependencies, fetches the TLS CA bundle, then runs
# preflight. It does NOT start the server (run ./start_server.sh yourself).
#
# Assumes: Amazon Linux 2023, sudo available, EC2 instance role attached with
# the permissions in iam_policy.json. Direct VPC reachability to DocumentDB.
set -uo pipefail

_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib_common.sh
source "${_DIR}/lib_common.sh"

log_head "Amazon DocumentDB Prism EC2 setup (Amazon Linux 2023) — ${APP_ROOT}"

if ! command -v dnf >/dev/null 2>&1; then
    log_fail "dnf not found — this script targets Amazon Linux 2023. For other distros, install"
    log_info "python3.11 + python3.11-devel + gcc + git manually, then run scripts/preflight.sh."
    exit 1
fi

# 1. System packages (Python 3.11 + native build tools for lz4/zstd + git).
log_head "Installing system packages (sudo)"
sudo dnf install -y python3.11 python3.11-pip python3.11-devel gcc git \
    || { log_fail "dnf install failed"; exit 1; }

PYTHON_BIN="$(command -v python3.11 || true)"
[ -z "$PYTHON_BIN" ] && { log_fail "python3.11 not on PATH after install"; exit 1; }
log_pass "Python 3.11" "$PYTHON_BIN"

# Make python3 point to 3.11 so all commands (create_user.py, gunicorn) use it.
sudo alternatives --install /usr/bin/python3 python3 /usr/bin/python3.11 1 2>/dev/null || true
sudo alternatives --set python3 /usr/bin/python3.11 2>/dev/null || true

# 2. Python dependencies + gunicorn (production server).
log_head "Installing Python dependencies"
"$PYTHON_BIN" -m pip install --user -q -r "${APP_ROOT}/requirements.txt" \
    || { log_fail "pip install -r requirements.txt failed"; exit 1; }
"$PYTHON_BIN" -m pip install --user -q gunicorn \
    || { log_fail "pip install gunicorn failed"; exit 1; }
log_pass "Dependencies" "requirements.txt + gunicorn installed"

# 3. TLS CA bundle.
if [ ! -f "$CA_BUNDLE" ]; then
    log_head "Downloading DocumentDB TLS CA bundle"
    curl -fsSL -o "$CA_BUNDLE" "$CA_BUNDLE_URL" \
        || { log_fail "CA bundle download failed: ${CA_BUNDLE_URL}"; exit 1; }
    log_pass "TLS CA bundle" "$CA_BUNDLE"
fi

# 4. Preflight (server mode). DocDB reachability is probed if you export
#    PRISM_CHECK_ENDPOINT=<cluster-endpoint>:27017 before running.
log_head "Running preflight (server mode)"
if "${_DIR}/preflight.sh" --mode server; then
    PUB_IP="$(detect_public_ip || true)"
    log_head "Setup complete — server NOT started (start it yourself)"
    log_info "Start:  ./start_server.sh"
    if [ -n "$PUB_IP" ]; then
        log_info "Then open: http://${PUB_IP}:${PRISM_PORT}"
        log_info "  ↳ Security group must allow inbound TCP ${PRISM_PORT} from your IP,"
        log_info "    and outbound 27017 to DocumentDB + 443 to AWS APIs."
    else
        log_info "Then open: http://<this-instance-public-ip>:${PRISM_PORT}"
    fi
    log_warn "On the auth page, set Connection Mode = 'Direct' (it defaults to 'SSH Tunnel')."
    log_info "  ↳ EC2 reaches DocumentDB directly over the VPC; no bastion/tunnel needed here."
    log_info "Reachability tip: re-run with PRISM_CHECK_ENDPOINT=<cluster-endpoint>:27017 to test the DB path."
else
    log_head "Setup incomplete"
    log_info "Resolve the hard failure(s) above, then re-run: ${_DIR}/setup_ec2.sh"
    exit 1
fi
