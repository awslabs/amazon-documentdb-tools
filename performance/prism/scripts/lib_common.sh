#!/usr/bin/env bash
# lib_common.sh — shared functions for Prism setup / preflight / start.
#
# Sourced by preflight.sh, setup_local.sh, setup_ec2.sh, start_server.sh.
# Pure bash; no side effects on source (only function definitions + constants).
#
# Two-tier check model:
#   HARD checks  -> failure blocks startup (exit non-zero)
#   SOFT checks  -> failure warns but allows startup (e.g. creds entered later in UI)

# ── Resolve project root (dir that contains app.py), regardless of caller cwd ──
# This file lives in <root>/scripts/, so root is one level up.
_LIB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_ROOT="$(cd "${_LIB_DIR}/.." && pwd)"

# ── Config (overridable via environment) ──────────────────────────────────────
# New canonical prefix: PRISM_*. Legacy DOCULENS_* accepted as fallback.
PRISM_PORT="${PRISM_PORT:-${DOCULENS_PORT:-8501}}"
PRISM_HOST="${PRISM_HOST:-${DOCULENS_HOST:-0.0.0.0}}"
PRISM_WORKERS="${PRISM_WORKERS:-${DOCULENS_WORKERS:-1}}"        # 1 by design: agent keeps state in-process
PRISM_THREADS="${PRISM_THREADS:-${DOCULENS_THREADS:-8}}"        # threads per worker (gthread) for request concurrency
MIN_PY_MAJOR=3
MIN_PY_MINOR=11
CA_BUNDLE="${APP_ROOT}/global-bundle.pem"
CA_BUNDLE_URL="https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem"
LOG_DIR="${APP_ROOT}/logs"

# Required importable Python packages (HARD).
_REQUIRED_PKGS=(dash pymongo boto3 lz4 zstandard yaml plotly pandas)

# ── Output helpers ────────────────────────────────────────────────────────────
if [ -t 1 ]; then
    _C_GREEN=$'\033[0;32m'; _C_YELLOW=$'\033[0;33m'; _C_RED=$'\033[0;31m'
    _C_BOLD=$'\033[1m'; _C_RESET=$'\033[0m'
else
    _C_GREEN=""; _C_YELLOW=""; _C_RED=""; _C_BOLD=""; _C_RESET=""
fi

# Counters populated by the check_* functions.
HARD_FAILURES=0
SOFT_WARNINGS=0
declare -a CHECK_RESULTS   # "STATUS|name|detail" lines for the summary table.

log_info()  { printf '%s\n' "$*"; }
log_pass()  { printf '%s[PASS]%s %s\n' "$_C_GREEN" "$_C_RESET" "$*"; }
log_warn()  { printf '%s[WARN]%s %s\n' "$_C_YELLOW" "$_C_RESET" "$*"; }
log_fail()  { printf '%s[FAIL]%s %s\n' "$_C_RED" "$_C_RESET" "$*"; }
log_head()  { printf '\n%s== %s ==%s\n' "$_C_BOLD" "$*" "$_C_RESET"; }

# record STATUS name detail   (STATUS in PASS|WARN|FAIL|INFO)
_record() {
    local status="$1" name="$2" detail="$3"
    CHECK_RESULTS+=("${status}|${name}|${detail}")
    case "$status" in
        PASS) log_pass "${name} — ${detail}" ;;
        INFO) log_info "${name} — ${detail}" ;;
        WARN) log_warn "${name} — ${detail}"; SOFT_WARNINGS=$((SOFT_WARNINGS+1)) ;;
        FAIL) log_fail "${name} — ${detail}"; HARD_FAILURES=$((HARD_FAILURES+1)) ;;
    esac
}

# ── Python discovery ──────────────────────────────────────────────────────────
# Prefer python3.11+; fall back to python3. Echoes the interpreter path or empty.
detect_python() {
    local candidate
    for candidate in python3.13 python3.12 python3.11 python3 python; do
        if command -v "$candidate" >/dev/null 2>&1; then
            if "$candidate" - <<'PY' 2>/dev/null
import sys
raise SystemExit(0 if sys.version_info[:2] >= (3, 11) else 1)
PY
            then
                command -v "$candidate"
                return 0
            fi
        fi
    done
    return 1
}

# ── HARD checks ───────────────────────────────────────────────────────────────
check_python() {
    local py; py="$(detect_python)"
    if [ -z "$py" ]; then
        _record FAIL "Python ${MIN_PY_MAJOR}.${MIN_PY_MINOR}+" \
            "not found. Install python3.11 (AL2023: sudo dnf install python3.11; macOS: brew install python@3.11)"
        PYTHON_BIN=""
        return 1
    fi
    local ver; ver="$("$py" -c 'import sys;print("%d.%d.%d"%sys.version_info[:3])')"
    _record PASS "Python ${MIN_PY_MAJOR}.${MIN_PY_MINOR}+" "$py (v$ver)"
    PYTHON_BIN="$py"
    return 0
}

check_python_packages() {
    [ -z "${PYTHON_BIN:-}" ] && { _record FAIL "Python packages" "no interpreter"; return 1; }
    local missing=()
    local pkg
    for pkg in "${_REQUIRED_PKGS[@]}"; do
        "$PYTHON_BIN" -c "import ${pkg}" >/dev/null 2>&1 || missing+=("$pkg")
    done
    if [ ${#missing[@]} -gt 0 ]; then
        _record FAIL "Python packages" \
            "missing: ${missing[*]}. Run: ${PYTHON_BIN} -m pip install -r '${APP_ROOT}/requirements.txt'"
        return 1
    fi
    _record PASS "Python packages" "all required modules importable"
    return 0
}

check_gunicorn() {
    # HARD only when server mode is intended (passed as arg 1 = "server").
    local mode="${1:-server}"
    if [ -z "${PYTHON_BIN:-}" ]; then
        _record FAIL "gunicorn" "no interpreter"; return 1
    fi
    if "$PYTHON_BIN" -c "import gunicorn" >/dev/null 2>&1 || command -v gunicorn >/dev/null 2>&1; then
        _record PASS "gunicorn" "available"
        return 0
    fi
    if [ "$mode" = "server" ]; then
        _record FAIL "gunicorn" "not installed. Run: ${PYTHON_BIN} -m pip install gunicorn"
        return 1
    fi
    _record WARN "gunicorn" "not installed (only needed to run the production server)"
    return 0
}

check_ca_bundle() {
    if [ -f "$CA_BUNDLE" ]; then
        _record PASS "TLS CA bundle" "$CA_BUNDLE"
        return 0
    fi
    _record FAIL "TLS CA bundle" \
        "missing. Run: curl -fsSL -o '${CA_BUNDLE}' '${CA_BUNDLE_URL}'"
    return 1
}

# Ensure the Amazon RDS/DocumentDB CA bundle is present, downloading it if not.
# Prefers wget, falls back to curl. Returns 0 if the bundle is present after
# the call, 1 otherwise. Safe to call before preflight so a missing bundle is
# fetched automatically instead of blocking startup.
ensure_ca_bundle() {
    if [ -f "$CA_BUNDLE" ]; then
        return 0
    fi
    log_info "TLS CA bundle not found — downloading from ${CA_BUNDLE_URL}"
    if command -v wget >/dev/null 2>&1; then
        wget -q -O "$CA_BUNDLE" "$CA_BUNDLE_URL"
    elif command -v curl >/dev/null 2>&1; then
        curl -fsSL -o "$CA_BUNDLE" "$CA_BUNDLE_URL"
    else
        log_fail "TLS CA bundle missing and neither wget nor curl is available to download it."
        return 1
    fi
    if [ -s "$CA_BUNDLE" ]; then
        chmod 644 "$CA_BUNDLE" 2>/dev/null || true
        log_pass "TLS CA bundle downloaded to ${CA_BUNDLE}"
        return 0
    fi
    rm -f "$CA_BUNDLE" 2>/dev/null || true
    log_fail "TLS CA bundle download failed (${CA_BUNDLE_URL})."
    return 1
}

check_port_free() {
    local port="${1:-$PRISM_PORT}"
    if command -v lsof >/dev/null 2>&1; then
        if lsof -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then
            _record FAIL "Port ${port}" "already in use (lsof -iTCP:${port} -sTCP:LISTEN to inspect)"
            return 1
        fi
    elif command -v ss >/dev/null 2>&1; then
        if ss -ltn 2>/dev/null | grep -q ":${port} "; then
            _record FAIL "Port ${port}" "already in use (ss -ltn | grep :${port})"
            return 1
        fi
    fi
    _record PASS "Port ${port}" "free"
    return 0
}

# ── SOFT checks ───────────────────────────────────────────────────────────────
check_aws_credentials() {
    local region="${AWS_DEFAULT_REGION:-${AWS_REGION:-us-east-1}}"
    if [ -n "${PYTHON_BIN:-}" ] && "$PYTHON_BIN" - "$region" <<'PY' 2>/dev/null
import sys
try:
    import boto3
    ident = boto3.client("sts", region_name=sys.argv[1]).get_caller_identity()
    print(ident.get("Arn", ""))
    raise SystemExit(0)
except SystemExit:
    raise
except Exception:
    raise SystemExit(1)
PY
    then
        _record PASS "AWS credentials" "resolved via boto3 credential chain"
        return 0
    fi
    _record WARN "AWS credentials" \
        "not resolvable. Local: run 'aws configure' or set AWS_PROFILE. EC2: attach an instance role."
    return 1
}

check_bedrock_access() {
    local region="${AWS_DEFAULT_REGION:-${AWS_REGION:-us-east-1}}"
    if [ -n "${PYTHON_BIN:-}" ] && "$PYTHON_BIN" - "$region" <<'PY' 2>/dev/null
import sys
try:
    import boto3
    c = boto3.client("bedrock", region_name=sys.argv[1])
    c.list_foundation_models()
    raise SystemExit(0)
except SystemExit:
    raise
except Exception:
    raise SystemExit(1)
PY
    then
        _record PASS "Bedrock access" "API reachable (enable Claude Sonnet 4 + Haiku model access for AI features)"
        return 0
    fi
    _record WARN "Bedrock access" \
        "could not list models. AI features need bedrock:InvokeModel + model access in your region."
    return 1
}

check_ssh_client() {
    # Only relevant for SSH-tunnel (private cluster) mode.
    if command -v ssh >/dev/null 2>&1; then
        _record PASS "ssh client" "$(command -v ssh) (needed for tunnel mode)"
        return 0
    fi
    _record WARN "ssh client" "not found — required only for private-cluster SSH tunnel mode"
    return 1
}

# DocumentDB TCP reachability (SOFT). Endpoint from $PRISM_CHECK_ENDPOINT
# in host:port form, else skipped (credentials/endpoint are entered in the UI).
check_docdb_reachable() {
    local endpoint="${PRISM_CHECK_ENDPOINT:-${DOCULENS_CHECK_ENDPOINT:-}}"
    if [ -z "$endpoint" ]; then
        _record INFO "DocumentDB reachability" \
            "skipped (optional) — set PRISM_CHECK_ENDPOINT=host:27017 to test TCP path before starting"
        return 0
    fi
    local host="${endpoint%%:*}" port="${endpoint##*:}"
    [ "$host" = "$port" ] && port=27017
    if [ -z "${PYTHON_BIN:-}" ]; then
        _record WARN "DocumentDB reachability" "no interpreter to probe ${endpoint}"
        return 0
    fi
    if "$PYTHON_BIN" - "$host" "$port" <<'PY' 2>/dev/null
import socket, sys
host, port = sys.argv[1], int(sys.argv[2])
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.settimeout(5)
try:
    s.connect((host, port))
    raise SystemExit(0)
except SystemExit:
    raise
except Exception:
    raise SystemExit(1)
finally:
    s.close()
PY
    then
        _record PASS "DocumentDB reachability" "TCP connect to ${host}:${port} OK"
        return 0
    fi
    _record WARN "DocumentDB reachability" \
        "cannot reach ${host}:${port}. Check security groups (inbound 27017 from this host), subnet route, VPC peering, DNS."
    return 1
}

# ── Public IP detection (IMDSv2, EC2 only) ────────────────────────────────────
# Echoes the public IPv4 if on EC2 and available; empty otherwise.
detect_public_ip() {
    command -v curl >/dev/null 2>&1 || return 1
    local token ip
    token="$(curl -fsS -m 2 -X PUT "http://169.254.169.254/latest/api/token" \
        -H "X-aws-ec2-metadata-token-ttl-seconds: 60" 2>/dev/null)" || return 1
    [ -z "$token" ] && return 1
    ip="$(curl -fsS -m 2 -H "X-aws-ec2-metadata-token: $token" \
        "http://169.254.169.254/latest/meta-data/public-ipv4" 2>/dev/null)" || return 1
    [ -z "$ip" ] && return 1
    printf '%s' "$ip"
}

# ── Summary table ─────────────────────────────────────────────────────────────
print_summary() {
    log_head "Preflight summary"
    printf '%-26s %s\n' "CHECK" "RESULT"
    printf '%-26s %s\n' "--------------------------" "----------------------------------------"
    local line status name detail color
    for line in "${CHECK_RESULTS[@]}"; do
        status="${line%%|*}"; rest="${line#*|}"; name="${rest%%|*}"; detail="${rest#*|}"
        case "$status" in
            PASS) color="$_C_GREEN" ;; INFO) color="" ;; WARN) color="$_C_YELLOW" ;; FAIL) color="$_C_RED" ;; *) color="" ;;
        esac
        printf '%-26s %s%-4s%s %s\n' "$name" "$color" "$status" "$_C_RESET" "$detail"
    done
    printf '\n%d hard failure(s), %d warning(s).\n' "$HARD_FAILURES" "$SOFT_WARNINGS"
}

# Returns 0 if safe to proceed (no HARD failures), 1 otherwise.
preflight_ok() { [ "$HARD_FAILURES" -eq 0 ]; }
