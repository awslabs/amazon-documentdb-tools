#!/usr/bin/env bash
# Start Prism (gunicorn, nohup) on $PRISM_HOST:$PRISM_PORT.
#
# - Portable: derives its own directory (no hardcoded paths).
# - Single worker by design (the agent keeps analysis state in-process;
#   multiple workers would fragment it).
# - Writes a NEW timestamped log file under logs/ on every start.
# - Runs preflight first; refuses to start on a HARD failure.
#
# Env overrides: PRISM_PORT (8501), PRISM_HOST (0.0.0.0),
#                PRISM_WORKERS (1), PRISM_THREADS (8)
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/lib_common.sh
source "${SCRIPT_DIR}/scripts/lib_common.sh"

cd "$APP_ROOT" || { echo "Cannot cd to $APP_ROOT"; exit 1; }

# ── Ensure TLS CA bundle is present (download if missing) ─────────────────────
ensure_ca_bundle || true

# ── Preflight (HARD failures block startup) ───────────────────────────────────
if [ "${PRISM_SKIP_PREFLIGHT:-0}" != "1" ]; then
    if ! "${SCRIPT_DIR}/scripts/preflight.sh" --mode server; then
        echo ""
        log_fail "Aborting start due to preflight hard failure(s). (Set PRISM_SKIP_PREFLIGHT=1 to override — not recommended.)"
        exit 1
    fi
fi

# ── Stop any existing instance ────────────────────────────────────────────────
pkill -f "gunicorn app:server" 2>/dev/null && sleep 1 || true

# ── Fresh timestamped log per start ───────────────────────────────────────────
mkdir -p "$LOG_DIR"
TS="$(date +%Y%m%d_%H%M%S)"
LOG_FILE="${LOG_DIR}/prism_${TS}.log"

log_head "Starting Prism"
log_info "Bind:    ${PRISM_HOST}:${PRISM_PORT}"
log_info "Workers: ${PRISM_WORKERS} (gthread, ${PRISM_THREADS} threads)"
log_info "Log:     ${LOG_FILE}"

# Resolve gunicorn invocation (module form works whether or not it's on PATH).
if command -v gunicorn >/dev/null 2>&1; then
    GUNICORN=(gunicorn)
else
    GUNICORN=("${PYTHON_BIN:-python3}" -m gunicorn)
fi

nohup "${GUNICORN[@]}" app:server \
    --bind "${PRISM_HOST}:${PRISM_PORT}" \
    --workers "${PRISM_WORKERS}" \
    --worker-class gthread \
    --threads "${PRISM_THREADS}" \
    --timeout 120 \
    --graceful-timeout 30 \
    --access-logfile - \
    > "$LOG_FILE" 2>&1 &

PID=$!
echo "Started with PID: ${PID}"

# ── Verify it actually came up ────────────────────────────────────────────────
UP=0
for _ in $(seq 1 15); do
    sleep 1
    if curl -fsS -o /dev/null "http://localhost:${PRISM_PORT}/" 2>/dev/null; then
        UP=1; break
    fi
    # Bail early if the process died.
    kill -0 "$PID" 2>/dev/null || break
done

if [ "$UP" -eq 1 ]; then
    PUB_IP="$(detect_public_ip || true)"
    log_pass "Prism is running (PID ${PID})."
    if [ -n "$PUB_IP" ]; then
        log_info "Open: http://${PUB_IP}:${PRISM_PORT}"
        log_info "  ↳ Ensure the EC2 security group allows inbound TCP ${PRISM_PORT} from your IP."
    else
        log_info "Open: http://localhost:${PRISM_PORT}"
    fi
    log_info "Logs: ${LOG_FILE}    Stop: ./stop_server.sh"
else
    log_fail "Prism did not become healthy on port ${PRISM_PORT}. Last log lines:"
    tail -n 25 "$LOG_FILE" 2>/dev/null || true
    exit 1
fi
