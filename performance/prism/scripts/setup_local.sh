#!/usr/bin/env bash
# setup_local.sh — prepare Prism for a local (laptop) run.
#
# Philosophy: CHECK AND INSTRUCT. This script never installs system packages or
# mutates your machine. It verifies what's needed, fixes only project-local,
# safe things (the TLS CA bundle), and tells you exactly what to do for the rest.
#
# It does NOT start the server (run ./start_server.sh yourself afterwards).
set -uo pipefail

_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib_common.sh
source "${_DIR}/lib_common.sh"

log_head "Prism local setup (check & instruct) — ${APP_ROOT}"

# 1. Interpreter — instruct only, never install.
check_python
if [ -z "${PYTHON_BIN:-}" ]; then
    log_info "  macOS:  brew install python@3.11"
    log_info "  Linux:  use your package manager or pyenv to install Python 3.11+"
fi

# 2. Python deps — instruct (don't auto-install into an unknown environment).
if [ -n "${PYTHON_BIN:-}" ]; then
    check_python_packages || {
        log_info "  Fix:  ${PYTHON_BIN} -m pip install -r '${APP_ROOT}/requirements.txt'"
        log_info "  (Tip: use a virtualenv: ${PYTHON_BIN} -m venv .venv && source .venv/bin/activate)"
    }
fi

# 3. TLS CA bundle — safe, project-local: fetch if missing.
if [ ! -f "$CA_BUNDLE" ]; then
    log_info "Fetching DocumentDB TLS CA bundle..."
    if command -v curl >/dev/null 2>&1 && curl -fsSL -o "$CA_BUNDLE" "$CA_BUNDLE_URL"; then
        log_pass "TLS CA bundle" 2>/dev/null; log_info "  saved: ${CA_BUNDLE}"
    else
        log_warn "Could not download CA bundle. Manual: curl -fsSL -o '${CA_BUNDLE}' '${CA_BUNDLE_URL}'"
    fi
fi

# 4. gunicorn is optional locally (you can run `python app.py` for dev).
check_gunicorn dev

# 5. Run the full preflight in dev mode for the summary table.
log_head "Running preflight (dev mode)"
if "${_DIR}/preflight.sh" --mode dev; then
    log_head "Next steps"
    log_info "Dev run (foreground, localhost):   ${PYTHON_BIN:-python3} app.py"
    log_info "Production-style (gunicorn):        ./start_server.sh"
    log_info "Connect: open the URL, then enter region + cluster + DB credentials on the auth page."
    log_info "Private cluster? Provide bastion host + SSH key path on the auth page (needs the 'ssh' client)."
else
    log_head "Setup incomplete"
    log_info "Resolve the hard failure(s) above, then re-run: ${_DIR}/setup_local.sh"
    exit 1
fi
