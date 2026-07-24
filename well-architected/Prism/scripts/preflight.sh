#!/usr/bin/env bash
# preflight.sh — run all Prism readiness checks and print a summary.
#
# Exit code:
#   0  -> safe to start (no HARD failures; SOFT warnings allowed)
#   1  -> HARD failure(s) present; do NOT start the server
#
# Usage:
#   ./scripts/preflight.sh                 # server-mode checks (gunicorn HARD)
#   ./scripts/preflight.sh --mode dev      # dev-mode (gunicorn only a WARN)
#   PRISM_CHECK_ENDPOINT=host:27017 ./scripts/preflight.sh   # also probe DocDB
set -uo pipefail

_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib_common.sh
source "${_DIR}/lib_common.sh"

MODE="server"
while [ $# -gt 0 ]; do
    case "$1" in
        --mode) MODE="${2:-server}"; shift 2 ;;
        *) shift ;;
    esac
done

log_head "Prism preflight (${MODE} mode) — root: ${APP_ROOT}"

# HARD
check_python
check_python_packages
check_gunicorn "$MODE"
check_ca_bundle
check_port_free "$PRISM_PORT"

# SOFT
check_aws_credentials
check_bedrock_access
check_ssh_client
check_docdb_reachable

print_summary

if preflight_ok; then
    log_info ""
    if [ "$SOFT_WARNINGS" -gt 0 ]; then
        log_warn "Passed with ${SOFT_WARNINGS} warning(s). These may surface at runtime"
        log_warn "(e.g. enter DB credentials in the auth page; enable Bedrock model access)."
    fi
    log_info "${_C_GREEN}Ready.${_C_RESET} Start with: ./start_server.sh"
    exit 0
else
    log_info ""
    log_fail "Not ready — resolve the ${HARD_FAILURES} hard failure(s) above, then re-run preflight."
    exit 1
fi
