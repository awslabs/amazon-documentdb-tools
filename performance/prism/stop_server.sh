#!/usr/bin/env bash
# Stop the running Prism server (gunicorn or `python app.py`).
set -uo pipefail

stopped=0
for pat in "gunicorn app:server" "python.* app.py" "python3.* app.py"; do
    PIDS="$(pgrep -f "$pat" 2>/dev/null || true)"
    if [ -n "$PIDS" ]; then
        # shellcheck disable=SC2086
        kill $PIDS 2>/dev/null && echo "Stopped Prism (PID: ${PIDS})" && stopped=1
    fi
done

[ "$stopped" -eq 0 ] && echo "Prism is not running"
exit 0
