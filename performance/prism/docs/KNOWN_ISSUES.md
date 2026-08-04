# Known Issues / Deferred Fixes

Tracked items deliberately deferred. Each entry: symptom, confirmed root cause,
fix direction, and blast radius.

---

## 1. Activity tab loads slowly (blocking network I/O in render)

**Status:** Open — deferred (analysis complete, fix not started)
**Severity:** Medium (UX); worsens to "freeze for tens of seconds" when the SSH tunnel is degraded.

### Symptom
Clicking the Activity tab takes several seconds to render (~4s with 2 healthy
instances, observed in logs). Worse with more instances or a flaky tunnel.

### Root cause (confirmed in code + logs)
`tabs/current_activity.py::render_current_activity` performs serial, synchronous
network operations **directly in the Dash render callback** (`cb_render_panel`):

1. `_discover_instances(cluster_id, region)` — boto3 `describe_db_clusters` +
   `describe_db_instances` (uncached; runs on every tab open).
2. A loop over every instance calling `_fetch_current_ops(inst_conn)` — each is a
   fresh `pymongo.MongoClient` connect + `$currentOp` aggregation, with
   `serverSelectionTimeoutMS=5000`, preceded by `_check_tunnel_health` (which can
   call `ensure_tunnel()` → `reconnect_tunnel()` inline).

The render cannot return until all of this completes. Log evidence (healthy
tunnel, 2 instances): click at 19:24:24.982 → render POST 200 at 19:24:29.143 =
~4.2s, the callback itself blocking the whole time.

Contributing factors:
- `_fetch_current_ops` has a fallback chain (aggregation → `command` ×2) that
  adds round-trips when an instance returns empty/errors.
- `_discover_instances` is uncached → fixed boto3 tax on every open.
- On a degraded tunnel, each instance can burn the full 5s timeout (5s × N), and
  an inline `ensure_tunnel()`/`reconnect_tunnel()` can open N SSH processes from
  within the render path → the "tens of seconds freeze."

### Why it's the only slow tab
Every other heavy tab (slow query, WA, compression, code review) uses the
background-thread + `dcc.Interval` poll pattern: render returns instantly with a
placeholder, data fills in via poll. Activity is the only tab doing blocking
network I/O directly in render. This is the architectural root flaw.

### Fix direction (when prioritized)
1. Move the activity fetch to the established background-thread + `dcc.Interval`
   poll pattern: render an instant placeholder, fetch in a daemon thread, surface
   results via poll.
2. Cache `_discover_instances` per session (instances rarely change mid-session).
3. Lower the per-instance `serverSelectionTimeoutMS` so one bad instance can't
   stall the whole view; fetch instances in parallel or bounded.
4. Do NOT call `ensure_tunnel()`/`reconnect_tunnel()` from the render path.

### Blast radius
`tabs/current_activity.py` render + its callbacks. Moving to background+poll
touches the tab's callback wiring (new Interval, poll callback, module state).
Self-contained to the Activity tab; does not affect other tabs.

### Notes
- `activity-timer` interval is currently `disabled=True` (auto-refresh was
  previously disabled), so today the slowness is the one-shot initial load on tab
  open, not repeated polling.

---

## 2. SSH tunnel "half-dead" state not detected

**Status:** Open — deferred
**Severity:** Medium (root of recurring tunnel pain when it occurs).

### Symptom
Tunnel intermittently goes `server_type: Unknown` (TCP up, Mongo not responding).
Recovery is slow because the app doesn't notice until the SSH process fully exits
(observed multi-minute gaps before reconnect).

### Root cause (confirmed in code)
`ssh_tunnel.is_tunnel_active()` checks only `_tunnel["process"]` (writer) being
alive + `_is_port_in_use(BASE_LOCAL_PORT)` (a local TCP connect). An SSH
port-forward keeps the local listener open even when the remote leg is dead, so
the port check passes while the tunnel is actually unusable. Reconnect only fires
when ssh fully exits (returncode 255).

### Fix direction
- Make `is_tunnel_active()` (or a separate health probe) do a lightweight
  authenticated `hello`/`ping` through the tunnel with a short timeout, treating a
  handshake failure as "degraded → reconnect."
- Consider SSH keepalive tuning (`ServerAliveInterval`/`CountMax`) to reduce
  session drops.
- Per-instance (reader) tunnels are unmonitored — only the writer is tracked.

### Blast radius
`ssh_tunnel.py` health/reconnect logic — touches the entire tunnel-mode
connection. High blast radius; change with care and verify against a real bastion.

### Mitigations already in place
- `db_analyzer` MongoClient now uses bounded timeouts
  (`serverSelectionTimeoutMS=5000, connectTimeoutMS=5000, socketTimeoutMS=45000`)
  so db_analysis stays resilient to tunnel blips even without half-dead detection.
- `reconnect_tunnel()` now preserves the instances list, so reader tunnels are
  restored on reconnect.
- **Half-dead detection** is now implemented: `_probe_tunnel_alive()` runs a
  throttled (8s TTL) `ping` through the writer tunnel with a 2s bound and fails
  closed so `ensure_tunnel()` reconnects.

### Follow-up RESOLVED: writer-port drift caused a perpetual reconnect loop
**Confirmed via logs + live `ps`:** an orphaned `ssh -L 47017:...docdb...`
process from a *prior* `python3 app.py` run kept holding `BASE_LOCAL_PORT`
(47017). `_kill_all()` only reaps processes tracked by the current run, so the
orphan survived; `_next_free_port(47017)` then assigned the writer 47018 (reader
47019) on the *very first* open. Every consumer (`_probe_tunnel_alive`,
`get_tunnel_connection_string`, db_analysis) targets the `BASE_LOCAL_PORT`
constant, so they all hit the dead 47017 → probe always timed out → reconnect
every ~75s, and `db_analysis` failed with `localhost:47017: timed out`.

**Fix:** `open_tunnel` now pins the writer to `BASE_LOCAL_PORT` deterministically.
Before binding the writer it calls `_reap_orphan_tunnels_on_port(47017)` (narrowly
matches only our own `ssh -L 47017:...docdb...` forwards by command line, never
unrelated processes) then `_wait_port_free()`. Readers still use `_next_free_port`
from 47018+. Covered by `tests/test_ssh_tunnel_orphan_reap.py`.


---

## 3. `$build_conn` / "Load DB" path opens writer-only tunnel

**Status:** Open — deferred (low priority; not the primary connect flow)
**Severity:** Low.

### Detail
`app.py::_build_conn` (used by `cb_load_dbs`, the "Load DB" button) calls
`open_tunnel(...)` **without** `instances`, so it opens a cluster-endpoint-only
(writer) tunnel. The primary Fleet "Connect" flow (`cb_auth_connect`) correctly
passes instances. If a user reaches a connected state via the Load-DB path, reader
tunnels would be missing.

### Fix direction
Pass instances to `open_tunnel` in `_build_conn` as well, mirroring
`cb_auth_connect`.

### Blast radius
`app.py::_build_conn` only. Low.
