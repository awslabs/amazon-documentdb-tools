"""SSH Tunnel manager for DocumentDB connections via bastion host.

Supports multi-instance tunnels: one local port per cluster instance,
so readers can be queried independently for activity, index usage, etc.
"""
import subprocess
import threading
import logging
import time
import socket
import traceback
import signal
import re
import os as _os

logger = logging.getLogger(__name__)

BASE_LOCAL_PORT = 47017          # writer / cluster endpoint always gets this port
TUNNEL_TIMEOUT_SECONDS = 0      # 0 = stay alive while session is active

# Obfuscation key
_OBF_KEY = _os.urandom(64)


def _obfuscate(plaintext: str) -> bytearray:
    data = plaintext.encode("utf-8")
    return bytearray(b ^ _OBF_KEY[i % len(_OBF_KEY)] for i, b in enumerate(data))


def _deobfuscate(buf: bytearray) -> str:
    if not buf:
        return ""
    return bytes(b ^ _OBF_KEY[i % len(_OBF_KEY)] for i, b in enumerate(buf)).decode("utf-8")


def _wipe(buf):
    if isinstance(buf, bytearray):
        buf[:] = b'\x00' * len(buf)


# ── State ─────────────────────────────────────────────────────────────────────
# _tunnels: list of per-instance tunnel dicts
#   {process, endpoint, port, local_port, instance_id, role}
# _tunnel: legacy single-tunnel state (kept for backward compat)
_tunnel = {
    "process": None,
    "cluster_endpoint": None,
    "cluster_port": 27017,
    "bastion_host": None,
    "ssh_user": None,
    "key_path": None,
    "timer": None,
    "active": False,
    "_cred_user": None,
    "_cred_pass": None,
    "use_tls": False,
    "instances": None,   # last instances list passed to open_tunnel (for reconnect)
}
_tunnels: list = []          # [{local_port, endpoint, port, instance_id, role, process}]
_lock = threading.Lock()

# Expose for backward compat
LOCAL_PORT = BASE_LOCAL_PORT


def _is_port_in_use(port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(("127.0.0.1", port)) == 0


def _next_free_port(start):
    """Find next free local port starting from start."""
    p = start
    while _is_port_in_use(p):
        p += 1
    return p


def _reap_orphan_tunnels_on_port(port):
    """Kill *our own* orphaned `ssh -L <port>:...` tunnels left by a prior run.

    A previous `python3 app.py` process can exit without closing its SSH
    forwards, leaving an orphan that still holds the writer port. Because
    _kill_all() only reaps processes tracked by the current run, such an orphan
    would force the writer onto the next free port (47018, 47019, ...) while
    every consumer still targets BASE_LOCAL_PORT — producing a perpetual
    reconnect loop. We narrowly target only ssh processes whose command line
    contains our exact local-forward spec ("-L <port>:") so we never touch
    unrelated processes.

    Returns the number of orphan processes signalled.
    """
    forward_token = f"-L {port}:"
    killed = 0
    try:
        out = subprocess.run(
            ["ps", "-axo", "pid=,command="],
            capture_output=True, text=True, timeout=5).stdout
    except Exception as e:
        logger.warning("orphan reap: ps failed on port %d: %s", port, e)
        return 0

    for line in out.splitlines():
        line = line.strip()
        if not line:
            continue
        parts = line.split(None, 1)
        if len(parts) != 2:
            continue
        pid_str, cmd = parts
        # Only our DocumentDB SSH local-forwards on exactly this port.
        if cmd.startswith("ssh ") and forward_token in cmd and "docdb" in cmd:
            try:
                pid = int(pid_str)
            except ValueError:
                continue
            if pid == _os.getpid():
                continue
            try:
                _os.kill(pid, signal.SIGTERM)
                killed += 1
                logger.warning("orphan reap: SIGTERM to stale tunnel pid %d holding port %d",
                               pid, port)
            except ProcessLookupError:
                pass
            except Exception as e:
                logger.warning("orphan reap: could not kill pid %d: %s", pid, e)
    return killed


def _wait_port_free(port, timeout=5.0):
    """Block until `port` is released (or timeout). Returns True if free."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if not _is_port_in_use(port):
            return True
        time.sleep(0.2)
    return not _is_port_in_use(port)


# Hostnames/IPs (incl. IPv6 literals) and usernames must match these patterns
# and must not start with '-', so a user-supplied value can never be parsed by
# ssh as a command-line option (argument injection).
_HOST_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]*$")
_USER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._@-]*$")


def _valid_port(port):
    try:
        p = int(port)
    except (TypeError, ValueError):
        return False
    return 1 <= p <= 65535


def _open_one_tunnel(bastion_host, ssh_user, key_path,
                     remote_endpoint, remote_port, local_port):
    """Open a single SSH port-forward. Returns (process, error_str)."""
    import os
    if not os.path.isfile(key_path):
        return None, f"SSH key not found: {key_path}"

    # ── Validate untrusted, user-supplied inputs before building argv ──────
    # cmd is a list with shell=False, so shell injection is not possible; this
    # guards against argument injection (e.g. a value starting with '-' being
    # parsed as an ssh option) and malformed forward specs.
    if not (bastion_host and _HOST_RE.match(str(bastion_host))):
        return None, f"Invalid bastion host: {bastion_host!r}"
    if not (ssh_user and _USER_RE.match(str(ssh_user))):
        return None, f"Invalid SSH user: {ssh_user!r}"
    if not (remote_endpoint and _HOST_RE.match(str(remote_endpoint))):
        return None, f"Invalid remote endpoint: {remote_endpoint!r}"
    if not _valid_port(remote_port):
        return None, f"Invalid remote port: {remote_port!r}"
    if not _valid_port(local_port):
        return None, f"Invalid local port: {local_port!r}"

    forward = f"{local_port}:{remote_endpoint}:{remote_port}"
    cmd = [
        "ssh", "-i", key_path,
        "-L", forward, "-N",
        "-o", "StrictHostKeyChecking=no",
        "-o", "ServerAliveInterval=30",
        "-o", "ServerAliveCountMax=3",
        "-o", "ExitOnForwardFailure=yes",
        f"{ssh_user}@{bastion_host}",
    ]
    try:
        # SECURITY: cmd is a list and Popen runs with shell=False (default), so
        # no shell is involved and shell injection is not possible. All
        # interpolated values (bastion_host, ssh_user, remote_endpoint, ports)
        # are validated against strict allowlists above, which also prevents
        # argument injection (e.g. a value starting with '-' parsed as an option).
        # Semgrep's dangerous-subprocess-use-audit is an audit rule; this call is
        # not externally controllable. shlex.quote() does not apply (no shell).
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)  # nosemgrep: python.lang.security.audit.dangerous-subprocess-use-audit
        for _ in range(10):
            time.sleep(0.5)
            if proc.poll() is not None:
                stderr = proc.stderr.read().decode(errors="replace").strip()
                return None, stderr or "SSH process exited immediately"
            if _is_port_in_use(local_port):
                return proc, None
        proc.terminate()
        return None, f"Tunnel did not open on port {local_port}"
    except FileNotFoundError:
        return None, "ssh command not found"
    except Exception as e:
        return None, str(e)


def _kill_all():
    """Kill all tunnel processes."""
    with _lock:
        for t in _tunnels:
            p = t.get("process")
            if p and p.poll() is None:
                p.terminate()
                try:
                    p.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    p.kill()
        _tunnels.clear()
        proc = _tunnel["process"]
        if proc and proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                proc.kill()
        _tunnel["process"] = None
        _tunnel["active"] = False


# ── Public API ────────────────────────────────────────────────────────────────

def open_tunnel(bastion_host, ssh_user, key_path,
                cluster_endpoint, cluster_port=27017,
                instances=None):
    """Open SSH tunnels — one per cluster instance.

    If instances are provided, opens one tunnel per instance endpoint
    (writer gets BASE_LOCAL_PORT, readers get subsequent ports).
    The cluster endpoint is NOT given its own tunnel since it resolves
    to the writer anyway.

    Returns:
        dict with 'ok', 'local_port', 'instance_ports', 'error'
        instance_ports: {instance_id: local_port}
    """
    _kill_all()

    import os
    if not os.path.isfile(key_path):
        return {"ok": False, "error": f"SSH key not found: {key_path}"}

    # DIAG: log exactly what instances arrived so we can see why per-instance
    # (reader) tunnels do or don't open.
    try:
        _inst_summary = [(i.get("id"), i.get("role"), bool(i.get("endpoint")))
                         for i in (instances or [])]
    except Exception:
        _inst_summary = "unparseable"
    logger.info("open_tunnel: received %d instance(s): %s",
                len(instances or []), _inst_summary)

    # Persist the instances list so reconnect_tunnel can reopen per-instance
    # (reader) tunnels rather than collapsing to a writer-only tunnel.
    with _lock:
        _tunnel["instances"] = list(instances) if instances else None
        # Invalidate the liveness-probe cache so a freshly (re)opened tunnel is
        # re-probed rather than judged by a stale "dead" result (prevents a
        # reconnect loop).
        _probe_cache["ts"] = 0.0
        _probe_cache["result"] = True

    # Deduplicate instances by endpoint; skip any that match cluster endpoint
    seen_endpoints = set()
    ordered = []
    if instances:
        # Writer first so it gets BASE_LOCAL_PORT
        for inst in sorted(instances,
                           key=lambda i: 0 if i.get("role", "") == "Writer" else 1):
            ep = inst.get("endpoint") or ""
            if ep and ep not in seen_endpoints:
                seen_endpoints.add(ep)
                ordered.append(inst)

    # If no instances provided, fall back to cluster endpoint
    if not ordered:
        logger.warning("open_tunnel: NO usable instances — falling back to "
                       "cluster-endpoint-only tunnel (no reader tunnels will exist)")
        if _is_port_in_use(BASE_LOCAL_PORT):
            _reap_orphan_tunnels_on_port(BASE_LOCAL_PORT)
            _wait_port_free(BASE_LOCAL_PORT, timeout=5.0)
        proc, err = _open_one_tunnel(bastion_host, ssh_user, key_path,
                                     cluster_endpoint, cluster_port, BASE_LOCAL_PORT)
        if err:
            return {"ok": False, "error": err}
        with _lock:
            _tunnel.update(process=proc, cluster_endpoint=cluster_endpoint,
                           cluster_port=cluster_port, bastion_host=bastion_host,
                           ssh_user=ssh_user, key_path=key_path, active=True)
            _tunnels.append({"local_port": BASE_LOCAL_PORT, "endpoint": cluster_endpoint,
                             "port": cluster_port, "instance_id": "_cluster",
                             "role": "cluster", "process": proc})
        logger.info("Cluster tunnel: localhost:%d -> %s:%d (pid %d)",
                    BASE_LOCAL_PORT, cluster_endpoint, cluster_port, proc.pid)
        return {"ok": True, "local_port": BASE_LOCAL_PORT, "instance_ports": {}}

    # One tunnel per instance
    instance_ports = {}
    next_port = BASE_LOCAL_PORT
    primary_proc = None

    for inst in ordered:
        ep   = inst.get("endpoint", "")
        port = inst.get("port", 27017)
        iid  = inst.get("id", "")
        role = inst.get("role", "")

        is_writer = (role == "Writer") or (primary_proc is None)
        if is_writer:
            # The writer MUST own BASE_LOCAL_PORT: the liveness probe,
            # get_tunnel_connection_string, and all DB analysis target it by
            # constant. Reap any orphaned tunnel of ours from a prior run that
            # is still holding the port, then wait for the OS to release it so
            # the writer doesn't drift onto 47018+ (which caused a perpetual
            # reconnect loop).
            if _is_port_in_use(BASE_LOCAL_PORT):
                _reap_orphan_tunnels_on_port(BASE_LOCAL_PORT)
                _wait_port_free(BASE_LOCAL_PORT, timeout=5.0)
            lp = BASE_LOCAL_PORT
            if _is_port_in_use(lp):
                logger.error("Writer port %d still occupied after orphan reap — "
                             "cannot guarantee writer tunnel", lp)
                return {"ok": False,
                        "error": f"Writer port {lp} is held by another process"}
        else:
            lp = _next_free_port(next_port)

        proc, err = _open_one_tunnel(bastion_host, ssh_user, key_path, ep, port, lp)
        if err:
            logger.warning("Tunnel for %s (%s) failed: %s", iid, ep, err)
            continue

        with _lock:
            _tunnels.append({"local_port": lp, "endpoint": ep, "port": port,
                             "instance_id": iid, "role": role, "process": proc})
        instance_ports[iid] = lp
        next_port = lp + 1
        logger.info("%s tunnel: localhost:%d -> %s (%s, pid %d)",
                    role, lp, ep, iid, proc.pid)

        if primary_proc is None:
            primary_proc = proc
            with _lock:
                _tunnel.update(process=proc, cluster_endpoint=ep,
                               cluster_port=port, bastion_host=bastion_host,
                               ssh_user=ssh_user, key_path=key_path, active=True)

    if not instance_ports:
        return {"ok": False, "error": "All instance tunnels failed"}

    # DIAG: confirm how many per-instance tunnels opened (writer + readers).
    logger.info("open_tunnel: opened %d instance tunnel(s): %s",
                len(instance_ports), instance_ports)

    return {"ok": True, "local_port": BASE_LOCAL_PORT,
            "instance_ports": instance_ports}


def get_instance_ports():
    """Return {instance_id: local_port} for all open instance tunnels."""
    with _lock:
        return {t["instance_id"]: t["local_port"]
                for t in _tunnels if t["instance_id"] != "_cluster"}


# Amazon RDS/DocumentDB CA bundle, fetched into the app root by the setup scripts.
_CA_BUNDLE = _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), "global-bundle.pem")


def _tls_query(use_tls):
    """Return the TLS query-string fragment for a tunnel (localhost) connection.

    Empty when TLS is off, so non-TLS connections are unaffected.

    These builders always target 'localhost' (the SSH-tunnel forward), but the
    TLS handshake is end-to-end with DocumentDB, whose certificate is issued for
    *.docdb.amazonaws.com. So we:
      - validate the server certificate against the Amazon RDS CA bundle
        (tlsCAFile) — this is what prevents MITM, and replaces the old blanket
        tlsAllowInvalidCertificates=true; and
      - skip ONLY hostname verification (tlsAllowInvalidHostnames), which can
        never match 'localhost' in tunnel mode.

    Direct (non-tunnel) connections that use a real cluster hostname are built
    elsewhere (aws_discovery / index_usage_cluster) and keep full hostname
    validation — they do not pass through here. Fails closed: if the CA bundle
    is absent, TLS validation fails (we never silently downgrade).
    """
    if not use_tls:
        return ""
    from urllib.parse import quote
    if not _os.path.isfile(_CA_BUNDLE):
        logger.warning("TLS requested but CA bundle not found at %s — connection will "
                       "fail certificate validation until it is present (run setup to "
                       "fetch global-bundle.pem)", _CA_BUNDLE)
    return f"tls=true&tlsCAFile={quote(_CA_BUNDLE, safe='')}&tlsAllowInvalidHostnames=true&"


def get_instance_connection_string(instance_id, username, password, use_tls=False):
    """Build a connection string for a specific instance through its tunnel."""
    with _lock:
        entry = next((t for t in _tunnels if t["instance_id"] == instance_id), None)
    if not entry:
        return None
    lp = entry["local_port"]
    tls = _tls_query(use_tls)
    return (f"mongodb://{username}:{password}@localhost:{lp}/?"
            f"{tls}retryWrites=false&directConnection=true&appName=DocDB-Prism")


def get_all_instance_connection_strings(username, password, use_tls=False):
    """Return {instance_id: conn_str} for every open instance tunnel."""
    with _lock:
        entries = list(_tunnels)
    result = {}
    for t in entries:
        iid = t["instance_id"]
        lp  = t["local_port"]
        tls = _tls_query(use_tls)
        result[iid] = (f"mongodb://{username}:{password}@localhost:{lp}/?"
                       f"{tls}retryWrites=false&directConnection=true&appName=DocDB-Prism")
    return result


def close_tunnel():
    """Close all tunnels and wipe credentials."""
    with _lock:
        _wipe(_tunnel["_cred_user"])
        _wipe(_tunnel["_cred_pass"])
        _tunnel.update(_cred_user=None, _cred_pass=None,
                       cluster_endpoint=None, bastion_host=None,
                       ssh_user=None, key_path=None)
    _kill_all()
    logger.info("All SSH tunnels closed")


def reconnect_tunnel():
    """Reconnect using saved params."""
    # DIAG: capture who triggered the reconnect and at what cadence. The caller
    # stack tells us which periodic path is churning the tunnel.
    caller = "".join(traceback.format_stack(limit=6)[:-1])
    logger.warning("reconnect_tunnel() CALLED at %.3f. Caller stack:\n%s",
                   time.time(), caller)
    with _lock:
        endpoint = _tunnel["cluster_endpoint"]
        port     = _tunnel.get("cluster_port", 27017)
        bastion  = _tunnel["bastion_host"]
        user     = _tunnel["ssh_user"]
        key      = _tunnel["key_path"]
        saved_instances = _tunnel.get("instances")
    if not all([endpoint, bastion, user, key]):
        return False
    # Reuse the saved instances list so per-instance (reader) tunnels are
    # restored on reconnect instead of collapsing to a writer-only tunnel.
    result = open_tunnel(bastion, user, key, endpoint, port, instances=saved_instances)
    return result.get("ok", False)


def ensure_tunnel():
    """Check health; reconnect if dead."""
    if is_tunnel_active():
        return True
    with _lock:
        has_params = bool(_tunnel["cluster_endpoint"] and _tunnel["bastion_host"])
    if not has_params:
        return False
    logger.warning("Tunnel dead — attempting auto-reconnect")
    return reconnect_tunnel()


def save_credentials(username, password, use_tls=False):
    with _lock:
        _wipe(_tunnel["_cred_user"])
        _wipe(_tunnel["_cred_pass"])
        _tunnel["_cred_user"] = _obfuscate(username)
        _tunnel["_cred_pass"] = _obfuscate(password)
        _tunnel["use_tls"] = use_tls


def _get_credentials():
    with _lock:
        u = _deobfuscate(_tunnel["_cred_user"]) if _tunnel["_cred_user"] else None
        p = _deobfuscate(_tunnel["_cred_pass"]) if _tunnel["_cred_pass"] else None
        return u, p, _tunnel["use_tls"]


def is_tunnel_active():
    with _lock:
        proc = _tunnel["process"]
        if not proc:
            logger.info("is_tunnel_active: False — no writer process tracked")
            _tunnel["active"] = False
            return False
        rc = proc.poll()
        if rc is not None:
            logger.info("is_tunnel_active: False — writer ssh process exited (returncode=%s)", rc)
            _tunnel["active"] = False
            return False
        port_ok = _is_port_in_use(BASE_LOCAL_PORT)
        if not port_ok:
            logger.info("is_tunnel_active: False — port %d not accepting connections "
                        "(ssh process alive, pid=%s)", BASE_LOCAL_PORT, proc.pid)
            return False

    # Cheap checks passed (process alive + port open). But an SSH forward keeps
    # the local listener open even when the remote leg is dead ("half-dead"
    # tunnel), so a real liveness probe is needed to catch that. The probe is
    # throttled so frequent callers don't each pay its cost.
    return _probe_tunnel_alive()


# ── Half-dead detection probe ─────────────────────────────────────────────────
_PROBE_TTL = 8.0          # seconds — cache probe result to avoid per-call cost
_PROBE_TIMEOUT_MS = 2000  # short bound so the probe can never become a new hang
_probe_cache = {"ts": 0.0, "result": True}


def _probe_tunnel_alive():
    """Return True iff a lightweight `ping` succeeds through the tunnel.

    Detects the half-dead state (TCP up, DocumentDB unreachable) that the
    process/port checks miss. Throttled via _PROBE_TTL. Fails closed (returns
    False) on any probe error so ensure_tunnel() will reconnect.
    """
    now = time.time()
    with _lock:
        if now - _probe_cache["ts"] < _PROBE_TTL:
            return _probe_cache["result"]

    u, p, tls = _get_credentials()
    alive = False
    try:
        import pymongo
        tls_q = _tls_query(tls)
        cred = f"{u}:{p}@" if u else ""
        probe_uri = (f"mongodb://{cred}localhost:{BASE_LOCAL_PORT}/?"
                     f"{tls_q}retryWrites=false&directConnection=true&appName=DocDB-Prism-probe")
        client = pymongo.MongoClient(
            probe_uri, serverSelectionTimeoutMS=_PROBE_TIMEOUT_MS,
            connectTimeoutMS=_PROBE_TIMEOUT_MS, socketTimeoutMS=_PROBE_TIMEOUT_MS)
        try:
            client.admin.command("ping")
            alive = True
        finally:
            client.close()
    except Exception as e:
        logger.info("is_tunnel_active: False — liveness probe failed (half-dead tunnel): %s",
                    str(e)[:120])
        alive = False

    with _lock:
        _probe_cache["ts"] = time.time()
        _probe_cache["result"] = alive
    return alive


def get_tunnel_info():
    with _lock:
        active = (_tunnel["active"]
                  and _tunnel["process"]
                  and _tunnel["process"].poll() is None)
        return {
            "active": active,
            "local_port": BASE_LOCAL_PORT,
            "cluster_endpoint": _tunnel["cluster_endpoint"],
            "bastion_host": _tunnel["bastion_host"],
            "pid": _tunnel["process"].pid if _tunnel["process"] else None,
            "instance_tunnels": [
                {"instance_id": t["instance_id"], "role": t["role"],
                 "local_port": t["local_port"]}
                for t in _tunnels if t["instance_id"] != "_cluster"
            ],
        }


def get_tunnel_connection_string(username, password, use_tls=False):
    """Build connection string for the primary (writer/cluster) tunnel."""
    tls = _tls_query(use_tls)
    return (f"mongodb://{username}:{password}@localhost:{BASE_LOCAL_PORT}/?"
            f"{tls}retryWrites=false&directConnection=true&appName=DocDB-Prism")
