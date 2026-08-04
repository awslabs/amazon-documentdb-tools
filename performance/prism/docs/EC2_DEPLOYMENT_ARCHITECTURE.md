# Prism EC2 Deployment Architecture

> **Status note:** This document describes a *target* architecture for a hardened,
> multi-user EC2 deployment behind ALB + Cognito. Connection details are entered on
> the auth page in the UI (on EC2, use **Direct** mode). For the working, supported
> setup today, use [`EC2_SETUP_GUIDE.md`](EC2_SETUP_GUIDE.md) and
> [`SETUP.md`](../SETUP.md).

## Overview

Deploy Prism on EC2 in the same VPC as DocumentDB, protected by ALB + Cognito authentication. Multiple team members access the tool simultaneously via HTTPS.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                              AWS VPC                                      │
│                                                                          │
│  ┌──────────┐     ┌──────────────┐     ┌──────────────┐                │
│  │ Cognito  │◄────│     ALB      │────►│   EC2        │                │
│  │ User Pool│     │ (HTTPS/443)  │     │ (port 8501)  │                │
│  └──────────┘     └──────────────┘     │              │                │
│                                         │  Prism    │                │
│                                         │  app.py      │                │
│                                         └──────┬───────┘                │
│                                                │ Direct (no tunnel)      │
│                                         ┌──────▼───────┐                │
│                                         │  DocumentDB  │                │
│                                         │  Cluster     │                │
│                                         └──────────────┘                │
└─────────────────────────────────────────────────────────────────────────┘
```

## Key Differences from Local Deployment

| Aspect | Local (current) | EC2 (target) |
|--------|----------------|--------------|
| Network path | SSH tunnel → bastion → DocDB | Direct VPC → DocDB |
| Authentication | None (localhost) | Cognito + ALB |
| Connection mode | Tunnel (localhost:27017) | Direct (cluster endpoint) |
| TLS | Optional (via tunnel) | Required (direct connection) |
| Users | Single | Multiple simultaneous |
| State | Single-user globals | Shared state (acceptable for same cluster) |

## Changes Required

### 1. Connection Mode — Remove Tunnel Dependency

**Impact:** Low — the "direct" connection mode already exists in the code.

On EC2, the tool connects directly using the cluster endpoint:
```
mongodb://user:pass@docdb-80.cluster-xyz.us-east-1.docdb.amazonaws.com:27017/?tls=true&tlsCAFile=/opt/prism/global-bundle.pem&replicaSet=rs0&retryWrites=false
```

**Changes needed:**
- Default connection mode to "direct" when running on EC2
- Pre-install `global-bundle.pem` on the EC2 instance
- The auth page can skip the SSH tunnel option (or hide it via config flag)

### 2. Multi-User Shared State — Acceptable with Same Cluster

**Impact:** None for code — if all users work on the same cluster.

When multiple users connect to the same cluster:
- Agent runs once, all users see the same analysis results ✓
- Slow query cache is shared ✓  
- Well-Architected checks are shared ✓
- Activity view fetches live data per-request ✓

**When it breaks:** If User A connects to cluster-1 and User B connects to cluster-2 — the global state gets overwritten. 

**Mitigation options (choose one):**
- **Option A (simplest):** Lock the tool to one cluster via environment variable. No cluster selection UI needed.
- **Option B:** Allow cluster switching but show a warning ("Switching cluster will reset analysis for all users").
- **Option C (full isolation):** Session-keyed state (major refactor — not recommended for initial deployment).

**Recommendation:** Option A for initial EC2 deployment. One instance = one cluster.

### 3. Authentication — Cognito + ALB

**Impact:** Infrastructure only — no code changes to the Dash app.

ALB handles authentication before traffic reaches the app:
1. Create Cognito User Pool with team members
2. Create ALB with HTTPS listener (ACM certificate)
3. Add authentication action rule on ALB listener → Cognito
4. Only authenticated requests forward to EC2 target group

**ALB listener rule:**
```
IF path == /* 
THEN authenticate-cognito (user pool, app client, session cookie)
THEN forward to target group
```

The Dash app never sees unauthenticated requests.

### 4. Database Credentials — Server-Side Only

**Impact:** Already done (security fix #1 in this session).

Connection string is stored server-side in `_conn_state` after you enter credentials
on the auth page — it is never sent to the browser.

### 5. Startup Configuration

Connection details are entered on the auth page in the UI. The env vars the app reads
are `PRISM_PORT`, `PRISM_HOST`, `PRISM_WORKERS`, and `PRISM_DEBUG`:

```bash
PRISM_PORT=8501
PRISM_HOST=0.0.0.0
PRISM_WORKERS=1
```

On EC2, set Connection Mode to **Direct** on the auth page (no bastion needed in-VPC).

### 6. Security Hardening for EC2

| Item | Implementation |
|------|----------------|
| HTTPS | ALB with ACM certificate (no self-signed) |
| Security Group (ALB) | Inbound 443 from VPN/office CIDR only |
| Security Group (EC2) | Inbound 8501 from ALB SG only |
| IAM Role | EC2 instance role with `iam_policy.json` permissions |
| No SSH to EC2 | Use SSM Session Manager for admin access |
| Debug off | `PRISM_DEBUG` not set (defaults to False) |
| Disk encryption | EBS encrypted |
| Auto-updates | yum-cron for security patches |

### 7. Running the Service

**Supported today (nohup):** use the provided script — it runs preflight, launches
gunicorn with **1 worker** bound to `0.0.0.0:8501`, and writes a fresh timestamped
log to `logs/` on each start:

```bash
cd /home/ec2-user/prism
./start_server.sh     # start (prints public-IP URL, verifies health)
./stop_server.sh      # stop
```

nohup does not survive reboots — re-run `start_server.sh` after a reboot.

**Optional systemd alternative (corrected).** For reboot-safe auto-restart. Note it
runs **gunicorn** (not `python app.py`, which has no `--port` flag), uses **1 worker**,
and the existing `ec2-user` account with the real working directory:

```ini
[Unit]
Description=Prism DocumentDB Agent
After=network.target

[Service]
Type=simple
User=ec2-user
WorkingDirectory=/home/ec2-user/prism
Environment=PRISM_PORT=8501
Environment=PRISM_HOST=0.0.0.0
ExecStart=/usr/bin/python3.11 -m gunicorn app:server --bind 0.0.0.0:8501 --workers 1 --timeout 120 --graceful-timeout 30
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

> `ProtectHome=yes` would break a `/home/ec2-user` working dir; omit it or relocate
> the app to `/opt/prism` (and `chown ec2-user`) if you want strict hardening.

## Code Changes Summary

| File | Change | Effort |
|------|--------|--------|
| None | Infrastructure: ALB, Cognito, Security Groups, IAM role | Medium (one-time) |

## What Does NOT Change

- All analysis logic (agent, WA, slow queries, indexes, compression)
- All UI/rendering code
- The AI advisor
- Code review module
- Report generation

## Multi-User Considerations

For same-cluster usage (recommended initial deployment):
- Agent runs once on startup (or on first request) — results are shared
- Cache persists in `/opt/prism/.prism_cache/`
- Multiple users can view analysis simultaneously without conflict
- The AI advisor chat is NOT shared (module-level state would need per-user isolation)
- Activity tab fetches live data each time — no conflict

**Chat isolation gap:** The chat advisor stores conversation history in `_chat` dict (module-level). Multiple users would see each other's chat. Fix: either accept it (team tool) or scope chat to browser session via `dcc.Store`.

## Deployment Steps

1. Launch EC2 in DocumentDB VPC (private subnet)
2. Install Python 3, pip, clone code
3. Install deps: `pip install -r requirements.txt`
4. Download CA bundle: `wget -O /opt/prism/global-bundle.pem https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem`
5. (Connection details are entered on the auth page after launch — Direct mode on EC2)
6. Create systemd service, enable, start
7. Create Cognito User Pool + App Client
8. Create ALB with HTTPS listener + Cognito auth rule
9. Point ALB target group to EC2:8501
10. Create DNS record (Route 53) → ALB
11. Add team members to Cognito User Pool
