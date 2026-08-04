# Prism — Setup & Installation Guide

The single source of truth for getting Prism running — prerequisites, installation,
verification, and how to start the server. For *what Prism does* and how to use it
once running, see the [README](README.md).

> **Setup never starts the server.** The setup scripts install and verify; you start the
> server yourself once preflight passes.



## Contents
|  |  |
|---|---------|
| 1 | [Choose your environment](#1-choose-your-environment) |
| 2 | [Prerequisites](#2-prerequisites) |
| 3 | [Installation](#3-installation) |
| | &nbsp;&nbsp;↳ [Get the code](#get-the-code) |
| | &nbsp;&nbsp;↳ [EC2 (Amazon Linux 2023)](#ec2-amazon-linux-2023) |
| | &nbsp;&nbsp;↳ [Local — macOS / Linux](#local--macos--linux) |
| | &nbsp;&nbsp;↳ [Local — Windows](#local--windows) |
| 4 | [Run the server](#4-run-the-server) |
| | &nbsp;&nbsp;↳ [For EC2](#for-ec2) |
| | &nbsp;&nbsp;↳ [For macOS / Linux](#for-macos--linux) |
| | &nbsp;&nbsp;↳ [For Windows](#for-windows) |
| | &nbsp;&nbsp;↳ [Preflight verification](#preflight-verification) |
| 5 | [Create your first user](#5-create-your-first-user) |
| 6 | [Connect](#6-connect) |
| | &nbsp;&nbsp;↳ [On EC2 — use Direct](#on-ec2--use-direct) |
| | &nbsp;&nbsp;↳ [On local (macOS / Windows)](#on-local-macos--windows) |
| 7 | [Troubleshooting](#7-troubleshooting) |



## 1. Choose your environment

| You are running on… | Installation | Server |
|---------------------|--------------|--------|
| **EC2** with network access to DocumentDB | [§3 → EC2](#3-installation) | gunicorn |
| **macOS / Linux laptop** | [§3 → Local macOS / Linux](#3-installation) | `python app.py` (dev) or gunicorn |
| **Windows laptop** | [§3 → Local Windows](#3-installation) | `python app.py` (dev only) |

Pick one and follow it top to bottom. All paths end at [§4 Run the server](#4-run-the-server)
(which runs preflight verification first).

---

## 2. Prerequisites

| Requirement | Notes |
|-------------|-------|
| **Amazon DocumentDB cluster + credentials** | A running DocumentDB cluster you can reach, plus a database username and password. You select the cluster and enter the credentials on the auth page (never in a config file). |
| **Python 3.11+** | 3.11 specifically on EC2 (Amazon Linux 2023 ships 3.9). macOS/Windows install prebuilt wheels — no compiler needed. |
| **AWS CLI** (local only) | The simplest way to configure local credentials (`aws configure`). Not used on EC2 — the instance role supplies credentials. |
| **Build tools** (`gcc`, `python3.11-devel`) | Only on EC2/Linux, to compile the native `lz4` / `zstd` wheels. Installed by `setup_ec2.sh`. |
| **`ssh` client + `.pem` key** | Only for private-cluster SSH-tunnel mode. |
| **Bastion host / jump EC2** | Only for SSH-tunnel mode.<br>• A reachable host (typically an EC2 in the cluster's VPC) that you can SSH into and that can reach DocumentDB on TCP 27017.<br>• Provide its host/IP, SSH user, and `.pem` key on the auth page.<br>• For local mode you need such a reachable host (typically an EC2 in the cluster's VPC).<br>• Not needed for Direct mode (e.g. running on an in-VPC EC2). |
| **AWS credentials + IAM permissions** | Prism authenticates via the standard AWS credential chain, and that identity must carry the policy in [`iam_policy.json`](iam_policy.json) (read-only `Describe`/`List`/`Get` except `bedrock:InvokeModel`). **Local:** `aws configure` / `AWS_PROFILE` for an IAM user that has the policy attached. **EC2:** an instance role with the policy attached (no access keys needed). |
| **Amazon Bedrock model access** (in your region) | `anthropic.claude-sonnet-4-20250514-v1:0` (primary), `anthropic.claude-haiku-4-5-20251001-v1:0` (fallback). |

---

## 3. Installation

First [get the code](#get-the-code), then follow the subsection for your environment, and
finally go to [§4 Run the server](#4-run-the-server).

### Get the code

Clone the repository and change into the Prism directory. All commands in the
environment subsections below assume you are in this directory.

```bash
git clone https://github.com/kaarthiik-aws/amazon-documentdb-tools.git
cd amazon-documentdb-tools/performance/prism
```

> The same two commands work on macOS, Linux, and Windows (PowerShell). On Windows, run
> them from a shell that has `git` available.

### EC2 (Amazon Linux 2023)

Run Prism on an instance that has a network route to the cluster.

> **Before you start:** ensure the EC2 instance has an **IAM role attached that carries
> the [`iam_policy.json`](iam_policy.json) permissions**. Prism uses the instance role
> for all AWS calls — without it, cluster discovery, CloudWatch, and Bedrock will fail
> with `AccessDenied`.

```bash
# from amazon-documentdb-tools/performance/prism (see "Get the code" above)
./scripts/setup_ec2.sh      # installs deps + gunicorn + CA bundle, then runs preflight

# Optional: probe the DB network path during setup (catches SG / subnet / VPC issues)
PRISM_CHECK_ENDPOINT=<cluster-endpoint>:27017 ./scripts/setup_ec2.sh
```

**Instance baseline**

| Item | Requirement |
|------|-------------|
| AMI | Amazon Linux 2023 (`al2023-ami-*`) |
| Type | `t3.xlarge` (4 vCPU / 16 GB) minimum; larger for big fleets |
| Network | A network route to Amazon DocumentDB — the instance must reach the cluster endpoint on TCP 27017 |
| IAM | Instance role carrying [`iam_policy.json`](iam_policy.json) |
| Public IP | To reach the UI through the public internet. `start_server.sh` auto-detects prints the URL. |

**Security-group rules**

| SG | Direction | Port | Source / Dest | Purpose |
|----|-----------|------|---------------|---------|
| EC2 | Inbound | 8501 | Your IP / ALB | App UI access |
| EC2 | Outbound | 27017 | DocumentDB SG | Database connection |
| EC2 | Outbound | 443 | `0.0.0.0/0` | AWS APIs (CloudWatch, Bedrock, RDS) |
| DocumentDB | Inbound | 27017 | EC2 instance SG | Allow Prism to connect |

For an ALB + Cognito (HTTPS, team access) front end, see
[`docs/EC2_SETUP_GUIDE.md`](docs/EC2_SETUP_GUIDE.md).

### Local — macOS / Linux

```bash
# from amazon-documentdb-tools/performance/prism (see "Get the code" above)
pip install -r requirements.txt
./scripts/setup_local.sh    # verifies the environment, fetches the TLS CA bundle
```

`setup_local.sh` is **check-and-instruct** — it never installs system packages or changes
your machine. If anything is missing it prints the exact command to fix it.

### Local — Windows

Windows is supported for local use via the built-in dev server.

```powershell
# from amazon-documentdb-tools\performance\prism (see "Get the code" above)
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
aws configure                      # if not already configured

# TLS connections only: download the DocumentDB CA bundle into the app folder
Invoke-WebRequest -Uri https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem -OutFile global-bundle.pem
```

Windows notes:

- **Dev server only.** `gunicorn`, `start_server.sh` / `stop_server.sh`, and
  `scripts/*.sh` (including preflight) need a POSIX shell. To run them, use **WSL** or
  **Git Bash**; otherwise use `python app.py`.
- **OpenSSH client** is required only for SSH-tunnel mode (enable via *Settings → Optional
  features → OpenSSH Client*, or `Add-WindowsCapability -Online -Name OpenSSH.Client~~~~0.0.1.0`);
  its orphaned-tunnel cleanup is POSIX-only, so close a stale tunnel manually if one is
  left after a restart.
- **Quick dependency check** (stand-in for preflight):
  `python -c "import dash, pymongo, boto3, lz4, zstandard; print('ok')"`.

---

## 4. Run the server

### For EC2

```bash
./start_server.sh    # runs preflight, then gunicorn (1 worker), binds 0.0.0.0:8501,
                     # writes logs/prism_<timestamp>.log, prints the URL
./stop_server.sh     # stop
```

One worker by design — the agent keeps analysis state in-process, so multiple workers
would fragment it. A new timestamped log is written to `logs/` on each start.

### For macOS / Linux

Use the `start_server.sh` launcher or the dev server:

```bash
./start_server.sh    # gunicorn, same as EC2 (runs preflight, logs to logs/)
# or
python app.py        # dev server → http://localhost:8501
```

### For Windows

Dev server only — `gunicorn` and `start_server.sh` are POSIX-only:

```powershell
python app.py        # http://localhost:8501
```

**Env overrides (all platforms):** `PRISM_PORT` (8501), `PRISM_HOST` (0.0.0.0),
`PRISM_WORKERS` (1), `PRISM_DEBUG` (0). On Windows/PowerShell, set them first, e.g.
`$env:PRISM_PORT=9000; python app.py`.

### Preflight verification

`start_server.sh` runs `scripts/preflight.sh` automatically before starting the server;
you can also run it on its own. It classifies checks into two tiers:

- **HARD (blocks start):** Python 3.11+, required Python packages, gunicorn (server mode),
  TLS CA bundle, port free.
- **SOFT (warns, still starts):** AWS credentials, Bedrock access, `ssh` client, and
  DocumentDB TCP reachability.

```bash
./scripts/preflight.sh                                       # server-mode checks
./scripts/preflight.sh --mode dev                            # dev mode (gunicorn is a warning)
PRISM_CHECK_ENDPOINT=<host>:27017 ./scripts/preflight.sh  # also TCP-probe the cluster
```

A **hard failure stops the start and prints the exact fix command.** A pass with warnings
lists the soft items (for example, credentials you'll enter in the UI) and still lets you
start. So "checks passed" never hides a missing essential.

---

## 5. Create your first user

Prism has a local application login gate (separate from your DocumentDB credentials).
Before you can sign in, create at least one user:

```bash
python3 create_user.py add <username>      # prompts for password (min 12 characters)
# or non-interactive:
python3 create_user.py add admin --password "YourSecurePass12"
```

Other commands: `python3 create_user.py list`, `passwd <user>`, `delete <user>`.

> **EC2 note:** `setup_ec2.sh` makes `python3` point to Python 3.11 via `alternatives`.
> If you skipped the setup script, use `python3.11` explicitly.

> **Sessions:** After sign-in, your session remains active for 15 minutes of inactivity
> (sliding window — any interaction resets the timer). Sessions persist across server
> restarts as long as the same secret key is used.

**Production security (optional but recommended):**

| Env var | Purpose | Default if unset |
|---------|---------|-----------------|
| `PRISM_SECRET_KEY` | Flask session signing key. Set to a random 64-char hex string (`python -c "import secrets; print(secrets.token_hex(32))"`). Prevents session forgery if the auth DB file leaks. | Auto-generated and stored in the auth DB. |
| `PRISM_DB_KEY` | AES-256 encryption key for the auth database. If unset, auto-generated and stored in `.prism_db.key` (chmod 600). Set explicitly when restoring the DB to a different host. | Auto-managed via `.prism_db.key` file. |

> On EC2, you can set these in `/etc/environment` or export them in a wrapper script
> before `./start_server.sh`. For higher assurance, store them in AWS Secrets Manager
> or SSM Parameter Store and fetch at boot.

---

## 6. Connect

Open the URL printed at startup. Sign in with the credentials from step 5, then on the
auth page complete these steps:

1. **Region** — choose the AWS region of your cluster.
2. **Cluster** — pick it from the discovered list.
3. **Connection Mode** — choose **Direct** or **SSH Tunnel** (see below).
4. **DB credentials** — enter the DocumentDB username and password; set TLS on/off.

Then click **Connect & Load Databases**.

### On EC2 — use Direct

- Set **Connection Mode → Direct**.
- The instance reaches the cluster endpoint directly over the VPC — no bastion needed.
- Enter your DB credentials and connect.

> The toggle **defaults to SSH Tunnel**, so you must switch it to **Direct** on EC2.
> Leaving it on SSH Tunnel tries to open a bastion connection you don't have, and the
> connect fails.

### On local (macOS / Windows)

- **Private cluster** (not reachable from your laptop) → use **SSH Tunnel** and provide:
  - the bastion host IP,
  - the SSH user (default `ec2-user`),
  - the path to your `.pem` key.

Full walkthrough of the UI is in the [README User Guide](README.md#user-guide).

---

## 7. Troubleshooting

| Symptom | Cause / fix |
|---------|-------------|
| `numpy.dtype size changed … binary incompatibility` on import | `numpy 2.x` with `pandas 2.0.3` (built against numpy 1.x). `requirements.txt` pins `numpy<2.0.0`; if it slipped through, run `pip install 'numpy>=1.24.0,<2.0.0'`. |
| `ModuleNotFoundError` | `pip install -r requirements.txt` for the **same** interpreter gunicorn uses (3.11 on EC2). |
| `lz4` / `zstandard` build fails | Install build tools: `sudo dnf install -y gcc python3.11-devel`. |
| Preflight: TLS CA bundle missing | `curl -fsSL -o global-bundle.pem https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem` (setup scripts do this for you). |
| Port 8501 already in use | Set `PRISM_PORT`, or find the holder: `lsof -iTCP:8501 -sTCP:LISTEN`. |
| Bedrock `AccessDenied` / 403 | Enable model access in the Bedrock console **and** confirm `bedrock:InvokeModel` in [`iam_policy.json`](iam_policy.json). |
| `rds:Describe*` / other `AccessDenied` | The role is missing a permission — re-apply the current [`iam_policy.json`](iam_policy.json) (it is the complete, authoritative set). |
| Can't reach DocumentDB | Security groups (inbound 27017 from the app host), subnet route, VPC peering, DNS. Confirm with `PRISM_CHECK_ENDPOINT=<host>:27017 ./scripts/preflight.sh`. |
| App unreachable on EC2 over public IP | The app binds `0.0.0.0`; confirm the EC2 SG allows inbound TCP 8501 from your IP. `python app.py` without `PRISM_HOST` still binds `0.0.0.0` by default. |
