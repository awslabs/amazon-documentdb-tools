# Prism EC2 Setup Guide — Amazon Linux 2023

Complete guide to deploy Prism on a fresh Amazon Linux EC2 instance with direct VPC access to DocumentDB.

> **Prerequisites & inputs:** see [`SETUP.md`](../SETUP.md) for the
> authoritative list (tools, versions, IAM, security groups, DocumentDB reachability).
>
> **Fast path:** once the code is on the instance, run `./scripts/setup_ec2.sh`
> (installs deps + CA bundle, runs preflight), then `./start_server.sh`. The manual
> steps below explain what that script automates and cover the optional ALB/Cognito setup.

---

## Prerequisites

| Requirement | Details |
|-------------|---------|
| **EC2 Instance** | t3.medium minimum (2 vCPU, 4GB RAM). r-class recommended for large clusters. |
| **AMI** | Amazon Linux 2023 (al2023-ami-*) |
| **VPC** | Same VPC as your DocumentDB cluster |
| **Subnet** | Private subnet with route to DocumentDB |
| **Security Group** | Allow inbound 8501 (from ALB or your IP), outbound 27017 (to DocumentDB), outbound 443 (to AWS APIs) |
| **IAM Role** | Attached to EC2 instance (see IAM section below) |
| **Bedrock Access** | Model access enabled for Claude Sonnet 4 and Haiku 4.5 in us-east-1 |

---

## Step 1: Launch EC2 Instance

```bash
# Launch via CLI (or use console)
aws ec2 run-instances \
  --image-id ami-0c101f26f147fa7fd \
  --instance-type t3.medium \
  --subnet-id subnet-YOUR_PRIVATE_SUBNET \
  --security-group-ids sg-YOUR_SG \
  --iam-instance-profile Name=PrismRole \
  --key-name your-key-pair \
  --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=Prism}]'
```

---

## Step 2: System Dependencies

`./scripts/setup_ec2.sh` installs all of this for you. To do it manually, SSH into the instance and run:

```bash
# Update system
sudo dnf update -y

# Python 3.11+ and development tools (needed for lz4/zstd native compilation)
sudo dnf install -y python3.11 python3.11-pip python3.11-devel gcc git

# SSH client (for tunnel mode — already installed on AL2023)
which ssh  # should exist
```

> The TLS CA bundle (`global-bundle.pem`) is fetched automatically by
> `setup_ec2.sh` (and `setup_local.sh`) into the project root. To fetch it
> manually: `curl -fsSL -o global-bundle.pem https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem`

---

## Step 3: Deploy Application

```bash
# Copy the application to the instance (choose one):
#   git clone <YOUR_REPO_URL> /home/ec2-user/prism
#   rsync -avz prism-dash/ ec2-user@<IP>:/home/ec2-user/prism/

cd /home/ec2-user/prism

# One-shot setup: installs deps + gunicorn + CA bundle, then runs preflight.
./scripts/setup_ec2.sh

# Optional — also test the DB network path during preflight:
PRISM_CHECK_ENDPOINT=<cluster-endpoint>:27017 ./scripts/setup_ec2.sh
```

`setup_ec2.sh` does NOT start the server. It ends once preflight passes; you start
it yourself in Step 7.

---

## Step 4: Configuration

### Environment Variables (optional)

Prism runs with sensible defaults; environment variables are optional overrides.
They can be exported in your shell or placed in the instance's environment.

```bash
PRISM_PORT=8501          # listen port (default 8501)
PRISM_HOST=0.0.0.0       # bind address (default 0.0.0.0 — reachable via public IP)
PRISM_WORKERS=1          # gunicorn workers (KEEP AT 1 — see note below)
PRISM_DEBUG=0            # 0 for production
AWS_DEFAULT_REGION=us-east-1
```

> **Connection details (cluster, DB user/password, region, bastion) are entered on
> the auth page in the UI — not via environment variables.** The `DOCDB_*` /
> `PRISM_MODE` variables referenced in older drafts are not implemented; see
> [`SETUP.md`](../SETUP.md) (Notes).

> **Workers = 1 is required**, not a default to tune up. The autonomous agent keeps
> analysis state in-process; multiple workers would fragment it.

### Lazy Load Config

The file `prism_config.yaml` controls analysis behavior. Default settings are fine for most clusters. Key tunables:

```yaml
agent_prioritisation:
  max_databases_to_analyse: 20    # Increase if cluster has >20 databases
  skip_databases:
    - staging
    - local
    - admin
    - config
```

---

## Step 5: IAM Role

Prism defines its required permissions in one place —
[`iam_policy.json`](../iam_policy.json) in the repo root. Attach that file to the EC2
instance role:

```bash
aws iam put-role-policy \
  --role-name <your-ec2-role> \
  --policy-name PrismPolicy \
  --policy-document file://iam_policy.json
```

No AWS access keys needed — boto3 uses the instance profile automatically.

---

## Step 6: Security Group Rules

### EC2 Security Group (sg-prism)

| Direction | Port | Source/Dest | Purpose |
|-----------|------|-------------|---------|
| Inbound | 8501 | ALB SG or your IP | App access |
| Outbound | 27017 | DocumentDB SG | Database connection |
| Outbound | 443 | 0.0.0.0/0 | AWS APIs (CloudWatch, Bedrock, RDS) |

### DocumentDB Security Group

| Direction | Port | Source | Purpose |
|-----------|------|--------|---------|
| Inbound | 27017 | sg-prism | Allow Prism to connect |

---

## Step 7: Run the Server

Prism runs under gunicorn via `start_server.sh` (nohup; no systemd required).
The script runs preflight first and refuses to start on any hard failure.

```bash
cd /home/ec2-user/prism
./start_server.sh
```

What it does:
- Runs `scripts/preflight.sh` (HARD failures block the start).
- Launches `gunicorn app:server --bind 0.0.0.0:8501 --workers 1`.
- Writes a **new timestamped log** to `logs/prism_<timestamp>.log` on each start.
- Auto-detects the instance public IP (IMDSv2) and prints the URL to open.
- Verifies the HTTP endpoint is healthy before reporting success.

Stop it with:

```bash
./stop_server.sh
```

> **Reboot behavior:** nohup does not survive a reboot. Re-run `./start_server.sh`
> after a reboot, or wrap it with your own process manager if you need auto-restart.

---

## Step 8: Verify

The `start_server.sh` health check already confirms the app is up. To independently
verify AWS and DocumentDB connectivity:

```bash
# App responding
curl -fsS http://localhost:8501/ >/dev/null && echo "App: OK"

# AWS connectivity + cluster discovery
python3.11 -c "
import boto3
clusters = boto3.client('docdb', region_name='us-east-1').describe_db_clusters()['DBClusters']
print(f'Found {len(clusters)} DocumentDB clusters')
for c in clusters: print('  -', c['DBClusterIdentifier'], c['Status'])
"

# DocumentDB TCP reachability (catches security-group/subnet issues)
PRISM_CHECK_ENDPOINT=<cluster-endpoint>:27017 ./scripts/preflight.sh
```

---

## Step 9: ALB + HTTPS (Production)

For team access with authentication:

```bash
# 1. Create Target Group
aws elbv2 create-target-group \
  --name prism-tg \
  --protocol HTTP \
  --port 8501 \
  --vpc-id vpc-YOUR_VPC \
  --health-check-path "/" \
  --target-type instance

# 2. Register EC2 instance
aws elbv2 register-targets \
  --target-group-arn arn:aws:elasticloadbalancing:... \
  --targets Id=i-YOUR_INSTANCE

# 3. Create ALB with Cognito auth (see EC2_DEPLOYMENT_ARCHITECTURE.md for details)
```

---

## File Structure on EC2

```
/home/ec2-user/prism/
├── app.py                        # Entry point (server = app.server)
├── requirements.txt              # Python deps
├── prism_config.yaml         # Analysis config
├── iam_policy.json               # IAM reference
├── global-bundle.pem             # DocumentDB TLS CA bundle (fetched by setup script)
├── start_server.sh / stop_server.sh
├── scripts/                      # setup_ec2.sh, setup_local.sh, preflight.sh, lib_common.sh
├── logs/                         # New timestamped log per start (auto-created)
├── .prism_cache/              # Persistent analysis cache (auto-created)
├── agent_orchestrator.py         # Autonomous agent loop
├── analyzers/                    # Plugin analyzers
├── wa_checks/                    # Well-Architected checks
├── tabs/                         # UI tab modules
├── assets/                       # CSS, JS, images
├── documentdb-advisor/           # SKILL.md + reference docs
└── prompts/                      # Prompt templates
```

---

## Troubleshooting

| Problem | Check |
|---------|-------|
| `ModuleNotFoundError` | `pip3 install --user -r requirements.txt` |
| `lz4` build fails | `sudo dnf install -y python3-devel gcc` |
| Can't reach DocumentDB | Security group, subnet, VPC peering |
| Bedrock 403 | Enable model access in Bedrock console, check IAM |
| Slow startup | Normal — first run loads SKILL.md and warms caches |
| Cache permission denied | `chown -R ec2-user /home/ec2-user/prism/.prism_cache` |
| Port 8501 already in use | `lsof -i :8501` or change `PRISM_PORT` |
| Not reachable from browser | Confirm app binds `0.0.0.0` (default) and the SG allows inbound 8501 from your IP. `python app.py` also binds 0.0.0.0 now. |

---

## Quick Start (Copy-Paste)

```bash
# On a fresh Amazon Linux 2023 EC2 in the same VPC as DocumentDB,
# with an instance role carrying iam_policy.json:

# 1. Get the code
git clone YOUR_REPO /home/ec2-user/prism
cd /home/ec2-user/prism

# 2. One-shot setup: system deps + Python deps + gunicorn + CA bundle + preflight
./scripts/setup_ec2.sh

# 3. Start (you start it — setup does not). Prints the public-IP URL.
./start_server.sh
# → open the printed http://<public-ip>:8501  (SG must allow inbound 8501)

# 4. Stop when done
./stop_server.sh
```

> Connection details (cluster, DB credentials, region, bastion) are entered on the
> auth page in the UI after the app is up — not via config files or env vars.
> See [`SETUP.md`](../SETUP.md) for the full requirement list.
