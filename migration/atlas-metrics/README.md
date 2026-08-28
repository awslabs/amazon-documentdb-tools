# Atlas Metrics Collector -- User Guide

Collects capacity metrics from your MongoDB Atlas cluster (or self-managed MongoDB on EC2) and produces a sizing package for migrating to Amazon DocumentDB.

**Location:** part of [amazon-documentdb-tools](https://github.com/awslabs/amazon-documentdb-tools) at `migration/atlas-metrics/`.
**Current version:** 2.3.1 (see CHANGELOG.md)

## Which source?

The tool supports two source topologies via `--source`:

| Source | Use for | Auth needed |
|---|---|---|
| **`atlas`** (default) | MongoDB Atlas clusters | Atlas API key + DB user |
| **`ec2`** | Self-managed MongoDB running on Amazon EC2 | DB user + AWS credentials (CloudWatch + EC2 read) |

Existing v2.0.x Atlas invocations require no changes - `--source` defaults to `atlas`. For self-managed MongoDB on EC2, add `--source ec2` and skip the Atlas API setup below (jump to the ec2 section instead).

## Prerequisites

You will need:

1. **Atlas API key** with `Project Read Only` role (minimum)
2. **Atlas Project ID** (24-character hex string, from the Atlas UI URL)
3. **Atlas Database User** with `atlasAdmin` role (recommended) OR `clusterMonitor` + `readAnyDatabase` roles
4. **MongoDB connection URI** for the target cluster
5. **Python 3.9+**
6. **`git`** (required if using `--compat` - the tool auto-clones amazon-documentdb-tools). Install with `yum install git`, `apt install git`, or `brew install git`.
7. **Network access** - your machine's public IP must be on the Atlas Network Access list for the project

### Why `atlasAdmin` (or `clusterMonitor` + `readAnyDatabase`)?

The tool runs `serverStatus`, `collStats`, and `$indexStats` to gather sizing data. `readWriteAnyDatabase` alone is not sufficient - the preflight will fail fast with a clear error. Grant one of the recommended roles in Atlas UI → Project → Security → Database Access.

## Step 1: Create an Atlas API Key

1. Log in to [cloud.mongodb.com](https://cloud.mongodb.com)
2. Select your project from the top-left dropdown
3. Go to **Access Manager** (left sidebar) → **API Keys**
4. Click **Create API Key**
5. Set the role to **Project Read Only**
6. Copy the **Public Key** and **Private Key** (you won't see the private key again)
7. Click **Add Access List Entry** and add your current public IP

## Step 2: Find Your Project ID

Your Project ID is the 24-character hex string in the Atlas URL:

```
https://cloud.mongodb.com/v2/abcdef0123456789abcdef01#/clusters
                              ^^^^^^^^^^^^^^^^^^^^^^^^
                              This is your Project ID
```

Also available in **Project Settings** (gear icon in the left sidebar).

## Step 3: Grant Database User Permissions

Ensure the DB user in your connection URI has `atlasAdmin` role:

1. In Atlas: **Security** → **Database Access** → find your user → **Edit**
2. Under **Built-in Role**, select `atlasAdmin`
3. Save

Alternate: assign `clusterMonitor` + `readAnyDatabase` if you prefer minimum privilege.

## Step 4: Get Your MongoDB Connection URI

1. In Atlas, click **Connect** on your cluster
2. Choose **Drivers**
3. Copy the connection string (looks like `mongodb+srv://user:pass@cluster.xxxxx.mongodb.net/`)
4. Replace `<password>` with the DB user's password
5. If your password has special characters (`@`, `:`, `/`, `?`), percent-encode them

## Step 5: Install the Tool

```bash
# Clone amazon-documentdb-tools (this tool lives under migration/atlas-metrics)
git clone https://github.com/awslabs/amazon-documentdb-tools.git
cd amazon-documentdb-tools/migration/atlas-metrics

# Install Python dependencies (isolated environment recommended)
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Step 6: Set Environment Variables

```bash
export ATLAS_PUBLIC_KEY="your-public-key"
export ATLAS_PRIVATE_KEY="your-private-key"
export ATLAS_PROJECT_ID="your-24-char-project-id"
```

## Step 7: Run the Assessment

### Standard sizing run (recommended)

```bash
python3 atlas_metrics.py --all \
  --cluster my-cluster-name \
  --uri "mongodb+srv://user:pass@cluster.xxxxx.mongodb.net/" \
  --compat
```

This collects everything: 14 days of Atlas API metrics at 5-minute granularity, per-collection sizing analysis, index review, compatibility check, and produces a Cost Estimator CSV.

### Debug / quick check

```bash
python3 atlas_metrics.py \
  --cluster my-cluster-name \
  --uri "mongodb+srv://user:pass@cluster.xxxxx.mongodb.net/"
```

Runs at 1-minute granularity over 48 hours (default). Faster (~5 min).

## Preflight Checks

The tool runs 5 preflight gates before starting the long collection. This prevents 30+ minutes of wasted time on misconfigurations:

| Gate | What it checks |
|------|----------------|
| [1/5] | Atlas API credentials work + list clusters in project |
| [2/5] | `--cluster` exists in project. Detects PAUSED clusters and typos. Suggests corrections. |
| [3/5] | `--uri` reachable, auth works, DB user role sufficient (probes `serverStatus`). |
| [4/5] | `--uri` connects to the same cluster as `--cluster`. Cross-checks SHARDED/REPLICASET topology. |
| [5/5] | If `--compat`, MongoDB version >= 5.0. |

All errors include the current public IP and remediation steps.

## Options

| Flag | Required | Description |
|------|----------|-------------|
| `--uri URI` | **Yes** | MongoDB connection string |
| `--cluster NAME` | **Yes** | Atlas cluster name |
| `--all` | No | Standard sizing run: 14 days at 5-minute granularity |
| `--compat` | No | Run DocumentDB 8.0 compatibility check (requires MongoDB 5.0+) |
| `--granularity` | No | PT10S, PT1M (default), PT5M, PT1H, or P1D |
| `--period` | No | ISO 8601 duration (P2D default). Warns if exceeds retention. |
| `--percentile` | No | Sizing percentile: 90, 95 (default), or 99 |
| `--output` | No | Output directory (auto-generated if omitted) |

## Data Retention Per Granularity

Atlas retains monitoring data for different windows based on granularity:

| Granularity | Retention | Recommended --period |
|-------------|-----------|----------------------|
| PT10S | ~24 hours (M40+ only) | P1D |
| PT1M | ~14 days | P2D (kept small for debugging speed) |
| PT5M | ~14 days | P14D |
| PT1H | ~12 months | P365D |
| P1D | effectively forever | P730D |

If you set `--period` beyond retention, the tool prints a WARNING but proceeds. Beyond the window, Atlas silently downsamples to hourly rollups presented as fine-grained buckets - biasing P95 estimates low.

## Sharded Cluster Support

Sharded Atlas clusters are supported via mongos-aggregated `collStats`. The tool connects to the mongos router (via the standard SRV URI) and iterates the `shards` sub-document returned per collection.

No special network configuration is required - the tool works from any Atlas network path (public URI, PrivateLink, VPC Peering). Direct-shard connections are NOT used because MongoDB itself rejects `collStats` on sharded collections when connected directly to a shard.

Per-shard data captured:
- Document count, size, storage size, average object size
- Number of indexes, total index size
- WiredTiger cache pages/bytes read
- WiredTiger cursor stats (inserts, updates, removes, searches)
- `$indexStats` accesses per shard
- Live compression sampling (via mongos routing)
- Live index-key-type sampling (via mongos routing)

Output file `collstats.json` has `data_source: "mongos_aggregated"` for sharded clusters.

## What Gets Collected

### From Atlas Admin API (automatic)

Collected for each node in your cluster:

- **CPU:** user utilization
- **Memory:** system used/free, resident (working set)
- **Operations:** insert, query, update, delete, getmore, cmd per second
- **Disk I/O:** read/write IOPS, latency, queue depth
- **Storage:** data size, storage size, index size
- **Connections:** current count
- **Query efficiency:** scan-and-order operations, read/write/command execution times
- **Cache:** WiredTiger cache bytes read/written, dirty bytes, used bytes
- **Replication:** oplog rate

### From Direct MongoDB Connection

Collected by connecting to your cluster and running database commands:

- **Per-collection operations:** insert, update, delete, search counts from WiredTiger cursor stats
- **Per-collection working set:** percentage of data actively accessed
- **Index analysis:** unused indexes (0 accesses) and redundant indexes (prefix subsets)
- **Compression sampling:** real Zstandard-3 with 100-doc dictionary training (matches DocumentDB 8.0 behavior)
- **Index key type sampling:** first-50 + last-50 documents per collection, sampled for actual key sizes (compound, hashed, multikey)
- **Cost Estimator CSV:** all fields auto-populated, ready to upload to the DocumentDB Calculator

### From Compat-Tool (requires `--compat`)

Runs the DocumentDB compatibility checker against your cluster:

- **compat-8.0.txt:** operators and API features unsupported in DocumentDB 8.0

Requires MongoDB 5.0+. For older versions, run the check separately using source code or log file scanning.

## Output Files

All output is organized in a folder per cluster PLUS a zip file for easy customer handoff:

```
atlas-metrics-<timestamp>/
├── <cluster>/                              # full output tree (33 files typical)
│   ├── <cluster>-14d-sizing-summary.md
│   ├── <cluster>-14d-sizing-report.json
│   ├── collstats.json
│   ├── index_analysis.json
│   ├── cost-estimator.csv
│   ├── compat-8.0.txt
│   ├── runtime.log
│   └── <node>_<batch>.json  (raw Atlas API metrics)
├── <cluster>.zip                           # 9-10x compressed, one-file handoff
└── processes.json                          # discovered processes across the project
```

| File | Description |
|------|-------------|
| `<cluster>-14d-sizing-summary.md` | Human-readable sizing report with recommendations |
| `<cluster>-14d-sizing-report.json` | Machine-readable report (all metrics as structured JSON) |
| `collstats.json` | Per-collection metrics + working set + cursor stats + compression sampling |
| `index_analysis.json` | Unused and redundant index report |
| `cost-estimator.csv` | Ready to upload to the [DocumentDB Calculator](https://d12ozu47xvq6hb.cloudfront.net/) |
| `compat-8.0.txt` | Compatibility check output (if `--compat`) |
| `runtime.log` | Full run log |
| `<node>-<batch>.json` | Raw Atlas API measurements per process/metric batch |
| `<cluster>.zip` | All the above bundled for one-file handoff (typical size 200-500 KB) |

## Next Steps After Running

1. **Review the sizing summary** (`*-sizing-summary.md`) for instance type and cluster type recommendations
2. **Review index analysis** (`index_analysis.json`) - drop unused and redundant indexes before migration
3. **Upload the CSV** (`cost-estimator.csv`) to the [DocumentDB Calculator](https://d12ozu47xvq6hb.cloudfront.net/)
4. **Review compatibility** (`compat-8.0.txt`) - plan workarounds for any unsupported operators

## Troubleshooting

**"Missing Atlas API credentials"** - Verify `ATLAS_PUBLIC_KEY`, `ATLAS_PRIVATE_KEY`, `ATLAS_PROJECT_ID` are exported.

**"HTTP 401 Unauthorized"** - Verify API keys are correct. Get them from Atlas UI → Project → Access Manager → API Keys.

**"HTTP 403 Forbidden - IP not on access list"** - Add your current public IP to the API key's Access List Entries: Atlas UI → Organization → Access Manager → API Keys → edit key → Access List.

**"Cluster '...' not found in project"** - The tool suggests near-matches. Check for typos or serverless clusters (not currently supported).

**"Cluster is currently PAUSED"** - Resume in Atlas UI (cluster page → Resume) or via API: `PATCH /clusters/{name}` with `{"paused": false}`. Resume takes ~3-4 minutes.

**"DB user lacks required role"** - Grant `atlasAdmin` (or `clusterMonitor` + `readAnyDatabase`) to the DB user: Atlas UI → Security → Database Access → edit user.

**"AUTHENTICATION FAILED during <phase>"** - Credentials rotated mid-run. Preflight validated the credentials at start, but a fresh MongoDB client mid-run failed. Common causes:
- Vault or AWS Secrets Manager auto-rotated the DB user password
- Manual password change during the run
- Short-lived credential TTL expired (< 45 min is risky for a 30-min run)

Recommendation: use a static `atlasAdmin` user for the collection window, or ensure the credential TTL is longer than 45 minutes. Partial output is preserved at the specified path. Re-run with fresh credentials to complete.

**"--cluster is SHARDED but --uri connects to a single replica set member"** - Get the SRV connection string from Atlas UI → cluster → Connect. Do NOT set `directConnection=true` in the URI.

**"--period ... exceeds retention"** - This is a warning, not an error. The tool proceeds but the data past retention is Atlas's coarser rollup. For accurate P95, use the tool's recommended `--period` for your granularity.

**Compression sampling shows `~3.5:1 (estimated conservative default)`** - The tool couldn't run real Zstandard sampling on that collection. Verify `zstandard` is installed: `pip install zstandard`. Common causes: empty collection, sampling exception (see runtime.log), or missing library.

**Per-collection ops/sec shows 0** - The tool takes a 5-second snapshot to compute per-collection ops/sec. If the cluster is idle at that moment, ops will be 0. Run during peak hours. The tool falls back to cumulative stats when the snapshot is idle.

## Known Limitations (v2.0.0)

- **Serverless Atlas clusters are not supported.** Serverless uses a different metrics API and pricing model (RPU/WPU) not yet implemented.
- **Tested against MongoDB 7.0 and 8.0.** Older versions (4.4, 5.0, 6.0) should work but are less extensively validated.
- **Time-series collections** (5.0+ feature) may produce partial output - untested.
- **Views, capped collections, GridFS** - untested.
- **Wildcard indexes** - captured in output but not supported in DocumentDB. Flag in your migration plan.
- **URI passwords with special characters** - must be percent-encoded per pymongo requirements.

## License

Apache License 2.0 - see LICENSE and NOTICE files.


---

## Using `--source ec2` (self-managed MongoDB on EC2)

For customers running MongoDB on Amazon EC2 (not Atlas), the tool discovers the underlying EC2 instances and pulls 14 days of CloudWatch metrics in addition to the MongoDB-side data.

### Prerequisites

1. **MongoDB URI** to any member of the replica set (or the mongos for a sharded cluster). The URI's DB user must have `clusterMonitor` + `readAnyDatabase` roles.
2. **AWS credentials** in the standard credential chain (env vars, profile, or IAM instance profile). Must have:

    ```json
    {
      "Effect": "Allow",
      "Action": [
        "cloudwatch:GetMetricData",
        "cloudwatch:GetMetricStatistics",
        "ec2:DescribeInstances",
        "ec2:DescribeVolumes",
        "ec2:DescribeNetworkInterfaces",
        "sts:GetCallerIdentity"
      ],
      "Resource": "*"
    }
    ```

3. **Network path** from the machine running the tool to the MongoDB nodes (must resolve hostnames to private IPs - run from a bastion inside the same VPC or with equivalent DNS access).
4. **Python 3.9+** with `pymongo`, `requests`, `zstandard`, **and `boto3`** installed.
5. **git** on PATH (required if using `--compat`).

### Install boto3 for the ec2 source

```bash
# On top of the existing requirements
pip install boto3
```

### Run

```bash
# Replica set
python3 atlas_metrics.py --source ec2 --compat \
    --uri "mongodb://monitor:MonitorPass@node1:27017,node2:27017,node3:27017/?replicaSet=rs0&authSource=admin" \
    --cluster my-mongo-cluster \
    --aws-region us-east-1

# Sharded (URI points at mongos)
python3 atlas_metrics.py --source ec2 --compat \
    --uri "mongodb://monitor:MonitorPass@mongos:27017/?authSource=admin" \
    --cluster my-mongo-cluster \
    --aws-region us-east-1

# Multi-sample variance analysis (3 samples × 60s = 3 minutes of MongoDB delta sampling)
python3 atlas_metrics.py --source ec2 --samples 3 \
    --uri "..." --cluster my-mongo-cluster
```

If `--aws-region` is omitted, the tool auto-detects from `AWS_REGION` / `AWS_DEFAULT_REGION` env vars, boto3 session config, or EC2 IMDSv2 (in that order).

### What `--source ec2` produces

Same output structure as `--source atlas`, plus:

- `ec2_instances.json` - the AWS account, region, MongoDB topology, server version, and per-instance metadata (private IP, instance type, state, attached EBS volumes)
- `cloudwatch.json` - 14 days of EC2 (CPU, network) + EBS (IOPS, bytes, latency, queue depth, throughput %) metrics at 5-minute granularity for every discovered instance
- `mongo_sampling.json` - one or more 60-second `serverStatus` deltas (op rates, network throughput, connection counts, WT cache pressure, memory)
- `profiler_data.json` - cross-references the MongoDB profiler (if enabled) against DocumentDB unsupported operator list
- `collstats.json` + `index_analysis.json` - per-collection stats with unused-index detection and prefix-subset redundancy (uses the same mongos-aware collector that atlas uses for sharded clusters)
- `compat-8.0.txt` (with `--compat`) - DocumentDB 8.0 compatibility report

**Note:** `sizing-summary.md` and `cost-estimator.csv` are Atlas-source only in v2.1.0. Adapter work to consume CloudWatch metric shape for these outputs is planned for a follow-up patch.

### 6-gate preflight

`--source ec2` validates all prerequisites before starting the (long) CloudWatch pull:

1. MongoDB URI reachable + auth valid + topology detected + `clusterMonitor` role present
2. Instance discovery: `rs.status()` (RS) or `sh.status()` (sharded) → private IPs
3. AWS credentials valid (`sts:GetCallerIdentity`)
4. IAM permissions: `ec2:DescribeInstances` + `cloudwatch:GetMetricData`
5. All discovered instances in same AWS region (single-region scope in v2.1.0)
6. If `--compat`, MongoDB >= 5.0

Any gate failure exits cleanly with actionable remediation text.

