# MongoDB Metrics Collector -- User Guide

Collects capacity metrics from a MongoDB deployment and produces a sizing package for migrating to Amazon DocumentDB. Works against MongoDB Atlas or self-managed MongoDB on Amazon EC2.

**Current version:** 2.4.0 (see CHANGELOG.md)

Part of [amazon-documentdb-tools](https://github.com/awslabs/amazon-documentdb-tools). This is the measured counterpart to [`migration/sizing-tool`](../sizing-tool): `sizing-tool` is the quick self-service CSV and leaves the workload columns as placeholders for you to fill in by hand, while this tool measures them from a real metric window. Both use the same [`compression-review`](../../performance/compression-review) sampling.

This guide is organized by source. Read [Sources](#sources) first, then go to the section for your deployment:

- [Atlas](#atlas) -- MongoDB Atlas clusters
- [Self-managed MongoDB](#self-managed-mongodb) -- MongoDB on Amazon EC2

Each of those sections is self-contained: prerequisites, installation, how to run, flags, and output.

## Sources

The tool supports two source topologies via `--source`:

| Source | Use for | Auth needed | Metric source |
|---|---|---|---|
| **`atlas`** (default) | MongoDB Atlas clusters | Atlas API key + DB user | Atlas Admin API |
| **`ec2`** | Self-managed MongoDB on Amazon EC2 | DB user + AWS credentials (CloudWatch + EC2 read) | Amazon CloudWatch |

`--source` defaults to `atlas`, so existing Atlas invocations need no changes. A third value, `--source onp` (on-premises), is reserved and currently exits with a not-implemented message.

## Sharded Cluster Support

Applies to both sources.

Sharded clusters are supported via mongos-aggregated `collStats`. The tool connects to the mongos router (the standard SRV URI on Atlas) and iterates the `shards` sub-document returned per collection.

No special network configuration is required. The tool works from any network path, including public URI, AWS PrivateLink, and VPC peering. Direct-shard connections are not used, because MongoDB itself rejects `collStats` on a sharded collection when the client is connected directly to a shard.

Per-shard data captured:

- Document count, size, storage size, average object size
- Number of indexes, total index size
- WiredTiger cache pages/bytes read
- WiredTiger cursor stats (inserts, updates, removes, searches)
- `$indexStats` accesses per shard
- Live compression sampling (via mongos routing)
- Live index-key-type sampling (via mongos routing)

`collstats.json` carries `data_source: "mongos_aggregated"` for sharded clusters.

## What Gets Collected

### From Atlas Admin API (`--source atlas`)

Collected per node, for every process in the cluster:

- **CPU:** user, kernel, iowait, steal
- **Memory:** system used/free, resident (working set), virtual, swap used, page faults
- **Operations:** insert, query, update, delete, getmore, cmd per second
- **Disk I/O:** read/write IOPS, read/write latency, queue depth
- **Storage:** data size, storage size, index size
- **Connections:** current count
- **Query efficiency:** keys and documents scanned per returned, scan-and-order operations, read/write/command execution times
- **WiredTiger cache:** bytes read into and written from cache, dirty bytes, used bytes, cache fill ratio, read and write tickets available
- **Replication:** oplog rate, and replication lag on secondary members

Replication-lag metrics exist only on secondary processes, so they are requested only for those roles. A primary has no replication lag, and the Atlas API rejects the metric name outright rather than returning an empty series.

### From CloudWatch (`--source ec2`)

- **EC2:** CPU utilization, network in/out
- **EBS:** read/write IOPS, read/write bytes, latency, queue depth, throughput percentage

Collected at 5-minute granularity over 14 days for every discovered instance.

### From Direct MongoDB Connection (both sources)

Collected by connecting to the deployment and running database commands:

- **Per-collection operations:** insert, update, delete, search counts from WiredTiger cursor stats
- **Per-collection working set:** percentage of data actively accessed
- **Index analysis:** unused indexes (0 accesses) and redundant indexes (prefix subsets)
- **Compression sampling:** real Zstandard-3 with 100-document dictionary training, matching DocumentDB 8.0 behavior
- **Index key type sampling:** first-50 and last-50 documents per collection, sampled for actual key sizes (compound, hashed, multikey)
- **Cost Estimator CSV:** fields auto-populated, ready to upload to the DocumentDB Calculator

When real Zstandard sampling is unavailable (the `zstandard` package is not installed, or the collection is empty), the tool falls back to a conservative **3.5:1** ratio. A real sampled ratio always takes precedence. The fallback is deliberately conservative: estimated size is data size divided by the ratio, so a higher ratio yields a smaller instance recommendation, and understating compression oversizes rather than undersizes.

### From Compat-Tool (`--compat`, both sources)

Runs the DocumentDB compatibility checkers against the deployment:

- **`compat-8.0.txt`** -- operators and API features unsupported in DocumentDB 8.0
- **`index_compat.json`** -- index types DocumentDB does not support, with a coverage verdict
- **`index_metadata/`** -- the index dump, re-scannable later without a cluster connection

Requires MongoDB 5.0+. For older versions, run the checks separately against source code or log files.

---

## Atlas

### Prerequisites

1. **Atlas API key** with `Project Read Only` role (minimum)
2. **Atlas Project ID** (24-character hex string, from the Atlas UI URL)
3. **Atlas database user** with `atlasAdmin` role (recommended) or `clusterMonitor` + `readAnyDatabase`
4. **MongoDB connection URI** for the target cluster
5. **Python 3.9+**
6. **`git`** on PATH, required only if using `--compat` (the tool auto-clones amazon-documentdb-tools). Install with `yum install git`, `apt install git`, or `brew install git`.
7. **Network access** -- the public IP of the machine running the tool must be on the Atlas Network Access list for the project

#### Why `atlasAdmin` (or `clusterMonitor` + `readAnyDatabase`)?

The tool runs `serverStatus`, `collStats`, and `$indexStats`. `readWriteAnyDatabase` alone is not sufficient, and the preflight fails fast with a clear error. Grant one of the recommended roles in Atlas UI -> Project -> Security -> Database Access.

### Installation

#### Step 1: Create an Atlas API key

1. Log in to [cloud.mongodb.com](https://cloud.mongodb.com)
2. Select your project from the top-left dropdown
3. Go to **Access Manager** (left sidebar) -> **API Keys**
4. Click **Create API Key**
5. Set the role to **Project Read Only**
6. Copy the **Public Key** and **Private Key**. The private key is shown only once.
7. Click **Add Access List Entry** and add your current public IP

#### Step 2: Find your Project ID

The Project ID is the 24-character hex string in the Atlas URL:

```
https://cloud.mongodb.com/v2/abcdef0123456789abcdef01#/clusters
                             ^^^^^^^^^^^^^^^^^^^^^^^^
                             This is your Project ID
```

Also available in **Project Settings** (gear icon, left sidebar).

#### Step 3: Grant database user permissions

1. In Atlas: **Security** -> **Database Access** -> find your user -> **Edit**
2. Under **Built-in Role**, select `atlasAdmin`
3. Save

Alternatively assign `clusterMonitor` + `readAnyDatabase` for minimum privilege.

#### Step 4: Get the MongoDB connection URI

1. In Atlas, click **Connect** on the cluster
2. Choose **Drivers**
3. Copy the connection string, of the form `mongodb+srv://user:pass@cluster.xxxxx.mongodb.net/`
4. Replace `<password>` with the database user's password
5. Percent-encode any special characters in the password (`@`, `:`, `/`, `?`)

#### Step 5: Install the tool

```bash
git clone https://github.com/awslabs/amazon-documentdb-tools.git
cd amazon-documentdb-tools/migration/metrics-collector
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

#### Step 6: Set environment variables

```bash
export ATLAS_PUBLIC_KEY="your-public-key"
export ATLAS_PRIVATE_KEY="your-private-key"
export ATLAS_PROJECT_ID="your-24-char-project-id"
```

### Preflight Checks

Five gates run before the long collection starts, so a misconfiguration fails in seconds rather than 30 minutes in:

| Gate | What it checks |
|------|----------------|
| [1/5] | Atlas API credentials work, and lists clusters in the project |
| [2/5] | `--cluster` exists in the project. Detects paused clusters and typos, and suggests corrections. |
| [3/5] | `--uri` reachable, auth works, database user role sufficient (probes `serverStatus`) |
| [4/5] | `--uri` connects to the same cluster as `--cluster`. Cross-checks SHARDED/REPLICASET topology. |
| [5/5] | If `--compat`, MongoDB version is 5.0 or later |

Every error includes the current public IP and remediation steps.

### Standard sizing run (recommended)

```bash
python3 metrics_collector.py --all \
  --cluster my-cluster-name \
  --uri "mongodb+srv://user:pass@cluster.xxxxx.mongodb.net/" \
  --compat
```

Collects 14 days of Atlas API metrics at 5-minute granularity, per-collection sizing analysis, index review, operator and index compatibility checks, and the Cost Estimator CSV.

### Debug / quick check

```bash
python3 metrics_collector.py \
  --cluster my-cluster-name \
  --uri "mongodb+srv://user:pass@cluster.xxxxx.mongodb.net/"
```

Runs at 1-minute granularity over 48 hours (the defaults). Takes roughly 5 minutes.

### Options

| Flag | Required | Description |
|------|----------|-------------|
| `--uri URI` | **Yes** | MongoDB connection string |
| `--cluster NAME` | **Yes** | Atlas cluster name |
| `--source atlas` | No | Default. See [Sources](#sources). |
| `--all` | No | Standard sizing run. **Implicitly sets `--granularity PT5M --period P14D`; explicit values for those two flags are ignored when `--all` is present.** |
| `--compat` | No | Run DocumentDB 8.0 operator and index compatibility checks (requires MongoDB 5.0+) |
| `--index-compat-from DIR` | No | Re-classify a stored index dump with no cluster connection |
| `--granularity` | No | `PT10S` (M40+ clusters only, see [retention](#data-retention-per-granularity)), `PT1M` (default), `PT5M`, `PT1H`, `P1D` |
| `--period` | No | ISO 8601 duration, `P2D` default. Accepts `P<n>Y`, `P<n>W`, `P<n>D`, `PT<n>H`, `PT<n>M`. Rejected at startup if unparseable. Warns if it exceeds retention. |
| `--percentile` | No | Sizing percentile: 90, 95 (default), or 99 |
| `--output` | No | Output directory (auto-generated if omitted) |

### Data Retention Per Granularity

Atlas retains monitoring data for different windows depending on granularity:

| Granularity | Retention | Recommended `--period` |
|-------------|-----------|------------------------|
| `PT10S` | ~24 hours (M40+ clusters only) | `P1D` |
| `PT1M` | ~14 days | `P2D` (kept small for debugging speed) |
| `PT5M` | ~14 days | `P14D` |
| `PT1H` | ~12 months | `P365D` |
| `P1D` | effectively indefinite | `P730D` |

Setting `--period` beyond retention prints a warning and proceeds. Past the window, Atlas downsamples to hourly rollups and returns them in buckets labeled at the requested granularity, which biases P95 estimates low.

### Output

Output is a folder per cluster, plus a zip for handoff:

```
metrics-collector-<timestamp>/
├── <cluster>/                              # full output tree (37 files typical)
│   ├── <cluster>-14d-sizing-summary.md
│   ├── <cluster>-14d-sizing-report.json
│   ├── collstats.json
│   ├── index_analysis.json
│   ├── index_compat.json
│   ├── index_metadata/
│   ├── cost-estimator.csv
│   ├── compat-8.0.txt
│   ├── runtime.log
│   └── <node>_<batch>.json                 # raw Atlas API metrics
├── <cluster>.zip                           # 9-10x compressed, one-file handoff
└── processes.json                          # processes discovered across the project
```

| File | Description |
|------|-------------|
| `<cluster>-14d-sizing-summary.md` | Human-readable sizing report with recommendations |
| `<cluster>-14d-sizing-report.json` | Machine-readable report, all metrics as structured JSON |
| `collstats.json` | Per-collection metrics, working set, cursor stats, compression sampling |
| `index_analysis.json` | Unused and redundant index report |
| `index_compat.json` | Index types unsupported by DocumentDB, plus a coverage verdict |
| `index_metadata/` | Index dump in mongodump format, re-scannable via `--index-compat-from` |
| `cost-estimator.csv` | Ready to upload to the [DocumentDB Calculator](https://d12ozu47xvq6hb.cloudfront.net/) |
| `compat-8.0.txt` | Operator compatibility output (with `--compat`) |
| `runtime.log` | Full run log |
| `<node>_<batch>.json` | Raw Atlas API measurements per process and metric batch |
| `<cluster>.zip` | All of the above bundled, typically 200-500 KB |

---

## Self-managed MongoDB

For MongoDB running on Amazon EC2. The tool discovers the underlying EC2 instances from the replica set or sharded topology, then pulls 14 days of CloudWatch metrics alongside the MongoDB-side data.

### Prerequisites

1. **MongoDB URI** to any member of the replica set, or to the mongos for a sharded cluster. The database user needs `clusterMonitor` + `readAnyDatabase`.
2. **AWS credentials** in the standard credential chain (environment variables, profile, or IAM instance profile), with:

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

3. **Network path** from the machine running the tool to the MongoDB nodes, resolving hostnames to private IPs. Run from a bastion inside the same VPC, or with equivalent DNS access.
4. **Python 3.9+** with `pymongo`, `requests`, `zstandard`, and `boto3`.
5. **`git`** on PATH, required only if using `--compat`.

### Installation

#### Step 1: Install the tool

```bash
git clone https://github.com/awslabs/amazon-documentdb-tools.git
cd amazon-documentdb-tools/migration/metrics-collector
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

#### Step 2: Install boto3

```bash
pip install boto3
```

No Atlas API key and no environment variables are needed for this source.

### Preflight Checks

Six gates run before the CloudWatch pull begins:

| Gate | What it checks |
|------|----------------|
| [1/6] | MongoDB URI reachable, auth valid, topology detected, `clusterMonitor` role present |
| [2/6] | Instance discovery via `rs.status()` (replica set) or `sh.status()` (sharded), resolving to private IPs |
| [3/6] | AWS credentials valid (`sts:GetCallerIdentity`) |
| [4/6] | IAM permissions: `ec2:DescribeInstances` and `cloudwatch:GetMetricData` |
| [5/6] | All discovered instances in the same AWS region (single-region scope) |
| [6/6] | If `--compat`, MongoDB version is 5.0 or later |

Any gate failure exits cleanly with actionable remediation text.

### Standard sizing run (recommended)

```bash
# Replica set
python3 metrics_collector.py --source ec2 --compat \
    --uri "mongodb://monitor:MonitorPass@node1:27017,node2:27017,node3:27017/?replicaSet=rs0&authSource=admin" \
    --cluster my-mongo-cluster \
    --aws-region us-east-1

# Sharded, URI points at the mongos
python3 metrics_collector.py --source ec2 --compat \
    --uri "mongodb://monitor:MonitorPass@mongos:27017/?authSource=admin" \
    --cluster my-mongo-cluster \
    --aws-region us-east-1
```

If `--aws-region` is omitted, the region is resolved from `AWS_REGION`, then `AWS_DEFAULT_REGION`, then boto3 session config, then EC2 IMDSv2, in that order.

### Debug / quick check

```bash
python3 metrics_collector.py --source ec2 \
    --uri "mongodb://monitor:MonitorPass@node1:27017/?authSource=admin" \
    --cluster my-mongo-cluster
```

### Options

Flags below are specific to, or behave differently under, `--source ec2`. Everything in the [Atlas Options table](#options) that is not Atlas-API-specific also applies.

| Flag | Required | Description |
|------|----------|-------------|
| `--source ec2` | **Yes** | Selects the self-managed MongoDB path |
| `--uri URI` | **Yes** | MongoDB connection string, any replica set member or the mongos |
| `--cluster NAME` | **Yes** | Label for the deployment, used for output naming |
| `--aws-region` | No | AWS region. Auto-detected if omitted, see above. **EC2 source only.** |
| `--samples N` | No | Number of 60-second `serverStatus` delta samples. Default 1. **EC2 source only.** |
| `--compat` | No | Run DocumentDB 8.0 operator and index compatibility checks |
| `--percentile` | No | Sizing percentile: 90, 95 (default), or 99 |
| `--output` | No | Output directory (auto-generated if omitted) |

`--granularity` and `--period` do not apply to this source. CloudWatch collection is fixed at 5-minute granularity over 14 days.

Multi-sample example, three samples of 60 seconds each:

```bash
python3 metrics_collector.py --source ec2 --samples 3 \
    --uri "..." --cluster my-mongo-cluster
```

### Output

Same structure as the Atlas source, with these additions and one omission:

| File | Description |
|------|-------------|
| `ec2_instances.json` | AWS account, region, MongoDB topology, server version, and per-instance metadata (private IP, instance type, state, attached EBS volumes) |
| `cloudwatch.json` | 14 days of EC2 and EBS metrics at 5-minute granularity for every discovered instance |
| `mongo_sampling.json` | One or more 60-second `serverStatus` deltas: op rates, network throughput, connection counts, WiredTiger cache pressure, memory |
| `profiler_data.json` | Cross-references the MongoDB profiler, if enabled, against the DocumentDB unsupported-operator list |
| `collstats.json`, `index_analysis.json` | Per-collection stats with unused-index detection and prefix-subset redundancy, via the same mongos-aware collector the Atlas source uses for sharded clusters |
| `compat-8.0.txt`, `index_compat.json` | Compatibility reports (with `--compat`) |

**Not produced by this source:** `sizing-summary.md` and `cost-estimator.csv` are Atlas-source only. Adapter work to consume the CloudWatch metric shape for these two outputs is planned for a follow-up release.

---

## Next Steps After Running

1. **Review the sizing summary** (`*-sizing-summary.md`) for instance type and cluster type recommendations
2. **Review index analysis** (`index_analysis.json`) and drop unused and redundant indexes before migrating
3. **Review index compatibility** (`index_compat.json`) and plan replacements for any unsupported index types
4. **Upload the CSV** (`cost-estimator.csv`) to the [DocumentDB Calculator](https://d12ozu47xvq6hb.cloudfront.net/)
5. **Review operator compatibility** (`compat-8.0.txt`) and plan workarounds for unsupported operators

## Troubleshooting

**"Missing Atlas API credentials"** -- Verify `ATLAS_PUBLIC_KEY`, `ATLAS_PRIVATE_KEY`, and `ATLAS_PROJECT_ID` are exported.

**"HTTP 401 Unauthorized"** -- Verify the API keys. Atlas UI -> Project -> Access Manager -> API Keys.

**"HTTP 403 Forbidden, IP not on access list"** -- Add the current public IP to the API key's access list entries: Atlas UI -> Organization -> Access Manager -> API Keys -> edit key -> Access List.

**"Cluster '...' not found in project"** -- The tool suggests near-matches. Check for typos, or for a serverless cluster, which is not supported.

**"Cluster is currently PAUSED"** -- Resume in the Atlas UI (cluster page -> Resume) or via the API: `PATCH /clusters/{name}` with `{"paused": false}`. Resume takes 3 to 4 minutes.

**"DB user lacks required role"** -- Grant `atlasAdmin`, or `clusterMonitor` + `readAnyDatabase`, to the database user: Atlas UI -> Security -> Database Access -> edit user.

**"Unrecognized --period value"** -- The period must be one of `P<n>Y`, `P<n>W`, `P<n>D`, `PT<n>H`, `PT<n>M`, for example `P14D` or `PT12H`. This is rejected at startup rather than silently substituted.

**"AUTHENTICATION FAILED during \<phase\>"** -- Credentials rotated mid-run. Preflight validated them at the start, but a fresh MongoDB client later in the run failed. Common causes: Vault or AWS Secrets Manager auto-rotated the database user password, a manual password change during the run, or a short-lived credential TTL expiring. A TTL under 45 minutes is risky for a 30-minute run. Use a static `atlasAdmin` user for the collection window, or extend the TTL. Partial output is preserved at the specified path; re-run with fresh credentials to complete.

**"--cluster is SHARDED but --uri connects to a single replica set member"** -- Use the SRV connection string from Atlas UI -> cluster -> Connect. Do not set `directConnection=true`.

**"--period ... exceeds retention"** -- A warning, not an error. The run proceeds, but data past retention is Atlas's coarser rollup. For an accurate P95, use the recommended `--period` for the granularity.

**Compression sampling shows `~3.5:1 (estimated conservative default)`** -- Real Zstandard sampling could not run on that collection. Verify `zstandard` is installed: `pip install zstandard`. Other causes are an empty collection or a sampling exception; check `runtime.log`.

**Per-collection ops/sec shows 0** -- The tool takes a 5-second snapshot to compute per-collection ops/sec. An idle cluster at that moment yields 0. Run during peak hours. The tool falls back to cumulative stats when the snapshot is idle.

**A metric batch logs "returned 404, falling back to individual metrics"** -- One metric name in that batch was rejected by the Atlas API, which rejects the whole batch rather than the single name. The tool retries each metric individually so the rest still collect. The run continues correctly, but makes one API call per metric in that batch instead of one for the batch.

**`replication.lag_sec` is absent on one node** -- Expected on the primary. Replication-lag metrics exist only on secondary members.

## Known Limitations

Originally documented at v2.0.0 and re-confirmed at v2.4.0.

- **Serverless Atlas clusters are not supported.** Serverless uses a different metrics API and a different pricing model (RPU/WPU).
- **Tested against MongoDB 7.0 and 8.0.** Versions 4.4, 5.0, and 6.0 should work but are less extensively validated.
- **Time-series collections** (5.0+) may produce partial output. Untested.
- **Views, capped collections, and GridFS** are untested.
- **Wildcard indexes** are captured in output but are not supported in DocumentDB. Flag them in the migration plan.
- **URI passwords with special characters** must be percent-encoded, per pymongo.
- **`--source ec2` is single-region.** All discovered instances must be in one AWS region.
- **`--source ec2` does not produce `sizing-summary.md` or `cost-estimator.csv`.** See that section's Output notes.
- **`--source onp`** (on-premises) is reserved and not implemented.

## License

Apache License 2.0. See LICENSE and NOTICE.
