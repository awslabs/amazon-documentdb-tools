# Custom Metrics Tool
Amazon DocumentDB exposes many CloudWatch metrics out of the box, but some useful cluster, collection, and index statistics aren't among them. The **custom-metrics** tool fills that gap: it connects to a cluster, collects the statistics you specify, and publishes them as custom CloudWatch metrics in the **CustomDocDB** namespace — so you can graph and alarm on them just like any native metric.

**Metrics collected**

- **Cluster** — collection count, database count, user count
- **Collection** — collection size, index count, collection scans¹, index scans¹
- **Index** — index size

¹ Collected per cluster instance — see [Collection scans and index scans](#collection-scans-and-index-scans-per-instance) below.

Each metric is published with the dimensions that identify what it describes:

| Metric(s) | Dimensions |
|---|---|
| collection count, database count, user count | `Cluster` |
| collection size, index count | `Cluster, Collection, Database` |
| index size | `Cluster, Collection, Database, Index` |
| collection scans, index scans | `Cluster, Collection, Database, Instance` |

### Collection scans and index scans (per instance)

The `collScans` and `idxScans` counters returned by `collStats` are per instance since every instance maintains its own counters. To capture scans across the whole cluster, the tool discovers every instance in the cluster (via the `hello` command), connects directly to each instance using a direct connection, then reads and publishes that instance's counters. Each metric therefore includes an **Instance** dimension identifying the instance the scans were observed on.

The counters are **cumulative** and reset when an instance restarts, or when the cluster is stopped/started or scaled. It is recommended to alarm on the rate/delta of these metrics (e.g. CloudWatch `RATE()` metric math), not on the raw value.

## Requirements

Python 3.x with modules:

* boto3 - AWS SDK that allows management of AWS resources through Python
* pymongo - MongoDB driver for Python applications

```
pip install boto3
pip install pymongo
```

Download the Amazon DocumentDB Certificate Authority (CA) certificate required to authenticate to your cluster:
```
wget https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem
```

------------------------------------------------------------------------------------------------------------------------
## Database user considerations

The tool only reads metadata and statistics and never writes application data. Because the tool lists all databases in the cluster, the user supplied in the connection URI must have a **cluster-wide read role** (e.g. `readAnyDatabase`) granted on the `admin` database. A user scoped to a single database cannot enumerate all databases and will fail with `Authorization failure` (error code 13).

------------------------------------------------------------------------------------------------------------------------
## Required IAM permissions

The tool publishes metrics with the CloudWatch `PutMetricData` API, so the AWS identity it runs under must be allowed to publish to CloudWatch. Attach a policy granting `cloudwatch:PutMetricData`. `PutMetricData` does not support resource-level permissions, so `Resource` must be `*`. The `cloudwatch:namespace` condition restricts publishing to the tool's **CustomDocDB** namespace:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "PublishCustomDocDBMetrics",
            "Effect": "Allow",
            "Action": "cloudwatch:PutMetricData",
            "Resource": "*",
            "Condition": {
                "StringEquals": {
                    "cloudwatch:namespace": "CustomDocDB"
                }
            }
        }
    ]
}
```

------------------------------------------------------------------------------------------------------------------------
## Performance and cost implications of publishing collection scan and index scan metrics

The collection scan and index scan metrics are gathered per collection, on every instance in the cluster. This is more expensive than gathering the other metrics, which run once against the cluster.

For each run, the tool:
1. Connects directly to every instance in the cluster.
2. For every instance, runs `collStats` for every monitored collection (based on the `--namespaces` option).

For example, a cluster with 3 instances and 500 monitored collections issues 3 × 500 = 1,500 `collStats` commands per run. When gathering these metrics on many collections, this can:
* **Add load to every instance**. `collStats` is lightweight individually, but thousands of them per run can compete with application workload.
* **Increase run time**, potentially causing overlapping runs if scheduled frequently.
* **Multiply CloudWatch custom-metric count and cost.** Each collection maintains `collScans` and `idxScans` metrics **per instance**, so the count of each metric scales with `collections × instances`. Custom metrics are billed per metric and this can grow quickly when monitoring large numbers of collections.

> **Important — scope your namespaces.** Do not default to `--namespaces "*.*"` for collection and index scan metrics on a cluster with many collections. Use `--namespaces` to limit collection scan and index scan metrics collection to the most important collections. For example, high-traffic collections where an unexpected collection scan is likely to impact other workload. This keeps instance load, run time, and CloudWatch cost proportional to the value of the data, rather than scaling with the entire cluster.

Guidance:

* Use an explicit list of the key namespaces (e.g. `"orders.transactions, users.sessions"`) over broad wildcards for collection and index scan metrics.
* If you must use a wildcard, use the narrowest that captures what you need (`"<database>.*"` for one important database rather than `"*.*"`).
* Collect collection and index scan metrics on a cadence appropriate to how quickly you need to detect a problem — more frequent collection multiplies all of the costs above.

------------------------------------------------------------------------------------------------------------------------
## Usage

The tool accepts the following arguments:

```
python3 custom-metrics.py --help
usage: custom-metrics.py [-h] [--skip-python-version-check] --cluster_name
                         CLUSTER_NAME --uri URI --namespaces NAMESPACES
                         [--collection_count] [--database_count]
                         [--user_count] [--collection_size] [--index_count]
                         [--index_size] [--collection_scans] [--index_scans]

optional arguments:
  -h, --help            show this help message and exit
  --skip-python-version-check
                        Permit execution on Python 3.6 and prior
  --cluster_name CLUSTER_NAME
                        Name of cluster for Amazon CloudWatch custom metric
  --uri URI             Amazon DocumentDB Connection URI
  --namespaces NAMESPACES
                        comma separated list of namespaces to monitor
  --collection_count    log cluster collection count
  --database_count      log cluster database count
  --user_count          log cluster user count
  --collection_size     log collection size
  --index_count         log collection index count
  --index_size          log collection index size
  --collection_scans    log collection scans for each cluster instance
  --index_scans         log index scans for each cluster instance
```

Examples of ```namespaces``` parameter:

1. Specific namespace: ```"<database>.<collection>"```
2. All collections in specific database: ```"<database>.*"```
3. Specific collection in any database: ```"*.<collection>"```
4. All namespaces: ```"*.*"```
5. Multiple namespaces: ```"<database>.*, *.<collection>, <database>.<collection>"```
