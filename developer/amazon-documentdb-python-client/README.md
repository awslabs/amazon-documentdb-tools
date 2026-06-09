# amazon-documentdb-python-client

Two lines of config. Every Amazon DocumentDB best practice, enforced automatically.

####  Before
```python

from pymongo import MongoClient

client = MongoClient(
    "mongodb://user:pass@mycluster.docdb.amazonaws.com:27017",
    tls=True,
    tlsCAFile="/certs/global-bundle.pem",
    # are these right? is anything missing?
)
db = client["mydb"]
```

#### After

```python

import docdb
from docdb.secrets import config_from_secret

docdb.init(config_from_secret("prod/myapp/docdb", app_name="my-service"))

db = docdb.get_client().db("mydb")
# replicaSet, directConnection, retryWrites, pool sizing, TLS — handled

```

|  | Without this library | With this library |
| --- | --- | --- |
| Replica set mode | Easy to forget | Always on |
| directConnection | Wrong default breaks failover | Enforced off |
| retry[Reads, Writes] | Guaranteed once | Built-in exponential retry w/ backoff |
| Client-per-request | Silent pool exhaustion | Singleton enforced |
| Cursor leaks | No guardrail | managed_cursor context manager |
| Troubleshooting | Queries are anonymous | app_name wired through |

---

## Prerequisites

- Python >= 3.10
- An [Amazon DocumentDB](https://docs.aws.amazon.com/documentdb/latest/developerguide/what-is.html) cluster
- Network connectivity to the cluster (VPC, security groups, [SSH tunnel](https://docs.aws.amazon.com/documentdb/latest/developerguide/connect-from-outside-a-vpc.html), etc.)
- The [Amazon RDS TLS CA bundle](https://docs.aws.amazon.com/documentdb/latest/developerguide/ca_cert_rotation.html):

  ```bash
  wget https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem
  ```

---

## Install

```bash
pip install .
```

With optional extras:

```bash
pip install ".[secrets]"      # AWS Secrets Manager support (requires boto3)
pip install ".[cloudwatch]"   # CloudWatch telemetry backend (requires boto3)
pip install ".[iam]"          # IAM authentication (requires pymongo[aws])
```

---

## Quickstart

**With explicit config:**

```python
import docdb
from docdb import DocumentDBConfig

docdb.init(DocumentDBConfig(
    host="mycluster.cluster-abc123.us-east-1.docdb.amazonaws.com",
    username="appuser",
    password="changeme",
    tls_ca_file="global-bundle.pem",
    app_name="my-service",
))

db = docdb.get_client().db("mydb")
db.orders.find_one({"_id": order_id})
```

**With Secrets Manager (production):**

Requires `pip install ".[secrets]"`. The secret must contain `host`, `username`, and `password` as JSON fields. See [src/docdb/secrets.py](src/docdb/secrets.py) for supported secret formats.

```python
import docdb
from docdb.secrets import config_from_secret

docdb.init(config_from_secret("prod/myapp/docdb", region="us-east-1", app_name="my-service"))

db = docdb.get_client().db("mydb")
db.orders.find_one({"_id": order_id})
```

Flask, FastAPI, Lambda patterns in [examples/](examples/).

---

## Go deeper

- [Full overview](docs/overview.md) — architecture, enforced defaults, plugin design, project structure
- [Connection tracker](docs/connection_tracker.md) — failover behavior
- [IAM authentication](docs/iam-auth.md) — password-less auth with IAM roles
- [Plugin system](docs/plugins.md) — telemetry, retry, custom middleware
- [Telemetry plugin](docs/telemetry.md) — metrics, heartbeats, tuning
- [Retry plugin](docs/retry.md) — backoff, idempotent writes, transactions
