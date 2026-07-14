# IAM Authentication

## Overview

The wrapper also supports Amazon DocumentDB's ability to use password-less authentication through [AWS IAM](https://docs.aws.amazon.com/IAM/latest/UserGuide/id.html) users and roles. The driver retrieves temporary credentials from [AWS STS](https://docs.aws.amazon.com/STS/latest/APIReference/Welcome.html) and uses them to authenticate.

---

## Quickstart

```python
from docdb import DocumentDBConfig

config = DocumentDBConfig(
    host="mycluster.cluster-abc123.us-east-1.docdb.amazonaws.com",
    app_name="my-service",
    iam_auth=True,
)
```

If your compute environment has an IAM role attached (EC2 instance profile, Lambda execution role, EKS pod identity, Fargate task role), the driver picks up credentials automatically from environment variables or instance metadata.

---

## Configuration

| Parameter | Default | Description |
| --- | --- | --- |
| `iam_auth` | `False` | When `True`, uses `authMechanism="MONGODB-AWS"` instead of username/password |

No other configuration is needed. Credentials are sourced from the environment automatically.

### Prerequisites

| Requirement | Details |
| --- | --- |
| Amazon DocumentDB version | 5.0+ (instance-based clusters only) |
| Python dependency | `pip install 'pymongo[aws]'` |
| IAM user restriction | Cannot be the cluster's primary user |
| TLS | Must be enabled |

### IAM policy

The IAM role or user needs permission to connect:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": "rds-db:connect",
            "Resource": "arn:aws:rds-db:us-east-1:123456789012:dbuser:cluster-abc123/*"
        }
    ]
}
```

Replace the `Resource` ARN with your cluster's resource ID.

### Creating a database user

You must create a database user in Amazon DocumentDB that maps to the IAM identity that will connect.

**Option 1: IAM role** (Lambda, ECS, EKS, EC2 instance profile)

```javascript
use $external;
db.createUser(
    {
        user: "arn:aws:iam::123456789123:role/iamrole",
        mechanisms: ["MONGODB-AWS"],
        roles: [ { role: "readWrite", db: "readWriteDB" } ]
    }
);
```

Any compute instance that assumes this role can authenticate without credentials in the application code.

**Option 2: IAM user** (developer workstations, CI/CD)

```javascript
use $external;
db.createUser(
    {
        user: "arn:aws:iam::123456789123:user/iamuser",
        mechanisms: ["MONGODB-AWS"],
        roles: [ { role: "readWrite", db: "readWriteDB" } ]
    }
);
```

The IAM user's access keys must be available as environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`) or in `~/.aws/credentials`. You can review the full details in [Authentication using IAM identity](https://docs.aws.amazon.com/documentdb/latest/developerguide/iam-identity-auth.html).

---

## Behavior

### How authentication works

1. PyMongo (with the `pymongo[aws]` extra) detects `authMechanism="MONGODB-AWS"`
2. It retrieves temporary credentials from one of:
   - Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`)
   - EC2 instance metadata (IMDS)
   - ECS task role endpoint
   - EKS pod identity
3. It sends an STS-signed authentication request to Amazon DocumentDB
4. Amazon DocumentDB validates the signature with STS and establishes the session
5. The connection is now authenticated and credentials are not stored or sent again

Credentials are only used during connection establishment. Once authenticated, the connection remains valid even if the temporary credentials expire or rotate. No token caching or refresh logic is needed as the connection pool is effectively the cache. New STS calls only happen when PyMongo opens a genuinely new connection (pool growth or replacement), which the wrapper's pool settings (`min_pool_size=5`, `max_idle_time_ms=10000`) already minimize.

### STS throttling

IAM authentication calls AWS STS for every new connection. At very high connection rates (hundreds of new connections per second), this can hit STS throttling limits.

In practice, the wrapper's defaults already prevent this: `min_pool_size=5` keeps connections warm, `max_idle_time_ms=10000` avoids unnecessary churn, and the singleton pattern prevents per-request client creation. New STS calls only happen when the pool genuinely needs to grow.

If you're running an extremely high-throughput service and still see STS throttling, increase `min_pool_size` to keep more connections warm and reduce the rate of new connection establishment.

## FAQ

### When should I use IAM auth vs. Secrets Manager?

| Scenario | Use | Why |
| --- | --- | --- |
| Lambda, ECS, EKS, Fargate | IAM role | Credentials are automatic |
| EC2 with instance profile | IAM role | Credentials are automatic (from IMDS) |
| CI/CD pipeline | IAM user | Pipeline has static credentials |
| Local development | IAM user | Access keys in `~/.aws/credentials` |
| Pre-5.0 clusters | Secrets Manager | IAM auth requires 5.0+ |
| Primary user | Secrets Manager | IAM auth cannot be used with the primary user |

### Do I need to rotate credentials?

No. STS tokens are short-lived by design and only used at connection time. Once a connection is established, it stays authenticated regardless of token expiry. There's nothing to rotate at the application level.

### What happens during failover?

The connection tracker clears stale connections. When PyMongo opens new connections to the new primary, it calls STS again to authenticate the new connections.
