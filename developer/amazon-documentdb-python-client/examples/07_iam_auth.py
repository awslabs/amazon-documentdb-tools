"""
IAM Authentication Example — Verify password-less authentication works

This script connects to Amazon DocumentDB using IAM authentication
(no username/password), performs a simple operation, and shows the
authentication mechanism in use.

Prerequisites:
    - Amazon DocumentDB 5.0+ cluster with IAM authentication enabled
    - IAM role/user with rds-db:connect permission
    - Database user created with the IAM identity ARN and MONGODB-AWS mechanism
    - pip install 'amazon-documentdb-python-client[iam]'

Environment variables:
    DOCDB_HOST       — Cluster endpoint (required)
    DOCDB_CA_FILE    — Path to CA bundle (default: global-bundle.pem)

    For IAM role (Lambda, ECS, EKS, EC2):
        N/A - credentials come from instance metadata

    For IAM user (local dev, CI/CD):
        AWS_ACCESS_KEY_ID       — IAM user access key
        AWS_SECRET_ACCESS_KEY   — IAM user secret key
        AWS_SESSION_TOKEN       — (optional, for assumed roles)

Usage:
    # On EC2/Lambda/ECS with IAM role attached:
    export DOCDB_HOST="your-cluster.cluster-abc.us-east-1.docdb.amazonaws.com"
    python examples/07_iam_auth.py

    # On local dev with IAM user credentials:
    export DOCDB_HOST="your-cluster.cluster-abc.us-east-1.docdb.amazonaws.com"
    export AWS_ACCESS_KEY_ID="AKIA..."
    export AWS_SECRET_ACCESS_KEY="..."
    python examples/07_iam_auth.py
"""

import os
import sys

sys.path.insert(0, "src")

HOST = os.environ.get("DOCDB_HOST", "")
CA_FILE = os.environ.get("DOCDB_CA_FILE", "global-bundle.pem")

if not HOST:
    print("ERROR: Set DOCDB_HOST environment variable")
    sys.exit(1)

# Check pymongo[aws] is installed
try:
    import pymongo_auth_aws  # noqa: F401
except ImportError:
    print("ERROR: pymongo[aws] is not installed.")
    print("       Run: pip install 'amazon-documentdb-python-client[iam]'")
    sys.exit(1)

# Connect with IAM auth
import docdb
from docdb import DocumentDBConfig

print(f"\n{'=' * 70}")
print(f"  IAM AUTHENTICATION TEST")
print(f"  Host: {HOST}")
print(f"{'=' * 70}\n")

print("  Configuration:")
print("    iam_auth=True")
print("    (no username or password provided)\n")

config = DocumentDBConfig(
    host=HOST,
    tls=True,
    tls_ca_file=CA_FILE,
    app_name="iam-auth-test",
    iam_auth=True,
)

docdb.init(config)
client = docdb.get_client()

# Verify connection
print("  >>> client.ping()")
if client.ping():
    print(" Connected successfully with IAM authentication\n")
else:
    print(" Connection failed.")
    print("    Check:")
    print("    - Is IAM authentication enabled on the cluster?")
    print("    - Does the IAM role/user have rds-db:connect permission?")
    print("    - Was a database user created with the IAM ARN?")
    print("    - Is the cluster running Amazon DocumentDB 5.0+?")
    docdb.shutdown()
    sys.exit(1)

# Show connection details
print("  >>> Checking authentication mechanism on the connection...")
raw_client = client.raw
server_info = raw_client.admin.command("connectionStatus")
auth_info = server_info.get("authInfo", {})
users = auth_info.get("authenticatedUsers", [])
print(f"  Authenticated as: {users}\n")

# Perform a test operation
print("  >>> db.iam_test.insert_one({'test': 'iam_auth_works'})")
db = client.db("iam_auth_test")
try:
    db.iam_test.insert_one({"test": "iam_auth_works"})
    print(" Write succeeded\n")

    print("  >>> db.iam_test.find_one({'test': 'iam_auth_works'})")
    result = db.iam_test.find_one({"test": "iam_auth_works"})
    print(f" Read succeeded: {result}\n")

    db.iam_test.drop()
    print(" Cleanup complete\n")
except Exception as e:
    print(f" Operation failed: {type(e).__name__}: {e}")
    print("    If authentication succeeded but the operation failed,")
    print("    check that the database user has the correct roles.")
    db.iam_test.drop()

# Summary
docdb.shutdown()

print(f"{'=' * 70}")
print("""  SUMMARY

  IAM authentication verified:
    • No username or password in the config
    • Credentials retrieved automatically from environment/instance metadata
    • Connection established via MONGODB-AWS auth mechanism
    • Operations executed successfully

  In production, this is all you need:

    config = DocumentDBConfig(
        host="your-cluster.cluster-abc.us-east-1.docdb.amazonaws.com",
        app_name="my-service",
        iam_auth=True,
    )
""")
print(f"{'=' * 70}")
