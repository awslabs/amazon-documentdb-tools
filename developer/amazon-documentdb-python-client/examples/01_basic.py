"""
Example 01 — Basic usage with explicit config

Use this pattern in scripts or local development.
"""

import docdb
from docdb import DocumentDBConfig, managed_cursor

# Build config manually. In production, prefer secrets.config_from_secret().
config = DocumentDBConfig(
    host="mycluster.cluster-abc123.us-east-1.docdb.amazonaws.com",
    username="appuser",
    password="changeme",
    app_name="example-script",  # shows up in slow query logs
    tls_ca_file="/etc/ssl/certs/global-bundle.pem",
)

# Initialize once. This creates the connection pool.
docdb.init(config)

client = docdb.get_client()
db = client.db("shop")

# --- Simple find_one (cursor exhausted immediately and no managed_cursor needed)
order = db.orders.find_one({"status": "pending"})
print(order)

# --- find() with limit — use managed_cursor to guarantee close()
with managed_cursor(db.orders.find({"status": "pending"}).limit(10)) as cursor:
    for doc in cursor:
        print(doc["_id"])

# --- Insert
db.orders.insert_one({"item": "widget", "qty": 5, "status": "new"})

# --- Health check
if client.ping():
    print("cluster is reachable")
