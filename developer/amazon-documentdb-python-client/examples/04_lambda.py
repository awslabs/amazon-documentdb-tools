"""
Example 04 — AWS Lambda

Lambda re-uses the execution environment between invocations.
Initialize the client at module level (outside the handler function) so
the connection pool is created once per container, not once per invocation.

If the client is initialized inside the handler, every invocation opens
a fresh pool and Amazon DocumentDB will see a surge of connections under any load.

IMPORTANT: Lambda has a configurable concurrency limit. If your function
can scale to N concurrent executions, Amazon DocumentDB will see up to
  N * max_pool_size
connections from this function alone. Size accordingly.
"""

import logging
import docdb
from docdb.secrets import config_from_secret
from docdb import managed_cursor

logger = logging.getLogger(__name__)

# Module-level init — runs once per Lambda container (warm or cold start)
_config = config_from_secret(
    "prod/shop/docdb",
    region="us-east-1",
    app_name="shop-order-processor",
    # Lambda concurrency * max_pool_size = total connections from this function.
    # Keep this low for Lambda. The pool doesn't benefit from being large
    # when each invocation handles a single request.
    max_pool_size=5,
    min_pool_size=1,
)
docdb.init(_config)

# Handler
def handler(event: dict, context) -> dict:
    db = docdb.get_client().db("shop")

    order_id = event.get("order_id")
    if not order_id:
        return {"statusCode": 400, "body": "missing order_id"}

    order = db.orders.find_one({"_id": order_id})
    if not order:
        return {"statusCode": 404, "body": "not found"}

    # Example - find all pending items for this order using managed_cursor
    with managed_cursor(
        db.order_items.find({"order_id": order_id, "status": "pending"})
    ) as cur:
        pending_items = [item["sku"] for item in cur]

    logger.info("Processed order %s with %d pending items", order_id, len(pending_items))
    return {"statusCode": 200, "pending_items": pending_items}
