"""
Example 03 — FastAPI application

FastAPI's lifespan context manager is the correct place to initialize
and tear down the Amazon DocumentDB client. This replaces the deprecated
@app.on_event("startup") pattern.
"""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
import docdb
from docdb.secrets import config_from_secret


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator:
    # Startup - initialize the connection pool
    config = config_from_secret(
        "prod/shop/docdb",
        region="us-east-1",
        app_name="shop-api",
        max_pool_size=50,
    )
    docdb.init(config)
    yield
    # Shutdown - drain the pool cleanly
    docdb.shutdown()


app = FastAPI(lifespan=lifespan)


@app.get("/health")
def health():
    ok = docdb.get_client().ping()
    if not ok:
        raise HTTPException(status_code=503, detail="Amazon DocumentDB unreachable")
    return {"docdb": "ok"}


@app.get("/orders/{order_id}")
def get_order(order_id: str):
    db = docdb.get_client().db("shop")
    order = db.orders.find_one({"_id": order_id})
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    order["_id"] = str(order["_id"])
    return order
