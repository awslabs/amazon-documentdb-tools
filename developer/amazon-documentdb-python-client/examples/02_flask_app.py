"""
Example 02 — Flask application

The MongoClient must be initialized ONCE when the app starts, not per
request. Creating a new client per request opens a new connection pool
on every call and can exhaust Amazon DocumentDB's connection limit under any
meaningful load.

This example uses an app factory pattern with Flask.
"""

import atexit

from flask import Flask, jsonify
import docdb
from docdb.secrets import config_from_secret


def create_app() -> Flask:
    app = Flask(__name__)

    # Initialize at startup. The connection pool is created here (once).
    config = config_from_secret(
        "prod/shop/docdb",
        region="us-east-1",
        app_name="shop-api",          # attributes slow queries in profiler
        max_pool_size=50,             # tune to instance size / replica count
    )
    docdb.init(config)
    atexit.register(docdb.shutdown)

    @app.route("/health")
    def health():
        ok = docdb.get_client().ping()
        return jsonify({"docdb": "ok" if ok else "unreachable"}), 200 if ok else 503

    @app.route("/orders/<order_id>")
    def get_order(order_id):
        # get_client() returns the singleton (no new connection is opened)
        db = docdb.get_client().db("shop")
        order = db.orders.find_one({"_id": order_id})
        if not order:
            return jsonify({"error": "not found"}), 404
        order["_id"] = str(order["_id"])
        return jsonify(order)

    return app


if __name__ == "__main__":
    create_app().run(debug=True)
