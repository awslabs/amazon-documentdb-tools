"""
docdb — Amazon DocumentDB client for Python

- Enforces Amazon DocumentDB best practices.

Quickstart:
    import docdb
    from docdb.secrets import config_from_secret

    # At app startup — once
    config = config_from_secret("prod/myapp/docdb", app_name="my-service")
    docdb.init(config)

    # Anywhere in the app
    client = docdb.get_client()
    doc = client.db("mydb").orders.find_one({"_id": order_id})
"""

from importlib.metadata import PackageNotFoundError, version

from .client import DocumentDBClient, get_client, init, reset, shutdown
from .config import DocumentDBConfig
from .cursor import find_all, managed_cursor
from .plugins import (
    BaseConnectionPlugin,
    ConnectionPlugin,
    PluginConfig,
    register_plugin,
)

try:
    __version__ = version("amazon-documentdb-python-client")
except PackageNotFoundError:
    __version__ = "0.0.0-dev"

__all__ = [
    "DocumentDBClient",
    "DocumentDBConfig",
    "get_client",
    "init",
    "shutdown",
    "reset",
    "managed_cursor",
    "find_all",
    "ConnectionPlugin",
    "BaseConnectionPlugin",
    "PluginConfig",
    "register_plugin",
]
