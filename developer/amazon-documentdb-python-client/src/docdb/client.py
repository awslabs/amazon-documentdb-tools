"""
Thread-safe singleton Amazon DocumentDB client.

Usage pattern (app startup):
    import docdb
    docdb.init(config)          # once, at process start

Usage pattern (anywhere in the app):
    client = docdb.get_client()
    db = client.db("mydb")
    result = db.orders.find_one({"_id": order_id})
"""

import logging
import os
import threading
from typing import Any

from pymongo import MongoClient
from pymongo.errors import PyMongoError

from .config import DocumentDBConfig
from .connection_tracker import ConnectionTracker
from .plugins.chain import PluginChainBuilder, PluginConfig, PluginPipeline
from .plugins.proxy import PluginAwareDatabase
from .telemetry.backend import NoOpBackend, TelemetryBackend
from .telemetry.config import TelemetryConfig, build_backend
from .telemetry.listeners import (
    DocumentDBCommandListener,
    DocumentDBHeartbeatListener,
    DocumentDBPoolListener,
    DocumentDBTopologyListener,
)

logger = logging.getLogger(__name__)

# Module-level singleton — one client per process
_instance: "DocumentDBClient | None" = None
_lock = threading.Lock()


class DocumentDBClient:
    """
    Wraps PyMongo MongoClient with Amazon DocumentDB best practices enforced at construction.

    Do not instantiate directly — use docdb.init() and docdb.get_client().
    """

    def __init__(self, config: DocumentDBConfig) -> None:
        if not config.app_name:
            logger.warning(
                "DocumentDBConfig.app_name is not set. "
                "Set it to your service name so operations can be attributed "
                "to this service within log files."
            )
        self._config = config
        self._telemetry_backend = self._build_telemetry_backend()
        self._connection_tracker = ConnectionTracker(telemetry_backend=self._telemetry_backend)
        self._client = self._build_client()
        self._connection_tracker.set_client(self._client)
        self._pipeline = self._build_pipeline()
        logger.info(
            "DocumentDBClient initialized",
            extra={
                "host": config.host,
                "replica_set": config.replica_set,
                "max_pool_size": config.max_pool_size,
                "app_name": config.app_name,
                "telemetry_enabled": not isinstance(self._telemetry_backend, NoOpBackend),
            },
        )

    def _build_telemetry_backend(self) -> TelemetryBackend:
        telemetry_config = self._config.telemetry
        if telemetry_config is None:
            return NoOpBackend()
        if not isinstance(telemetry_config, TelemetryConfig):
            raise TypeError(
                "DocumentDBConfig.telemetry must be a TelemetryConfig instance. "
                "Import it with: from docdb.telemetry import TelemetryConfig"
            )
        return build_backend(
            telemetry_config,
            host=self._config.host,
            app_name=self._config.app_name,
        )

    _MANAGED_OPTIONS = frozenset({
        "host",
        "port",
        "username",
        "password",
        "authSource",
        "tls",
        "tlsCAFile",
        "tlsAllowInvalidCertificates",
        "tlsAllowInvalidHostnames",
        "tlsInsecure",
        "replicaSet",
        "directConnection",
        "retryWrites",
        "retryReads",
        "readPreference",
        "maxPoolSize",
        "minPoolSize",
        "maxIdleTimeMS",
        "serverSelectionTimeoutMS",
        "socketTimeoutMS",
        "connectTimeoutMS",
        "appName",
        "event_listeners",
        "authMechanism",
    })

    def _build_client(self) -> MongoClient:
        conflicts = self._config.extra_options.keys() & self._MANAGED_OPTIONS
        if conflicts:
            raise ValueError(
                f"extra_options cannot override managed settings: "
                f"{', '.join(sorted(conflicts))}. "
                f"Set these via DocumentDBConfig fields instead."
            )

        tls_opts: dict = {"tls": self._config.tls}
        if self._config.tls:
            if not os.path.exists(self._config.tls_ca_file):
                raise FileNotFoundError(
                    f"TLS CA file not found: {self._config.tls_ca_file}. "
                    "Download from: https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem"
                )
            tls_opts["tlsCAFile"] = self._config.tls_ca_file

        timeout_opts: dict = {
            "serverSelectionTimeoutMS": self._config.server_selection_timeout_ms,
            "connectTimeoutMS": self._config.connect_timeout_ms,
        }
        if self._config.socket_timeout_ms:
            timeout_opts["socketTimeoutMS"] = self._config.socket_timeout_ms

        telemetry_opts: dict = {}
        event_listeners = self._build_event_listeners()
        if event_listeners:
            telemetry_opts["event_listeners"] = event_listeners

        auth_opts: dict = {}
        if self._config.iam_auth:
            auth_opts["authMechanism"] = "MONGODB-AWS"
        else:
            auth_opts["username"] = self._config.username
            auth_opts["password"] = self._config.password
            auth_opts["authSource"] = self._config.auth_source

        return MongoClient(
            **self._config.extra_options,
            host=self._config.host,
            port=self._config.port,
            **auth_opts,
            **tls_opts,
            replicaSet=self._config.replica_set,
            directConnection=False,
            retryWrites=False,
            retryReads=True,
            readPreference=self._config.read_preference,
            maxPoolSize=self._config.max_pool_size,
            minPoolSize=self._config.min_pool_size,
            maxIdleTimeMS=self._config.max_idle_time_ms,
            **timeout_opts,
            appName=self._config.app_name,
            **telemetry_opts,
        )

    def _build_event_listeners(self) -> list | None:
        # Connection tracker is always active (cleans up stale connections)
        listeners = [
            self._connection_tracker.pool_listener,
            self._connection_tracker.topology_listener,
        ]

        # Telemetry listeners are conditional
        if not isinstance(self._telemetry_backend, NoOpBackend):
            telemetry_config = self._config.telemetry
            enabled = telemetry_config.enabled_listeners

            if "command" in enabled:
                listeners.append(DocumentDBCommandListener(
                    self._telemetry_backend,
                    emit_spans=telemetry_config.emit_spans,
                ))
            if "heartbeat" in enabled:
                listeners.append(DocumentDBHeartbeatListener(self._telemetry_backend))
            if "topology" in enabled:
                listeners.append(DocumentDBTopologyListener(self._telemetry_backend))
            if "pool" in enabled:
                listeners.append(DocumentDBPoolListener(self._telemetry_backend))

        return listeners

    def _build_pipeline(self) -> PluginPipeline:
        builder = PluginChainBuilder()
        plugin_configs = [
            cfg if isinstance(cfg, PluginConfig) else PluginConfig(**cfg)
            for cfg in self._config.plugins
        ]

        # Auto-inject TelemetryPlugin if telemetry is enabled and not already listed
        if not isinstance(self._telemetry_backend, NoOpBackend):
            codes = {pc.code for pc in plugin_configs}
            if "telemetry" not in codes:
                plugin_configs.insert(0, PluginConfig("telemetry"))

        extra_options: dict[str, Any] = {"config": self._config}
        if not isinstance(self._telemetry_backend, NoOpBackend):
            extra_options["backend"] = self._telemetry_backend

        return builder.build(plugin_configs, extra_options=extra_options)

    def db(self, name: str) -> PluginAwareDatabase:
        """Return a plugin-aware database handle."""
        raw_db = self._client[name]
        return PluginAwareDatabase(raw_db, self._pipeline)

    def __getitem__(self, name: str) -> PluginAwareDatabase:
        """Support client['mydb'] access like native PyMongo."""
        return self.db(name)

    def __getattr__(self, name: str) -> PluginAwareDatabase:
        """Support client.mydb access like native PyMongo."""
        if name.startswith("_"):
            raise AttributeError(name)
        return self.db(name)

    def start_session(self, **kwargs):
        """Start a client session. Delegates to the underlying MongoClient."""
        return self._client.start_session(**kwargs)

    @property
    def raw(self) -> MongoClient:
        """
        Escape hatch to the underlying PyMongo MongoClient.
        Use only when the wrapper doesn't expose what you need.
        """
        return self._client

    def ping(self) -> bool:
        """Return True if the cluster is reachable. Useful for health checks."""
        try:
            self._client.admin.command("ping")
            return True
        except PyMongoError as exc:
            logger.error("Amazon DocumentDB ping failed: %s", exc)
            return False

    @property
    def connections(self) -> ConnectionTracker:
        """Access the connection tracker (inspect open connections per host)."""
        return self._connection_tracker

    @property
    def telemetry(self) -> TelemetryBackend:
        """Access the telemetry backend (e.g., to flush or inspect)."""
        return self._telemetry_backend

    def close(self) -> None:
        self._telemetry_backend.flush()
        self._telemetry_backend.shutdown()
        self._pipeline.close_all()
        self._client.close()
        logger.info("DocumentDBClient connection pool closed")

    # Context manager support — useful in scripts and tests
    def __enter__(self) -> "DocumentDBClient":
        return self

    def __exit__(self, *args) -> None:
        self.close()


# Module-level factory functions
def init(config: DocumentDBConfig) -> DocumentDBClient:
    """
    Initialize the singleton client. Call once at application startup.

    Raises RuntimeError if called more than once in the same process
    (instead of silently discarding the new config).
    """
    global _instance
    with _lock:
        if _instance is not None:
            raise RuntimeError(
                "DocumentDBClient is already initialized. "
                "Call docdb.init() exactly once at process startup. "
                "Use docdb.get_client() to retrieve the existing instance."
            )
        _instance = DocumentDBClient(config)
        return _instance


def get_client() -> DocumentDBClient:
    """
    Retrieve the singleton client. Must call docdb.init() first.

    Raises RuntimeError if init() has not been called — this surfaces the
    anti-pattern of creating clients on-demand rather than at startup.
    """
    if _instance is None:
        raise RuntimeError(
            "DocumentDBClient is not initialized. "
            "Call docdb.init(config) once at application startup "
            "before calling get_client()."
        )
    return _instance


def shutdown() -> None:
    """
    Gracefully close the connection pool and release the singleton.

    Call this in your application's shutdown hook (e.g., atexit, SIGTERM
    handler, FastAPI lifespan teardown, Flask teardown_appcontext).
    After calling shutdown(), init() may be called again if needed.
    """
    global _instance
    with _lock:
        if _instance is not None:
            _instance.close()
            _instance = None


def reset() -> None:
    """
    Alias for shutdown(). Exists for test readability.
    """
    shutdown()
