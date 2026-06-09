"""
Amazon DocumentDB connection configuration.

All parameters are documented with the reason they exist
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)

_VALID_READ_PREFERENCES = frozenset({
    "primary",
    "primaryPreferred",
    "secondary",
    "secondaryPreferred",
    "nearest",
})


@dataclass
class DocumentDBConfig:
    # -------------------------------------------------------------------------
    # Required
    # -------------------------------------------------------------------------
    host: str
    """Cluster endpoint (cluster-id.cluster-xxx.region.docdb.amazonaws.com).
    Avoid using an instance endpoint — those bypass replica set routing."""

    # -------------------------------------------------------------------------
    # Auth
    # -------------------------------------------------------------------------
    username: str | None = None
    password: str | None = field(default=None, repr=False)
    auth_source: str = "admin"
    port: int = 27017

    iam_auth: bool = False
    """Use IAM authentication instead of username/password.

    When True, the wrapper passes authMechanism="MONGODB-AWS" to PyMongo,
    which retrieves temporary credentials from AWS STS via environment
    variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN)
    or instance/task role.

    Requirements:
      - Amazon DocumentDB 5.0+ (instance-based clusters only)
      - pip install 'pymongo[aws]'
      - IAM user/role must NOT be the cluster's primary user
      - TLS must be enabled (enforced by this wrapper)

    Credentials are only used during connection establishment. Once
    authenticated, the connection remains valid even if credentials rotate.
    """

    # -------------------------------------------------------------------------
    # TLS — enabled by default
    # -------------------------------------------------------------------------
    tls: bool = True
    """TLS is enabled by default on Amazon DocumentDB clusters and is strongly
    recommended. Can be disabled on clusters configured without TLS, but
    should not be False in production."""

    tls_ca_file: str = "/etc/ssl/certs/global-bundle.pem"
    """Path to the Amazon DocumentDB CA bundle.
    Download: https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem
    Common locations:
      - EC2/ECS container: /etc/ssl/certs/global-bundle.pem
      - Lambda (via layer): /opt/global-bundle.pem
    """

    # -------------------------------------------------------------------------
    # Replica set
    # -------------------------------------------------------------------------
    replica_set: str = "rs0"
    """Amazon DocumentDB clusters use replica set name 'rs0'.
    Without this, the driver treats the cluster as a standalone node and
    will not re-route operations after a primary failover."""

    # -------------------------------------------------------------------------
    # Read preference
    # ------------------------------------------------------------------------
    read_preference: str = "secondaryPreferred"
    """Route reads to replicas when available. Change to 'primary' only if
    your application requires read-after-write consistency."""

    # -------------------------------------------------------------------------
    # Connection pool
    # -------------------------------------------------------------------------
    max_pool_size: int = 100
    """Maximum connections per pool. Each MongoClient holds one pool per server
    in the replica set. Tune based on your Amazon DocumentDB instance connection limit
    Divide max_pool_size across all replicas x all app instances."""

    min_pool_size: int = 5
    """Keep this many connections warm. Avoids cold-start latency on burst traffic."""

    max_idle_time_ms: int = 10_000
    """Close connections idle longer than this. Prevents stale connections
    from accumulating when traffic is bursty."""

    # -------------------------------------------------------------------------
    # Timeouts
    # -------------------------------------------------------------------------
    server_selection_timeout_ms: int = 30_000
    """How long the driver waits to find a suitable server. Amazon DocumentDB
    failovers typically complete within 30 seconds from start to finish.
    This must be high enough to ride through a primary election without surfacing 
    errors to the application."""

    socket_timeout_ms: int = 0
    """How long to wait for a response on an open socket. 0 means no timeout
    (PyMongo default). Set a positive value only if you need to bound maximum
    query duration. Amazon DocumentDB implements a 2-hour server-side timeout 
    as a safety mechanism to limit runaway queries from consuming resources 
    indefinitely."""

    connect_timeout_ms: int = 10_000
    """Timeout for establishing a new connection to a server."""

    # -------------------------------------------------------------------------
    # Observability
    # -------------------------------------------------------------------------
    app_name: str | None = None
    """Appears in log output. Set this to your service name — it gives a way to 
    attribute slow queries back to the originating service in a shared cluster.
    Example: 'inventory-service', 'order-api'"""

    # -------------------------------------------------------------------------
    # Plugins
    # -------------------------------------------------------------------------
    plugins: list[Any] = field(default_factory=list)
    """List of PluginConfig instances for the middleware chain.
    Plugins are sorted by weight; DefaultPlugin is always appended last.
    Import PluginConfig from docdb.plugins:
        from docdb.plugins import PluginConfig
        config = DocumentDBConfig(host=..., plugins=[PluginConfig("retry")])
    """

    # -------------------------------------------------------------------------
    # Telemetry
    # -------------------------------------------------------------------------
    telemetry: Any = None
    """TelemetryConfig instance to enable observability (metrics, traces).
    When None, telemetry is disabled. Import TelemetryConfig from
    docdb.telemetry to configure:
        from docdb.telemetry import TelemetryConfig
        config = DocumentDBConfig(host=..., telemetry=TelemetryConfig(enabled=True))
    """

    # -------------------------------------------------------------------------
    # Additional options
    # -------------------------------------------------------------------------
    extra_options: dict[str, Any] = field(default_factory=dict)
    """Additional keyword arguments passed directly to MongoClient.
    Use for options not exposed above (e.g., compressors, event_listeners).
    Enforced settings (retryWrites, directConnection, replicaSet) cannot be
    overridden via extra_options."""

    def __post_init__(self):
        if self.read_preference not in _VALID_READ_PREFERENCES:
            raise ValueError(
                f"Invalid read_preference {self.read_preference!r}. "
                f"Must be one of: {', '.join(sorted(_VALID_READ_PREFERENCES))}"
            )
        if self.iam_auth and (self.username or self.password):
            logger.warning(
                "iam_auth=True with username/password set. "
                "IAM authentication will be used; username and password will be ignored."
            )
