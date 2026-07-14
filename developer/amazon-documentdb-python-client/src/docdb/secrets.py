"""
AWS Secrets Manager integration.

The recommended pattern is to store connection credentials in Secrets 
Manager and retrieve them at startup. This module makes that the path 
of least resistance.

Supports both the native format:
{
    "username":            "appuser",
    "password":            "...",
    "engine":              "mongo",
    "host":                "cluster.cluster-xxx.us-east-1.docdb.amazonaws.com",
    "port":                27017,
    "ssl":                 true,
    "dbClusterIdentifier": "mycluster"
}

...and a custom format with DocumentDBConfig field names:
{
    "host":        "cluster.cluster-xxx.us-east-1.docdb.amazonaws.com",
    "port":        27017,
    "username":    "appuser",
    "password":    "...",
    "tls_ca_file": "/etc/ssl/certs/global-bundle.pem"
}

The secret must contain host, username, and password. All other fields
fall back to DocumentDBConfig defaults if not present.
Any keyword arguments passed to config_from_secret() override both the
secret and the defaults — use this for per-service settings like app_name
and max_pool_size that don't belong in a shared secret.
"""

import json
import logging
from dataclasses import fields as dataclass_fields
from typing import Any

try:
    import boto3
except ImportError:
    boto3 = None  # type: ignore[assignment]

from .config import DocumentDBConfig

logger = logging.getLogger(__name__)

_CONFIG_FIELD_NAMES = {f.name for f in dataclass_fields(DocumentDBConfig)}

_DEFAULT_FIELD_ALIASES: dict[str, str] = {}


def config_from_secret(
    secret_name: str,
    region: str = "us-east-1",
    secrets_client=None,
    field_aliases: dict[str, str] | None = None,
    **overrides: Any,
) -> DocumentDBConfig:
    """
    Load a DocumentDBConfig from an AWS Secrets Manager secret.

    Args:
        secret_name:    The secret name or ARN.
        region:         AWS region where the secret is stored.
        secrets_client: Optional pre-built boto3 Secrets Manager client.
                        When provided, ``region`` is ignored.
        field_aliases:  Optional mapping of secret field names to
                        DocumentDBConfig field names. Use when your secret
                        uses non-standard keys that need to map to config
                        fields (e.g., {"db_host": "host"}).
        **overrides:    Any DocumentDBConfig field values that should override
                        the secret. Common use: app_name, max_pool_size.

    Returns:
        A fully populated DocumentDBConfig.

    Example:
        config = config_from_secret(
            "prod/myapp/docdb",
            region="us-east-1",
            app_name="inventory-service",
            max_pool_size=50,
        )
    """
    if secrets_client is None and boto3 is None:
        raise ImportError(
            "boto3 is required for Secrets Manager integration. "
        )

    client = secrets_client or boto3.client("secretsmanager", region_name=region)
    response = client.get_secret_value(SecretId=secret_name)

    if "SecretString" not in response:
        raise ValueError(
            f"Secret {secret_name!r} does not contain a SecretString. "
        )

    secret: dict = json.loads(response["SecretString"])

    logger.info("Loaded config from secret: %s", secret_name)

    aliases = field_aliases if field_aliases is not None else _DEFAULT_FIELD_ALIASES
    normalized = {aliases.get(k, k): v for k, v in secret.items()}

    # Merge: secret values < config defaults < caller overrides
    # Unknown keys in the secret are silently ignored so the secret can carry
    # extra metadata (e.g., "engine", "dbClusterIdentifier") without breaking
    # deserialization
    merged = {
        k: v
        for k, v in {**normalized, **overrides}.items()
        if k in _CONFIG_FIELD_NAMES
    }

    # Secrets Manager secrets should always contain credentials — unlike
    # DocumentDBConfig (which allows None for local dev or IAM auth),
    # a secret without credentials is a misconfiguration.
    _REQUIRED_FIELDS = {"host", "username", "password"}
    missing = _REQUIRED_FIELDS - merged.keys()
    if missing:
        raise ValueError(
            f"Secret {secret_name!r} is missing required fields: "
            f"{', '.join(sorted(missing))}. "
            "Ensure the secret contains host, username, and password."
        )

    return DocumentDBConfig(**merged)
