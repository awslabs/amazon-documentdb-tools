"""
Plugin registry — maps plugin codes to factory callables.

Populated at import time by built-in modules and by user calls
to register_plugin().
"""

from __future__ import annotations

from typing import Callable

from .base import ConnectionPlugin

PLUGIN_REGISTRY: dict[str, Callable[..., ConnectionPlugin]] = {}


def register_plugin(code: str, factory: Callable[..., ConnectionPlugin]) -> None:
    """
    Register a plugin factory under the given code string.

    Factory receives **kwargs from PluginConfig.options at chain-build time.
    """
    if code in PLUGIN_REGISTRY:
        raise ValueError(f"Plugin code {code!r} is already registered.")
    PLUGIN_REGISTRY[code] = factory
