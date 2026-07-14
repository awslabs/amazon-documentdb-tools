"""
docdb.plugins — Composable plugin/middleware system.

Plugins intercept collection operations (find, insert, update, etc.)
and form an ordered chain sorted by weight. Each plugin declares which
methods it subscribes to and can modify, observe, or short-circuit
the operation before it reaches PyMongo.

Quickstart:
    from docdb.plugins import BaseConnectionPlugin, PluginConfig, register_plugin

    class MyPlugin(BaseConnectionPlugin):
        @property
        def plugin_code(self):
            return "my_plugin"

        def execute(self, ctx, next_fn):
            print(f"Before {ctx.method}")
            result = next_fn(ctx)
            print(f"After {ctx.method}")
            return result

    register_plugin("my_plugin", lambda **opts: MyPlugin())

    config = DocumentDBConfig(
        host="...",
        plugins=[PluginConfig("my_plugin")],
    )
"""

from .base import (
    INTERCEPTABLE_METHODS,
    WILDCARD,
    BaseConnectionPlugin,
    ConnectionPlugin,
    OperationContext,
)
from .chain import PluginChainBuilder, PluginConfig, PluginPipeline
from .proxy import PluginAwareCollection, PluginAwareDatabase
from .registry import PLUGIN_REGISTRY, register_plugin

# Trigger built-in plugin registrations
from . import builtin as _builtin  # noqa: F401

__all__ = [
    "ConnectionPlugin",
    "BaseConnectionPlugin",
    "OperationContext",
    "PluginConfig",
    "PluginChainBuilder",
    "PluginPipeline",
    "PluginAwareDatabase",
    "PluginAwareCollection",
    "register_plugin",
    "PLUGIN_REGISTRY",
    "INTERCEPTABLE_METHODS",
    "WILDCARD",
]
