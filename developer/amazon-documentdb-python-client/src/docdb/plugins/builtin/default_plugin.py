"""
DefaultPlugin — terminal plugin that calls the PyMongo method.

Always appended last (weight=1000). The chain builder adds it
unconditionally; it is never registered in the plugin registry.
"""

from __future__ import annotations

from typing import Any, Callable

from ..base import ConnectionPlugin, OperationContext, WILDCARD


class DefaultPlugin(ConnectionPlugin):

    @property
    def plugin_code(self) -> str:
        return "_default"

    @property
    def weight(self) -> int:
        return 1000

    def subscribed_methods(self) -> frozenset[str]:
        return frozenset({WILDCARD})

    def execute(
        self,
        ctx: OperationContext,
        next_fn: Callable[[OperationContext], Any],
    ) -> Any:
        collection = ctx.attributes["_pymongo_collection"]
        method = getattr(collection, ctx.method)
        return method(*ctx.args, **ctx.kwargs)
