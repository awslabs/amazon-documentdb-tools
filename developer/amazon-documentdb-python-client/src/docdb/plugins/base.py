"""
Plugin system core types.

Defines the plugin interface, base class, operation context, and the set
of PyMongo collection methods that the plugin chain intercepts.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Callable

INTERCEPTABLE_METHODS = frozenset({
    "find",
    "find_one",
    "insert_one",
    "insert_many",
    "update_one",
    "update_many",
    "replace_one",
    "delete_one",
    "delete_many",
    "aggregate",
    "bulk_write",
    "find_one_and_update",
    "find_one_and_replace",
    "find_one_and_delete",
    "count_documents",
    "distinct",
})

WILDCARD = "*"


@dataclass
class OperationContext:
    """Carries state for a single operation through the plugin chain."""

    method: str
    collection_name: str
    database_name: str
    args: tuple
    kwargs: dict[str, Any]
    attributes: dict[str, Any] = field(default_factory=dict)


class ConnectionPlugin(ABC):
    """
    Base interface for all plugins.

    Plugins intercept collection operations routed through the proxy.
    Each plugin declares which methods it cares about via subscribed_methods().
    The chain builder only inserts a plugin for methods it subscribes to.
    """

    @property
    @abstractmethod
    def plugin_code(self) -> str:
        """Unique identifier, e.g. 'telemetry', 'retry'."""

    @property
    def weight(self) -> int:
        """
        Execution order. Lower weight = outermost interceptor.
        """
        return 500

    @abstractmethod
    def subscribed_methods(self) -> frozenset[str]:
        """
        Method names this plugin intercepts.
        Use WILDCARD ("*") to subscribe to all INTERCEPTABLE_METHODS.
        """

    @abstractmethod
    def execute(
        self,
        ctx: OperationContext,
        next_fn: Callable[[OperationContext], Any],
    ) -> Any:
        """
        Intercept a single operation.

        Call next_fn(ctx) to proceed to the next plugin in the chain.
        """


class BaseConnectionPlugin(ConnectionPlugin):
    """
    Pass-through implementation. Subclass and override only what you need.
    """

    @property
    def plugin_code(self) -> str:
        return self.__class__.__name__

    def subscribed_methods(self) -> frozenset[str]:
        return frozenset({WILDCARD})

    def execute(
        self,
        ctx: OperationContext,
        next_fn: Callable[[OperationContext], Any],
    ) -> Any:
        return next_fn(ctx)
