"""
Plugin chain builder and pipeline.

PluginChainBuilder resolves plugin configs to instances, sorts by weight,
and appends DefaultPlugin. PluginPipeline pre-builds a per-method dispatch
table of closures for zero-overhead call routing.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable

from .base import (
    INTERCEPTABLE_METHODS,
    WILDCARD,
    ConnectionPlugin,
    OperationContext,
)
from .builtin.default_plugin import DefaultPlugin
from .registry import PLUGIN_REGISTRY


@dataclass
class PluginConfig:
    """Declares a plugin to include in the chain."""

    code: str
    options: dict[str, Any] = field(default_factory=dict)


class PluginPipeline:
    """
    Pre-built, per-method dispatch table.

    For each method in INTERCEPTABLE_METHODS, a closure chain is built
    at init time. Per-call cost is a single dict lookup + closure invocation.
    """

    def __init__(self, plugins: list[ConnectionPlugin]) -> None:
        self._plugins = plugins
        self._dispatch: dict[str, Callable[[OperationContext], Any]] = {}
        for method in INTERCEPTABLE_METHODS:
            self._dispatch[method] = self._build_chain_for_method(method, plugins)

    def _build_chain_for_method(
        self,
        method: str,
        plugins: list[ConnectionPlugin],
    ) -> Callable[[OperationContext], Any]:
        active = [
            p for p in plugins
            if WILDCARD in p.subscribed_methods()
            or method in p.subscribed_methods()
        ]

        def _make_chain(remaining: list[ConnectionPlugin]) -> Callable[[OperationContext], Any]:
            head = remaining[0]
            if len(remaining) == 1:
                return lambda ctx: head.execute(ctx, None)  # type: ignore[arg-type]
            tail = _make_chain(remaining[1:])
            return lambda ctx, _h=head, _t=tail: _h.execute(ctx, _t)

        return _make_chain(active)

    def invoke(self, ctx: OperationContext) -> Any:
        chain_fn = self._dispatch.get(ctx.method)
        if chain_fn is None:
            raise ValueError(f"Method {ctx.method!r} is not interceptable")
        return chain_fn(ctx)

    def close_all(self) -> None:
        """Close all plugins that implement a close() method."""
        for plugin in self._plugins:
            if hasattr(plugin, "close") and callable(plugin.close):
                plugin.close()


class PluginChainBuilder:
    """Resolves PluginConfig specs to instances, sorts by weight, appends DefaultPlugin."""

    def build(
        self,
        plugin_configs: list[PluginConfig],
        extra_options: dict[str, Any] | None = None,
    ) -> PluginPipeline:
        """
        Build the pipeline from plugin configs.

        extra_options are merged into each plugin's options (e.g., telemetry
        backend injected by DocumentDBClient).
        """
        plugins: list[ConnectionPlugin] = []
        merged_extra = extra_options or {}

        for cfg in plugin_configs:
            factory = PLUGIN_REGISTRY.get(cfg.code)
            if factory is None:
                raise ValueError(
                    f"Unknown plugin code {cfg.code!r}. "
                    f"Available: {sorted(PLUGIN_REGISTRY)}"
                )
            opts = {**merged_extra, **cfg.options}
            plugins.append(factory(**opts))

        plugins.sort(key=lambda p: p.weight)
        plugins.append(DefaultPlugin())

        return PluginPipeline(plugins)
