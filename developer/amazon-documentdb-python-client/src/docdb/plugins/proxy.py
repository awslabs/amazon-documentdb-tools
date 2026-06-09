"""
Proxy layer — PluginAwareDatabase and PluginAwareCollection.

Transparent delegation proxies that intercept INTERCEPTABLE_METHODS
and route them through the plugin pipeline. All other attribute access
delegates directly to the underlying PyMongo objects.
"""

from __future__ import annotations

from typing import Any

from pymongo.collection import Collection
from pymongo.database import Database

from .base import INTERCEPTABLE_METHODS, OperationContext
from .chain import PluginPipeline


class PluginAwareCollection:
    """
    Transparent proxy for pymongo.Collection.

    Interceptable methods route through the PluginPipeline.
    All other attributes delegate directly to the underlying Collection.
    """

    __slots__ = ("_collection", "_pipeline", "_db_name", "_interceptors")

    def __init__(
        self,
        collection: Collection,
        pipeline: PluginPipeline,
        db_name: str,
    ) -> None:
        object.__setattr__(self, "_collection", collection)
        object.__setattr__(self, "_pipeline", pipeline)
        object.__setattr__(self, "_db_name", db_name)
        object.__setattr__(self, "_interceptors", {
            m: self._make_interceptor(m) for m in INTERCEPTABLE_METHODS
        })

    def __getattr__(self, name: str) -> Any:
        if name in INTERCEPTABLE_METHODS:
            return self._interceptors[name]
        attr = getattr(self._collection, name)
        if isinstance(attr, Collection):
            return PluginAwareCollection(attr, self._pipeline, self._db_name)
        if name in _COL_METHODS_RETURNING_COLLECTION and callable(attr):
            return self._wrap_collection_method(attr)
        return attr

    def _wrap_collection_method(self, method):
        pipeline = self._pipeline
        db_name = self._db_name

        def wrapper(*args, **kwargs):
            result = method(*args, **kwargs)
            if isinstance(result, Collection):
                return PluginAwareCollection(result, pipeline, db_name)
            return result
        return wrapper

    def __getitem__(self, name: str) -> PluginAwareCollection:
        sub = self._collection[name]
        return PluginAwareCollection(sub, self._pipeline, self._db_name)

    def _make_interceptor(self, method: str):
        collection = self._collection
        pipeline = self._pipeline
        db_name = self._db_name
        col_name = collection.name

        def interceptor(*args: Any, **kwargs: Any) -> Any:
            ctx = OperationContext(
                method=method,
                collection_name=col_name,
                database_name=db_name,
                args=args,
                kwargs=kwargs,
                attributes={"_pymongo_collection": collection},
            )
            return pipeline.invoke(ctx)
        return interceptor

    def __repr__(self) -> str:
        return repr(self._collection)

    def __eq__(self, other) -> bool:
        if isinstance(other, PluginAwareCollection):
            return self._collection == other._collection
        if isinstance(other, Collection):
            return self._collection == other
        return NotImplemented

    def __hash__(self) -> int:
        return hash(self._collection)

    def __bool__(self) -> bool:
        return bool(self._collection)


_DB_METHODS_RETURNING_COLLECTION = frozenset({
    "get_collection",
    "create_collection",
    "with_options",
})

_COL_METHODS_RETURNING_COLLECTION = frozenset({
    "with_options",
})


class PluginAwareDatabase:
    """
    Transparent proxy for pymongo.Database.

    Returns PluginAwareCollection from attribute/item access.
    Everything else delegates to the underlying Database.
    """

    __slots__ = ("_database", "_pipeline")

    def __init__(self, database: Database, pipeline: PluginPipeline) -> None:
        object.__setattr__(self, "_database", database)
        object.__setattr__(self, "_pipeline", pipeline)

    def __getattr__(self, name: str) -> Any:
        attr = getattr(self._database, name)
        if isinstance(attr, Collection):
            return PluginAwareCollection(attr, self._pipeline, self._database.name)
        if name in _DB_METHODS_RETURNING_COLLECTION and callable(attr):
            return self._wrap_collection_method(attr)
        return attr

    def __getitem__(self, name: str) -> PluginAwareCollection:
        col = self._database[name]
        return PluginAwareCollection(col, self._pipeline, self._database.name)

    def _wrap_collection_method(self, method):
        pipeline = self._pipeline
        db_name = self._database.name

        def wrapper(*args, **kwargs):
            result = method(*args, **kwargs)
            if isinstance(result, Collection):
                return PluginAwareCollection(result, pipeline, db_name)
            return result
        return wrapper

    def __repr__(self) -> str:
        return repr(self._database)

    def __eq__(self, other) -> bool:
        if isinstance(other, PluginAwareDatabase):
            return self._database == other._database
        if isinstance(other, Database):
            return self._database == other
        return NotImplemented

    def __hash__(self) -> int:
        return hash(self._database)

    def __bool__(self) -> bool:
        return bool(self._database)

    @property
    def raw(self) -> Database:
        """Escape hatch to the underlying pymongo.Database."""
        return self._database
