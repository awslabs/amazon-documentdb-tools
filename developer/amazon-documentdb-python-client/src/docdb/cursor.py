"""
Managed cursor utilities.

PyMongo cursors must be explicitly closed when you don't exhaust them.
An un-exhausted, un-closed cursor holds a server-side cursor open until
the server kills it (default 10 minutes). Under load, this depletes
the server's cursor capacity and can cause query failures.

Use managed_cursor() for any find() that uses limit(), skip(), or may
break out of iteration early.
"""

import logging
from contextlib import contextmanager
from typing import Generator

from pymongo.cursor import Cursor

logger = logging.getLogger(__name__)


@contextmanager
def managed_cursor(cursor: Cursor) -> Generator[Cursor, None, None]:
    """
    Context manager that guarantees cursor.close() is called.

    Usage:
        with managed_cursor(db.orders.find({"status": "open"}).limit(10)) as cur:
            for doc in cur:
                process(doc)
        # cursor is closed here regardless of how iteration ended

    When NOT needed:
        Iterating a cursor to exhaustion (for doc in cursor: ...) closes it
        automatically. managed_cursor() adds safety for early exits (break,
        exception, limit/skip patterns).
    """
    try:
        yield cursor
    finally:
        cursor.close()
        logger.debug("Cursor closed via managed_cursor")


def find_all(cursor: Cursor) -> list:
    """
    Convenience: exhaust a cursor into a list and close it.

    Equivalent to list(cursor) but explicit about intent.
    """
    try:
        return list(cursor)
    finally:
        cursor.close()
