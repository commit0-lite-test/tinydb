"""A collection of update operations for TinyDB.

They are used for updates like this:

>>> db.update(delete('foo'), where('foo') == 2)

This would delete the ``foo`` field from all documents where ``foo`` equals 2.
"""

from typing import Any, Callable, Dict


def delete(field: str) -> Callable[[Dict[str, Any]], None]:
    """Delete a given field from the document."""
    def transform(doc: Dict[str, Any]) -> None:
        if field in doc:
            del doc[field]
    return transform


def add(field: str, n: Any) -> Callable[[Dict[str, Any]], None]:
    """Add ``n`` to a given field in the document."""
    def transform(doc: Dict[str, Any]) -> None:
        if field in doc:
            doc[field] += n
        else:
            doc[field] = n
    return transform


def subtract(field: str, n: Any) -> Callable[[Dict[str, Any]], None]:
    """Subtract ``n`` from a given field in the document."""
    def transform(doc: Dict[str, Any]) -> None:
        if field in doc:
            doc[field] -= n
        else:
            doc[field] = -n
    return transform


def set(field: str, val: Any) -> Callable[[Dict[str, Any]], None]:
    """Set a given field to ``val``."""
    def transform(doc: Dict[str, Any]) -> None:
        doc[field] = val
    return transform


def increment(field: str) -> Callable[[Dict[str, Any]], None]:
    """Increment a given field in the document by 1."""
    def transform(doc: Dict[str, Any]) -> None:
        if field in doc:
            doc[field] += 1
        else:
            doc[field] = 1
    return transform


def decrement(field: str) -> Callable[[Dict[str, Any]], None]:
    """Decrement a given field in the document by 1."""
    def transform(doc: Dict[str, Any]) -> None:
        if field in doc:
            doc[field] -= 1
        else:
            doc[field] = -1
    return transform
