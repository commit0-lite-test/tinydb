"""Contains the :class:`base class <tinydb.middlewares.Middleware>` for
middlewares and implementations.
"""

from typing import Any, Dict, Optional
from tinydb import Storage


class Middleware:
    """The base class for all Middlewares.

    Middlewares hook into the read/write process of TinyDB allowing you to
    extend the behaviour by adding caching, logging, ...

    Your middleware's ``__init__`` method has to call the parent class
    constructor so the middleware chain can be configured properly.
    """

    def __init__(self, storage_cls: Any) -> None:
        self._storage_cls = storage_cls
        self.storage: Optional[Storage] = None

    def __call__(self, *args: Any, **kwargs: Any) -> "Middleware":
        """Create the storage instance and store it as self.storage."""
        self.storage = self._storage_cls(*args, **kwargs)
        return self

    def __getattr__(self, name: str) -> Any:
        """Forward all unknown attribute calls to the underlying storage."""
        return getattr(self.__dict__["storage"], name)


class CachingMiddleware(Middleware):
    """Add some caching to TinyDB.

    This Middleware aims to improve the performance of TinyDB by writing only
    the last DB state every :attr:`WRITE_CACHE_SIZE` time and reading always
    from cache.
    """

    WRITE_CACHE_SIZE = 1000

    def __init__(self, storage_cls: Any) -> None:
        super().__init__(storage_cls)
        self.cache: Optional[Dict[str, Any]] = None
        self._cache_modified_count = 0

    def read(self) -> Dict[str, Any]:
        """Read data from the cache or underlying storage."""
        if self.cache is None:
            if self.storage is None:
                raise RuntimeError("Storage is not initialized")
            self.cache = self.storage.read()
        return self.cache if self.cache is not None else {}

    def write(self, data: Dict[str, Any]) -> None:
        """Write data to the cache and possibly to the underlying storage."""
        self.cache = data
        self._cache_modified_count += 1

        if self._cache_modified_count >= self.WRITE_CACHE_SIZE:
            self.flush()

    def flush(self) -> None:
        """Flush all unwritten data to disk."""
        if self.cache is not None and self.storage is not None:
            self.storage.write(self.cache)
            self._cache_modified_count = 0

    def close(self) -> None:
        """Close the storage and flush any unwritten data."""
        self.flush()
        if self.storage is not None:
            self.storage.close()
