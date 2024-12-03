"""Utility functions for TinyDB."""

from collections import OrderedDict, abc
from typing import Iterator, TypeVar, Generic, Type, Any, Callable, NoReturn

K = TypeVar("K")
V = TypeVar("V")
D = TypeVar("D")
T = TypeVar("T")
__all__ = ("LRUCache", "freeze", "with_typehint")


def with_typehint(baseclass: Type[T]) -> Callable[[Type[Any]], Type[T]]:
    """Add type hints from a specified class to a base class."""
    return baseclass


class LRUCache(abc.MutableMapping, Generic[K, V]):
    """A least-recently used (LRU) cache with a fixed cache size."""

    def __init__(self, capacity: int | None = None) -> None:
        self.capacity = capacity
        self.cache: OrderedDict[K, V] = OrderedDict()

    def __len__(self) -> int:
        return len(self.cache)

    def __contains__(self, key: object) -> bool:
        return key in self.cache

    def __setitem__(self, key: K, value: V) -> None:
        if key in self.cache:
            del self.cache[key]
        self.cache[key] = value
        if self.capacity is not None and len(self.cache) > self.capacity:
            self.cache.popitem(last=False)

    def __delitem__(self, key: K) -> None:
        del self.cache[key]

    def __getitem__(self, key: K) -> V:
        value = self.cache.pop(key)
        self.cache[key] = value
        return value

    def __iter__(self) -> Iterator[K]:
        return iter(self.cache)

    @property
    def lru(self) -> bool:
        """Return True if the cache is at capacity, False otherwise."""
        return self.capacity is not None and len(self.cache) >= self.capacity


class FrozenDict(dict):
    """An immutable dictionary."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._hash = None

    def __hash__(self) -> int:
        if self._hash is None:
            self._hash = hash(tuple(sorted(self.items())))
        return self._hash

    def _immutable(self, *args: Any, **kwargs: Any) -> NoReturn:
        raise TypeError("FrozenDict is immutable")

    __setitem__ = _immutable
    __delitem__ = _immutable
    clear = _immutable
    update = _immutable
    pop = _immutable
    popitem = _immutable


def freeze(obj: Any) -> Any:
    """Freeze an object by making it immutable and thus hashable."""
    if isinstance(obj, dict):
        return FrozenDict((k, freeze(v)) for k, v in obj.items())
    elif isinstance(obj, list):
        return tuple(freeze(el) for el in obj)
    elif isinstance(obj, set):
        return frozenset(freeze(el) for el in obj)
    return obj
