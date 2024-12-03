"""Contains the :class:`base class <tinydb.storages.Storage>` for storages and
implementations.
"""

import json
import os
import warnings
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional

__all__ = ("Storage", "JSONStorage", "MemoryStorage")


def touch(path: str, create_dirs: bool) -> None:
    """Create a file if it doesn't exist yet.

    :param path: The file to create.
    :param create_dirs: Whether to create all missing parent directories.
    """
    if create_dirs:
        os.makedirs(os.path.dirname(path), exist_ok=True)

    if not os.path.exists(path):
        with open(path, "a"):
            os.utime(path, None)


class Storage(ABC):
    """The abstract base class for all Storages.

    A Storage (de)serializes the current state of the database and stores it in
    some place (memory, file on disk, ...).
    """

    @abstractmethod
    def read(self) -> Optional[Dict[str, Dict[str, Any]]]:
        """Read the current state.

        Any kind of deserialization should go here.

        Return ``None`` here to indicate that the storage is empty.
        """
        raise NotImplementedError

    @abstractmethod
    def write(self, data: Dict[str, Dict[str, Any]]) -> None:
        """Write the current state of the database to the storage.

        Any kind of serialization should go here.

        :param data: The current state of the database.
        """
        raise NotImplementedError

    def close(self) -> None:
        """Optional: Close open file handles, etc."""
        pass


class JSONStorage(Storage):
    """Store the data in a JSON file."""

    def __init__(
        self,
        path: str,
        create_dirs: bool = False,
        encoding: Optional[str] = None,
        access_mode: str = "r+",
        **kwargs: Any,
    ):
        """Create a new instance."""
        super().__init__()
        self.path = path
        self._mode = access_mode
        self.kwargs = kwargs
        self.encoding = encoding
        self.json_kwargs = {
            'sort_keys': kwargs.get('sort_keys', None),
            'indent': kwargs.get('indent', None),
            'separators': kwargs.get('separators', None),
        }
        if access_mode not in ("r", "rb", "r+", "rb+"):
            warnings.warn(
                "Using an `access_mode` other than 'r', 'rb', 'r+' or 'rb+' can cause data loss or corruption"
            )
        if any([character in self._mode for character in ("+", "w", "a")]):
            touch(path, create_dirs=create_dirs)

    def read(self) -> Optional[Dict[str, Dict[str, Any]]]:
        """Read the current state from the JSON file."""
        try:
            with open(self.path, 'r', encoding=self.encoding) as handle:
                return json.load(handle)
        except FileNotFoundError:
            return None
        except ValueError:
            # If the file is empty or contains invalid JSON, return None
            return None

    def write(self, data: Dict[str, Dict[str, Any]]) -> None:
        """Write the current state to the JSON file."""
        with open(self.path, 'w', encoding=self.encoding) as handle:
            json.dump(data, handle, **self.json_kwargs)

    def close(self) -> None:
        """Close the file handle."""
        pass  # No need to close anything as we're using 'with' statements


class MemoryStorage(Storage):
    """Store the data as JSON in memory."""

    def __init__(self):
        """Create a new instance."""
        super().__init__()
        self.memory = None

    def read(self) -> Optional[Dict[str, Dict[str, Any]]]:
        """Read the current state from memory."""
        return self.memory

    def write(self, data: Dict[str, Dict[str, Any]]) -> None:
        """Write the current state to memory."""
        self.memory = data

    def close(self) -> None:
        """No-op for MemoryStorage."""
        pass
