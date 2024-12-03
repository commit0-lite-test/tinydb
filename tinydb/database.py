"""Contains the main component of TinyDB: the database."""

from typing import Dict, Iterator, Set, Type, Any
from . import JSONStorage
from .storages import Storage
from .table import Table, Document
from .utils import with_typehint

TableBase: Type[Table] = with_typehint(Table)


class TinyDB(TableBase):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Create a new instance of TinyDB."""
        storage_class = kwargs.pop("storage", self.default_storage_class)
        self._storage: Storage = storage_class(*args, **kwargs)
        self._opened = True
        self._tables: Dict[str, Table] = {}
        self._default_table: Optional[Table] = None

    def table(self, name: str, **kwargs: Any) -> Table:
        """Get access to a specific table."""
        if name not in self._tables:
            table_data = self._storage.read() or {}
            if name not in table_data:
                table_data[name] = {}
                self._storage.write(table_data)
            self._tables[name] = self.table_class(self._storage, name, **kwargs)
        return self._tables[name]

    def tables(self) -> Set[str]:
        """Get the names of all tables in the database."""
        table_data = self._storage.read() or {}
        return set(table_data.keys())

    def drop_tables(self) -> None:
        """Drop all tables from the database."""
        self._tables.clear()
        self._default_table = None
        self._storage.write({})

    def drop_table(self, name: str) -> None:
        """Drop a specific table from the database."""
        if name in self._tables:
            del self._tables[name]
        if self._default_table and self._default_table.name == name:
            self._default_table = None
        table_data = self._storage.read() or {}
        if name in table_data:
            del table_data[name]
            self._storage.write(table_data)

    def close(self) -> None:
        """Close the database."""
        self._storage.close()
        self._opened = False

    def __getattr__(self, name: str) -> Any:
        """Forward all unknown attribute calls to the default table instance."""
        if self._default_table is None:
            self._default_table = self.table(self.default_table_name)
        return getattr(self._default_table, name)

    def __len__(self):
        """Get the total number of documents in the default table."""
        if self._default_table is None:
            self._default_table = self.table(self.default_table_name)
        return len(self._default_table)

    def __iter__(self) -> Iterator[Document]:
        """Return an iterator for the default table's documents."""
        if self._default_table is None:
            self._default_table = self.table(self.default_table_name)
        return iter(self._default_table)
