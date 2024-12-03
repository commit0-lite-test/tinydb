"""Contains the main component of TinyDB: the database."""

from typing import Dict, Iterator, Set, Type, Any
from . import JSONStorage
from .storages import Storage
from .table import Table, Document
from .utils import with_typehint

TableBase: Type[Table] = with_typehint(Table)


class TinyDB(TableBase):
    default_storage_class = JSONStorage
    table_class = Table

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Create a new instance of TinyDB."""
        self.default_table_name = "_default"
        storage_class = kwargs.pop("storage", self.default_storage_class)
        self._storage: Storage = storage_class(*args, **kwargs)
        self._opened = True
        self._tables: Dict[str, Table] = {}
        self._default_table: Optional[Table] = None
        self._load_tables()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def _load_tables(self):
        data = self._storage.read() or {}
        for table_name in data.keys():
            self._tables[table_name] = self.table_class(self._storage, table_name)
        if self.default_table_name not in self._tables:
            self._tables[self.default_table_name] = self.table_class(self._storage, self.default_table_name)

    def __len__(self):
        return len(self.table(self.default_table_name))

    def __iter__(self):
        return iter(self.table(self.default_table_name))

    def insert(self, document: Mapping) -> int:
        """Insert a document into the default table."""
        return self.table(self.default_table_name).insert(document)

    def insert_multiple(self, documents: Iterable[Mapping]) -> List[int]:
        """Insert multiple documents into the default table."""
        return self.table(self.default_table_name).insert_multiple(documents)

    def all(self) -> List[Document]:
        """Get all documents from the default table."""
        return self.table(self.default_table_name).all()

    def search(self, cond: QueryLike) -> List[Document]:
        """Search for documents in the default table."""
        return self.table(self.default_table_name).search(cond)

    def count(self, cond: QueryLike) -> int:
        """Count documents in the default table."""
        return self.table(self.default_table_name).count(cond)

    def contains(self, cond: Optional[QueryLike] = None, doc_id: Optional[int] = None) -> bool:
        """Check if the default table contains a matching document."""
        return self.table(self.default_table_name).contains(cond, doc_id)

    def get(self, cond: Optional[QueryLike] = None, doc_id: Optional[int] = None) -> Optional[Document]:
        """Get one document from the default table."""
        return self.table(self.default_table_name).get(cond, doc_id)

    def update(self, fields: Union[Mapping, Callable[[Mapping], None]], cond: Optional[QueryLike] = None, doc_ids: Optional[Iterable[int]] = None) -> List[int]:
        """Update documents in the default table."""
        return self.table(self.default_table_name).update(fields, cond, doc_ids)

    def remove(self, cond: Optional[QueryLike] = None, doc_ids: Optional[Iterable[int]] = None) -> List[int]:
        """Remove documents from the default table."""
        return self.table(self.default_table_name).remove(cond, doc_ids)

    def __getattr__(self, name: str) -> Any:
        """Forward all unknown attribute calls to the default table instance."""
        if name in self.__dict__:
            return self.__dict__[name]
        if self._default_table is None:
            self._default_table = self.table(self.default_table_name)
        return getattr(self._default_table, name)

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

    def __len__(self) -> int:
        """Get the total number of documents in the default table."""
        return len(self.table(self.default_table_name))

    def __iter__(self) -> Iterator[Document]:
        """Return an iterator for the default table's documents."""
        return iter(self.table(self.default_table_name))
