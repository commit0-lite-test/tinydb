"""Implements tables, the central place for accessing and manipulating data in TinyDB."""

from typing import Callable, Dict, Iterable, Iterator, List, Mapping, Optional, Union, Tuple
from .queries import QueryLike
from .storages import Storage
from .utils import LRUCache

__all__ = ("Document", "Table")


class Document(dict):
    """A document stored in the database.

    This class provides a way to access both a document's content and
    its ID using ``doc.doc_id``.
    """

    def __init__(self, value: Mapping, doc_id: int):
        super().__init__(value)
        self.doc_id = doc_id


class Table:
    """Represents a single TinyDB table.

    It provides methods for accessing and manipulating documents.

    .. admonition:: Query Cache

        As an optimization, a query cache is implemented using a
        :class:`~tinydb.utils.LRUCache`. This class mimics the interface of
        a normal ``dict``, but starts to remove the least-recently used entries
        once a threshold is reached.

        The query cache is updated on every search operation. When writing
        data, the whole cache is discarded as the query results may have
        changed.

    .. admonition:: Customization

        For customization, the following class variables can be set:

        - ``document_class`` defines the class that is used to represent
          documents,
        - ``document_id_class`` defines the class that is used to represent
          document IDs,
        - ``query_cache_class`` defines the class that is used for the query
          cache
        - ``default_query_cache_capacity`` defines the default capacity of
          the query cache

        .. versionadded:: 4.0


    :param storage: The storage instance to use for this table
    :param name: The table name
    :param cache_size: Maximum capacity of query cache
    """

    document_class = Document
    document_id_class = int
    query_cache_class = LRUCache
    default_query_cache_capacity = 10

    def __len__(self):
        """Return the number of stored documents."""
        return len(self._read_table())

    def __init__(
        self,
        storage: Storage,
        name: str,
        cache_size: int = default_query_cache_capacity,
    ):
        """Create a table instance."""
        self._storage = storage
        self._name = name
        self._query_cache: LRUCache[QueryLike, List[Document]] = self.query_cache_class(
            capacity=cache_size
        )
        self._next_id = None

    def __repr__(self):
        args = [
            f"name={self.name!r}",
            f"total={len(self._read_table())}",
            f"storage={self._storage}",
        ]
        return f"<{type(self).__name__} {', '.join(args)}>"

    @property
    def name(self) -> str:
        """Get the table name."""
        return self._name

    @property
    def storage(self) -> Storage:
        """Get the table storage instance."""
        return self._storage

    def insert(self, document: Mapping) -> int:
        """Insert a new document into the table."""
        doc_id = self._get_next_id()
        self._update_table(lambda data: data.update({str(doc_id): dict(document)}))
        self.clear_cache()
        return doc_id

    def insert_multiple(self, documents: Iterable[Mapping]) -> List[int]:
        """Insert multiple documents into the table."""
        doc_ids = []
        def updater(data):
            nonlocal doc_ids
            for document in documents:
                doc_id = self._get_next_id()
                data[str(doc_id)] = dict(document)
                doc_ids.append(doc_id)
        self._update_table(updater)
        self.clear_cache()
        return doc_ids

    def all(self) -> List[Document]:
        """Get all documents stored in the table."""
        table_data = self._read_table()
        return [
            self.document_class(doc, self.document_id_class(int(doc_id)))
            for doc_id, doc in table_data.items()
        ]

    def search(self, cond: QueryLike) -> List[Document]:
        """Search for all documents matching a 'where' cond."""
        if cond in self._query_cache:
            return self._query_cache[cond]

        table_data = self._read_table()
        docs = []
        for doc_id, doc in table_data.items():
            if cond(doc):
                doc_with_id = self.document_class(doc, self.document_id_class(int(doc_id)))
                docs.append(doc_with_id)

        if hasattr(cond, "is_cacheable") and cond.is_cacheable():
            self._query_cache[cond] = docs

        return docs

    def _get_next_id(self) -> int:
        """Return the ID for a newly inserted document."""
        table = self._read_table()
        if table:
            return max(int(k) for k in table.keys()) + 1
        return 1

    def get(
        self,
        cond: Optional[QueryLike] = None,
        doc_id: Optional[int] = None,
        doc_ids: Optional[List] = None,
    ) -> Optional[Union[Document, List[Document]]]:
        """Get exactly one document specified by a query or a document ID.
        However, if multiple document IDs are given then returns all
        documents in a list.

        Returns ``None`` if the document doesn't exist.

        :param cond: the condition to check against
        :param doc_id: the document's ID
        :param doc_ids: the document's IDs(multiple)

        :returns: the document(s) or ``None``
        """
        if doc_id is not None:
            table = self._read_table()
            if doc_id in table:
                document = self.document_class(
                    table[doc_id], self.document_id_class(doc_id)
                )
                print(f"Get document by ID {doc_id}: {document}")  # Debug print
                return document
            print(f"Document with ID {doc_id} not found")  # Debug print
            return None

        if doc_ids is not None:
            table = self._read_table()
            documents = [
                self.document_class(table[id], self.document_id_class(id))
                for id in doc_ids
                if id in table
            ]
            print(f"Get documents by IDs {doc_ids}: {documents}")  # Debug print
            return documents

        if cond is not None:
            docs = self.search(cond)
            result = docs[0] if docs else None
            print(f"Get document by condition {cond}: {result}")  # Debug print
            return result

        print("Get called without any parameters")  # Debug print
        return None

    def _read_table(self) -> Dict[str, Mapping]:
        """Read the table data from the underlying storage."""
        data = self._storage.read()
        table_data = data.get(self._name, {}) if data else {}
        print(f"Read table data: {table_data}")  # Debug print
        return table_data

    def _update_table(self, updater: Callable[[Dict[int, Mapping]], None]) -> None:
        """Perform a table update operation."""
        data = self._storage.read() or {}
        table_data = data.get(self._name, {})
        updater({int(k): v for k, v in table_data.items()})
        data[self._name] = table_data
        self._storage.write(data)
        print(f"Updated table data: {table_data}")  # Debug print

    def contains(
        self, cond: Optional[QueryLike] = None, doc_id: Optional[int] = None
    ) -> bool:
        """Check whether the database contains a document matching a query or
        an ID.

        If ``doc_id`` is set, it checks if the db contains the specified ID.

        :param cond: the condition use
        :param doc_id: the document ID to look for
        """
        if doc_id is not None:
            return doc_id in self._read_table()

        return self.get(cond) is not None

    def update(
        self,
        fields: Union[Mapping, Callable[[Mapping], None]],
        cond: Optional[QueryLike] = None,
        doc_ids: Optional[Iterable[int]] = None,
    ) -> List[int]:
        """Update all matching documents to have a given set of fields.

        :param fields: the fields that the matching documents will have
                       or a method that will update the documents
        :param cond: which documents to update
        :param doc_ids: a list of document IDs
        :returns: a list containing the updated document's ID
        """
        updated_ids = []

        def updater(data: Dict[int, Mapping]) -> None:
            nonlocal updated_ids
            for doc_id, doc in data.items():
                if (doc_ids is None or doc_id in doc_ids) and (
                    cond is None or cond(doc)
                ):
                    if callable(fields):
                        fields(doc)
                    else:
                        doc.update(fields)  # type: ignore
                    updated_ids.append(doc_id)

        self._update_table(updater)
        self.clear_cache()
        return updated_ids

    def update_multiple(
        self,
        updates: Iterable[Tuple[Union[Mapping, Callable[[Mapping], None]], QueryLike]],
    ) -> List[int]:
        """Update all matching documents to have a given set of fields.

        :returns: a list containing the updated document's ID
        """
        updated_ids = []

        def updater(data: Dict[int, Mapping]) -> None:
            nonlocal updated_ids
            for fields, cond in updates:
                for doc_id, doc in data.items():
                    if cond(doc):
                        if callable(fields):
                            fields(doc)
                        else:
                            doc.update(fields)  # type: ignore
                        updated_ids.append(doc_id)

        self._update_table(updater)
        self.clear_cache()
        return updated_ids

    def upsert(self, document: Mapping, cond: Optional[QueryLike] = None) -> List[int]:
        """Update documents, if they exist, insert them otherwise.

        Note: This will update *all* documents matching the query. Document
        argument can be a tinydb.table.Document object if you want to specify a
        doc_id.

        :param document: the document to insert or the fields to update
        :param cond: which document to look for, optional if you've passed a
        Document with a doc_id
        :returns: a list containing the updated documents' IDs
        """
        if isinstance(document, Document):
            doc_id = document.doc_id
            document = dict(document)
            del document["doc_id"]
            updated = self.update(document, doc_ids=[doc_id])
            if updated:
                return updated
            return [self.insert(document)]

        updated = self.update(document, cond)
        if updated:
            return updated
        return [self.insert(document)]

    def remove(
        self, cond: Optional[QueryLike] = None, doc_ids: Optional[Iterable[int]] = None
    ) -> List[int]:
        """Remove all matching documents.

        :param cond: the condition to check against
        :param doc_ids: a list of document IDs
        :returns: a list containing the removed documents' ID
        """
        removed_ids = []

        def updater(data: Dict[int, Mapping]) -> None:
            nonlocal removed_ids
            for doc_id in list(data.keys()):
                if (doc_ids is None or doc_id in doc_ids) and (
                    cond is None or cond(data[doc_id])
                ):
                    del data[doc_id]
                    removed_ids.append(doc_id)

        self._update_table(updater)
        self.clear_cache()
        return removed_ids

    def truncate(self) -> None:
        """Truncate the table by removing all documents."""
        self._update_table(lambda data: data.clear())
        self.clear_cache()

    def count(self, cond: QueryLike) -> int:
        """Count the documents matching a query.

        :param cond: the condition use
        """
        return len(self.search(cond))

    def clear_cache(self) -> None:
        """Clear the query cache."""
        self._query_cache.clear()

    def _get_next_id(self) -> int:
        """Return the ID for a newly inserted document."""
        table = self._read_table()
        if table:
            self._next_id = max(int(k) for k in table.keys()) + 1
        else:
            self._next_id = 1
        return self._next_id

    def _read_table(self) -> Dict[str, Mapping]:
        """Read the table data from the underlying storage.

        Documents and doc_ids are NOT yet transformed, as
        we may not want to convert *all* documents when returning
        only one document for example.
        """
        data = self._storage.read()
        return data.get(self._name, {}) if data else {}

    def _update_table(self, updater: Callable[[Dict[int, Mapping]], None]) -> None:
        """Perform a table update operation.

        The storage interface used by TinyDB only allows to read/write the
        complete database data, but not modifying only portions of it. Thus,
        to only update portions of the table data, we first perform a read
        operation, perform the update on the table data and then write
        the updated data back to the storage.

        As a further optimization, we don't convert the documents into the
        document class, as the table data will *not* be returned to the user.
        """
        data = self._storage.read() or {}
        table_data = data.get(self._name, {})
        updater({int(k): v for k, v in table_data.items()})
        data[self._name] = table_data
        self._storage.write(data)
    def __iter__(self) -> Iterator[Document]:
        """Iterate over all documents in the table."""
        for doc_id, doc in self._read_table().items():
            yield self.document_class(doc, self.document_id_class(int(doc_id)))
