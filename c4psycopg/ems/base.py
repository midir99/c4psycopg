from typing import Any, Callable, Optional, Protocol, TypeVar, Union

from psycopg.rows import Row, RowFactory

Entity = dict[str, Any]
PK = Union[int, str]
PKType = TypeVar("PKType")


class EMProto(Protocol[PKType, Row]):
    """Defines the attributes and methods that Entity Manager classes must implement."""

    table: str
    pk: PKType
    columns: frozenset[str]
    defaults: dict[str, Callable[..., Any]]
    row_factory: RowFactory[Row]

    def count(self) -> int:
        ...

    def add_defaults(self, entity) -> None:
        ...

    def create(
        self,
        entity,
        conn,
        *,
        returning=True,
        add_defaults=True,
        row_factory=None,
    ) -> Union[int, Row]:
        ...

    def create_many(
        self,
        entities,
        conn,
        *,
        returning=True,
        add_defaults=True,
        row_factory=None,
    ) -> Union[int, list[Row]]:
        ...

    def find_by_pk(self, pk, conn, *, row_factory=None) -> Optional[Row]:
        ...

    def find_many_by_pk(
        self,
        pk_list,
        conn,
        *,
        order_by=None,
        limit=None,
        offset=None,
        row_factory=None,
    ) -> list[Row]:
        ...

    def find_many(
        self,
        pk_list,
        conn,
        *,
        row_factory=None,
    ) -> list[Row]:
        ...

    def delete_by_pk(self, pk, conn, *, row_factory=None) -> Optional[Row]:
        ...

    def delete_many_by_pk(
        self, pk_list, conn, *, returning=True, row_factory=None
    ) -> Union[int, list[Row]]:
        ...
