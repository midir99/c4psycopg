from typing import Any, Optional, Protocol, Union, TypeVar

from psycopg.rows import Row, RowFactory

Entity = dict[str, Any]
PK = Union[int, str]
T = TypeVar("T")


class EMProto(Protocol):
    """Defines the attributes and methods that Entity Manager classes must implement."""
    table: str
    pk: T
    columns: tuple[str, ...]
    row_factory: RowFactory[Row]

    def explain(self) -> str:
        ...

    def add_defaults(self, entity) -> Entity:
        ...

    def create(self, entity, conn, *, add_defaults=True, row_factory=None) -> Row:
        ...

    def create_many(
        self,
        entities,
        conn,
        *,
        add_defaults=True,
        returning=True,
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
