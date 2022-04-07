from typing import Any, Optional, Protocol, Union

from psycopg.rows import Row, RowFactory

Entity = dict[str, Any]
PK = Union[int, str]


class EMProto(Protocol):
    """Defines the attributes and methods that Entity Manager classes must implement."""

    row_factory: RowFactory[Row]

    def explain(self) -> str:
        ...

    def create(self, entity, conn, *, row_factory=None) -> Row:
        ...

    def create_many(
        self, entities, conn, *, returning=False, row_factory=None
    ) -> Union[int, tuple[Row, ...]]:
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
    ) -> tuple[Row, ...]:
        ...

    def find_many(
        self,
        pk_list,
        conn,
        *,
        row_factory=None,
    ) -> tuple[Row, ...]:
        ...
