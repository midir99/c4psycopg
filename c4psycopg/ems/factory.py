from typing import Optional, Union

from psycopg.rows import Row, RowFactory

from . import base, em


def entitymanager(
    table: str,
    pk: Union[str, tuple[str, ...]],
    columns: tuple[str, ...],
    row_factory: Optional[RowFactory[Row]] = None,
    async_=False,
) -> base.EMProto:
    return em.EntityManager(table, pk, columns, row_factory=row_factory)
