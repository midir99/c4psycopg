"""Entity Manager module."""

import itertools
from collections.abc import Iterable
from typing import Any, Callable, Optional, Union

import psycopg
from psycopg import sql
from psycopg.rows import Row, RowFactory

from .. import queries
from . import base, utils


class EntityManager:
    """Entity Manager."""

    __slots__ = (
        "table",
        "pk",
        "columns",
        "defaults",
        "row_factory",
        "cached_queries",
    )

    def __init__(
        self,
        table: str,
        pk: str,
        columns: Iterable[str],
        defaults: Optional[dict[str, Callable[..., Any]]] = None,
        row_factory: Optional[RowFactory[Row]] = None,
    ) -> None:
        self.table = table
        self.pk = pk
        self.columns = (pk,) + (*columns,)
        self.defaults = defaults or dict()
        self.row_factory = row_factory or psycopg.rows.dict_row
        self.cached_queries = {
            "insert": queries.insert(
                self.table,
                self.columns,
                returning=False,
                named_phs=True,
            ),
            "rinsert": queries.insert(
                self.table,
                self.columns,
                returning=True,
                named_phs=True,
            ),
            "find_by_id": queries.select_by_pk(
                self.table,
                self.pk,
                self.columns,
                named_phs=True,
            ),
            "delete_by_id": queries.delete_by_pk(
                self.table,
                self.pk,
                returning=False,
                named_phs=True,
            ),
            "rdelete_by_id": queries.delete_by_pk(
                self.table,
                self.pk,
                returning=True,
                columns=self.columns,
                named_phs=True,
            ),
            "delete_many_by_id": queries.delete_many_by_pk(
                self.table,
                self.pk,
                returning=False,
                named_phs=True,
            ),
            "rdelete_many_by_id": queries.delete_many_by_pk(
                self.table,
                self.pk,
                returning=True,
                columns=self.columns,
                named_phs=True,
            ),
        }

    def add_defaults(self, entity: base.Entity) -> None:
        mv = utils.missing_values(entity, self.columns, self.defaults)
        entity.update(mv)

    @utils.default_row_factory
    def create(
        self,
        entity: base.Entity,
        conn: psycopg.Connection,
        *,
        returning=True,
        use_defaults=True,
        row_factory: Optional[RowFactory[Row]] = None,
    ) -> Union[int, Row]:
        if use_defaults:
            self.add_defaults(entity)
            q = queries.insert(
                self.table,
                self.columns,
                defaults=utils.missing_columns(entity, self.columns),
                returning=returning,
                named_phs=True,
            )
        else:
            q = (
                self.cached_queries["rinsert"]
                if returning
                else self.cached_queries["insert"]
            )
        with conn.cursor(row_factory=row_factory) as cur:
            cur.execute(q, entity)
            r = cur.fetchone() if returning else cur.rowcount
            conn.commit()
        return r

    @utils.default_row_factory
    def create_many(
        self,
        entities: tuple[base.Entity, ...],
        conn: psycopg.Connection,
        *,
        returning=True,
        use_defaults=True,
        row_factory: Optional[RowFactory[Row]] = None,
    ) -> Union[int, list[Row]]:
        # TODO: Refactor when psycopg 3.1 is released and using
        # cursor.executemany(..., returning=True) is available.
        if use_defaults:
            entities = tuple(map(self.add_defaults, entities))
            defaults_per_entity = tuple(
                utils.missing_columns(entity, self.columns) for entity in entities
            )
            q = queries.insert_many(
                self.table,
                self.columns,
                defaults_per_entity=defaults_per_entity,
                returning=returning,
            )
        else:
            q = queries.insert_many(
                self.table, self.columns, qty=len(entities), returning=returning
            )
        with conn.cursor(row_factory=row_factory) as cur:
            try:
                tuple_entities = map(
                    lambda e: utils.entity2tuple(
                        self.columns, e, incomplete=use_defaults
                    ),
                    entities,
                )
                values = tuple(itertools.chain(tuple_entities))
            except KeyError as e:
                mc = str(e).replace("'", "")
                if mc in self.columns:
                    raise ValueError(
                        "When use_defaults=False you must provide a value for all the "
                        "columns used in the INSERT query ({columns}), the value for "
                        'the column "{mc}" was missing in some of the entities '
                        "provided.".format(columns=", ".join(self.columns), mc=mc)
                    ) from e
                raise e from e
            cur.execute(q, values)
            r = cur.fetchall() if returning else cur.rowcount
            conn.commit()
        return r

    @utils.default_row_factory
    def find_by_pk(
        self,
        pk: base.PK,
        conn: psycopg.Connection,
        *,
        row_factory: Optional[RowFactory[Row]] = None,
    ) -> Optional[Row]:
        with conn.cursor(row_factory=row_factory) as cur:
            cur.execute(self._find_by_pk_query, (pk,))
            result = cur.fetchone()
        return result

    @utils.default_row_factory
    def find_many_by_pk(
        self,
        pk_list: list[base.PK],
        conn: psycopg.Connection,
        *,
        order_by: Optional[sql.Composable] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        row_factory: Optional[RowFactory[Row]] = None,
    ) -> list[Row]:
        find_many_by_pk_query = queries.select_many_by_pk(
            self.table,
            self.pk,
            self.columns,
            order_by=order_by,
            limit=limit,
            offset=offset,
            named_phs=False,
        )
        print(find_many_by_pk_query.as_string(conn))
        with conn.cursor(row_factory=row_factory) as cur:
            cur.execute(find_many_by_pk_query, (pk_list,))
            result = cur.fetchall()
        return result

    @utils.default_row_factory
    def delete_by_pk(
        self,
        pk: base.PK,
        conn: psycopg.Connection,
        *,
        row_factory: Optional[RowFactory[Row]] = None,
    ) -> Optional[Row]:
        with conn.cursor(row_factory=row_factory) as cur:
            cur.execute(self._delete_by_pk_query, (pk,))
            result = cur.fetchone()
            conn.commit()
        return result

    @utils.default_row_factory
    def delete_many_by_pk(
        self,
        pk_list: tuple[base.PK, ...],
        conn: psycopg.Connection,
        *,
        returning=True,
        row_factory: Optional[RowFactory[Row]] = None,
    ) -> Union[int, list[Row]]:
        with conn.cursor(row_factory=row_factory) as cur:
            if returning:
                cur.execute(self._delete_many_by_pk_query, pk_list)
                r = cur.fetchall()
            else:
                cur.execute(self._delete_many_by_pk_nr_query, pk_list)
                r = cur.rowcount
            conn.commit()
        return r


class AsyncEntityManager:
    ...
