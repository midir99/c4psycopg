"""Entity Manager module."""

import itertools
from typing import Optional, Union

import psycopg
from psycopg.rows import Row, RowFactory
from psycopg import sql

from .. import gqueries
from . import base, utils


class EntityManager:
    """Entity Manager."""

    __slots__ = (
        "table",
        "pk",
        "columns",
        "row_factory",
        "_insert_query",
        "_find_by_pk_query",
    )

    def __init__(
        self,
        table: str,
        pk: str,
        columns: tuple[str, ...],
        row_factory: Optional[RowFactory[Row]] = None,
    ) -> None:
        self.table = table
        self.pk = pk
        self.columns = (pk,) + columns
        self.row_factory = row_factory or psycopg.rows.dict_row

        self._insert_query = gqueries.insert(
            self.table,
            self.columns,
            returning=True,
        )
        self._find_by_pk_query = gqueries.select_by_pk(
            self.table,
            self.pk,
            self.columns,
            named_ph=False,
        )

    def explain(self) -> str:
        """Returns a string that describes the queries used to operate the database."""

    def add_defaults(self, entity: base.Entity) -> base.Entity:
        entity.update(utils.entity_defaults(self.columns, entity.keys()))
        return entity

    @utils.default_row_factory
    def create(
        self,
        entity: base.Entity,
        conn: psycopg.Connection,
        *,
        add_defaults=True,
        row_factory: Optional[RowFactory[Row]] = None,
    ) -> Row:
        if add_defaults:
            entity = self.add_defaults(entity)
        with conn.cursor(row_factory=row_factory) as cur:
            cur.execute(self._insert_query, entity)
            result = cur.fetchone()
            conn.commit()
        return result

    @utils.default_row_factory
    def create_many(
        self,
        entities: tuple[base.Entity, ...],
        conn: psycopg.Connection,
        *,
        add_defaults=True,
        returning=True,
        row_factory: Optional[RowFactory[Row]] = None,
    ) -> Union[int, list[Row]]:
        if add_defaults:
            entities = map(self.add_defaults, entities)
        with conn.cursor(row_factory=row_factory) as cur:
            if returning:
                # TODO: Refactor when psycopg 3.1 is released and using
                # cursor.executemany(..., returning=True) is available.
                tuple_entities = map(
                    lambda e: utils.entity2tuple(self.columns, e), entities
                )
                values = itertools.chain(tuple_entities)
                query = gqueries.insert_many(
                    self.table, self.columns, qty=len(entities)
                )
                cur.execute(query, values)
                r = cur.fetchall()
            else:
                cur.executemany(self._insert_query, entities)
                r = cur.rowcount
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
        pk_list: tuple[base.PK, ...],
        conn: psycopg.Connection,
        *,
        order_by: Optional[sql.Composable] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        row_factory: Optional[RowFactory[Row]] = None,
    ) -> list[Row]:
        find_many_by_pk_query = gqueries.select_many_by_pk(
            self.table,
            self.pk,
            self.columns,
            order_by=order_by,
            limit=limit,
            offset=offset,
            named_ph=False,
        )
        with conn.cursor(row_factory=row_factory) as cur:
            cur.execute(find_many_by_pk_query, pk_list)
            result = cur.fetchall()
        return result


class AsyncEntityManager:
    ...
