import functools
from typing import Literal, Optional

from psycopg import sql

COLUMNS_ORDER = Literal["ASC", "DESC"]
NULLS_ORDER = Literal["FIRST", "LAST"]


@functools.cache
def rows_with_phs(rows=1, phs=1) -> sql.Composed:
    """Generates PostgreSQL ROWs literals with placeholders.

    Args:
      rows: The quantity of tuples to return.
      phs: The quantity of placeholders inside each tuple.

    Returns:
        A psycopg.sql.Composed, here you can see an example:

        tpls = rows_with_phs(3, 5)
        print(tpls.as_string(conn))

        Result:
        (%s, %s, %s, %s, %s),(%s, %s, %s, %s, %s),(%s, %s, %s, %s, %s)
    """
    row = sql.SQL("({})").format(sql.SQL(",").join((sql.Placeholder(),) * phs))
    return sql.SQL(",").join((row,) * rows)


@functools.cache
def csplaceholders(columns: tuple[str, ...], named_phs=True) -> sql.Composed:
    """Generates a psycopg.sql.Composed of Comma Separated Placeholders.

    Args:
      columns: a tuple containing the names of columns.
      named_phs: If True placeholders will be named placeholders (%(username)s),
      otherwise they will be simple placeholders (%s).

    Returns:
      A psycopg.sql.Composed with placeholders, here you can see an example:

      csphs_named = csidentifiers(("username", "email"), named_phs=True)
      csphs_simple = csidentifiers(("username", "email"), named_phs=False)
      print(csphs_named.as_string(conn))
      print(csphs_simple.as_string(conn))

      Result:
      %(username)s,%(email)s
      %s,%s
    """
    if named_phs:
        return sql.SQL(",").join(map(sql.Placeholder, columns))
    return sql.SQL(",").join((sql.Placeholder(),) * len(columns))


@functools.cache
def csidentifiers(columns: tuple[str, ...]) -> sql.Composed:
    """Generates a psycopg.sql.Composed of Comma Separated Identifiers.

    Args:
      columns: a tuple containing the names of columns.

    Returns:
      A psycopg.sql.Composed with identifiers, here you can see an example:

      csi = csidentifiers(("username", "email"))
      print(csi.as_string(conn))

      Result:
      "username","email"
    """
    return sql.SQL(",").join(map(sql.Identifier, columns))


@functools.cache
def ob(
    column: str,
    order: Optional[COLUMNS_ORDER] = None,
    nulls: Optional[NULLS_ORDER] = None,
) -> sql.Composed:
    """Generates a PostgreSQL sort expression to use it in an ORDER BY clause.

    Args:
      column: The name of the column that will be used to generate the sort expression.
      order: They KEYWORD (ASC or DESC) that will follow the column name, used to
        describe the sort direction to ascending or descending.
      nulls: If specified, NULLS FIRST or NULLS LAST is appended to the end of the sort
        expression.

    Returns:
      A psycopg.sql.Composed constructed with the parameters of the function. Here are
      some examples:

      by_name = ob("name")
      by_email_asc = ob("email", order="ASC")
      by_username_desc_nulls_first = ob("username", order="DESC", nulls="FIRST")
      by_last_name_nulls_last = ob("last_name", nulls="LAST")
      print(by_name)
      print(by_email_asc)
      print(by_username_desc_nulls_first)
      print(by_last_name_nulls_last)

      Result:
      "name"
      "email" ASC
      "username" DESC NULLS FIRST
      "last_name" NULLS LAST
    """
    stmt = "{column}"
    params = {
        "column": sql.Identifier(column),
    }
    if order:
        stmt += " {column_ordering}"
        params["column_ordering"] = sql.SQL(order)
    if nulls:
        stmt += " NULLS {nulls_ordering}"
        params["nulls_ordering"] = sql.SQL(nulls)
    return sql.SQL(stmt).format(**params)


def commas(*composed_list: sql.Composed) -> sql.Composed:
    """Separates psycopg.sql.Composed with commas.

    Args:
      *composed_list: A list with psycopg.sql.Composed items.

    Returns:
      A list of psycopg.sql.Composed items separated with commas.
    """
    return sql.SQL(",").join(composed_list)


def insert(
    table: str,
    columns: tuple[str, ...],
    returning=False,
    named_phs=True,
) -> sql.Composed:
    stmt = "INSERT INTO {table} ({columns}) VALUES ({phs})"
    params = {
        "table": sql.Identifier(table),
        "columns": csidentifiers(columns),
        "phs": csplaceholders(columns, named_phs=named_phs),
    }
    if returning:
        stmt += " RETURNING {columns}"
    return sql.SQL(stmt).format(**params)


def insert_many(
    table: str,
    columns: tuple[str, ...],
    qty=1,
    returning=False,
) -> sql.Composed:
    stmt = "INSERT INTO {table} ({columns}) VALUES {phs}"
    params = {
        "table": sql.Identifier(table),
        "columns": csidentifiers(columns),
        "phs": rows_with_phs(qty, len(columns)),
    }
    if returning:
        stmt += " RETURNING {columns}"
    return sql.SQL(stmt).format(**params)


def select_by_pk(
    table: str,
    pk_column: str,
    columns: tuple[str, ...],
    named_phs=True,
) -> sql.Composed:
    """It returns only 1 record."""
    return sql.SQL("SELECT {columns} FROM {table} WHERE {pk_column} = {phs}").format(
        columns=csidentifiers(columns),
        table=sql.Identifier(table),
        pk_column=sql.Identifier(pk_column),
        phs=sql.Placeholder(pk_column) if named_phs else sql.Placeholder(),
    )


def select_many_by_pk(
    table: str,
    pk_column: str,
    columns: tuple[str, ...],
    order_by: Optional[sql.Composable] = None,
    limit: Optional[int] = None,
    offset: Optional[int] = None,
    named_phs=True,
) -> sql.Composed:
    """
    It returns more than 1 record, so records must be:
    - orderable
    - paginable
    """
    stmt = "SELECT {columns} FROM {table} WHERE {pk_column} = ANY({phs})"
    params = {
        "columns": csidentifiers(columns),
        "table": sql.Identifier(table),
        "pk_column": sql.Identifier(pk_column),
        "phs": sql.Placeholder(pk_column) if named_phs else sql.Placeholder(),
    }
    if order_by:
        stmt += " ORDER BY {order_by}"
        params["order_by"] = order_by
    if limit:
        stmt += " LIMIT {limit}"
        params["limit"] = sql.Literal(limit)
    if offset:
        stmt += " OFFSET {offset}"
        params["offset"] = sql.Literal(offset)
    return sql.SQL(stmt).format(**params)


def select_many(
    table: str,
    pk_column: str,
    columns: tuple[str, ...],
    order_by: Optional[sql.Composable] = None,
    limit: Optional[int] = None,
    offset: Optional[int] = None,
    named_phs=True,
) -> sql.Composed:
    """
    It returns more than 1 record, so records must be:
    - orderable
    - paginable
    """


def select_by_cpk(
    table: str,
    pk_columns: tuple[str, ...],
    columns: tuple[str, ...],
    named_phs=True,
) -> sql.Composed:
    """It returns only 1 record."""
    return sql.SQL(
        "SELECT {columns} FROM {table} WHERE ({pk_columns}) = ({phs})"
    ).format(
        columns=csidentifiers(columns),
        table=sql.Identifier(table),
        pk_columns=csidentifiers(pk_columns),
        phs=csplaceholders(pk_columns, named_phs=named_phs),
    )


def select_many_by_cpk(
    table: str,
    pk_columns: tuple[str, ...],
    columns: tuple[str, ...],
    qty=1,
    order_by: Optional[sql.Composable] = None,
    limit: Optional[int] = None,
    offset: Optional[int] = None,
) -> sql.Composed:
    """
    It returns more than 1 record, so records must be:
    - orderable
    - paginable
    """
    stmt = "SELECT {columns} FROM {table} WHERE ({pk_columns}) IN ({phs})"
    params = {
        "columns": csidentifiers(columns),
        "table": sql.Identifier(table),
        "pk_columns": csidentifiers(pk_columns),
        "phs": rows_with_phs(qty, len(pk_columns)),
    }
    if order_by:
        stmt += " ORDER BY {order_by}"
        params["order_by"] = order_by
    if limit:
        stmt += " LIMIT {limit}"
        params["limit"] = sql.Literal(limit)
    if offset:
        stmt += " OFFSET {offset}"
        params["offset"] = sql.Literal(offset)
    return sql.SQL(stmt).format(**params)


def delete_by_pk(
    table: str,
    pk_column: str,
    columns: tuple[str, ...],
    returning=False,
    named_phs=True,
) -> sql.Composed:
    stmt = "DELETE FROM {table} WHERE {pk_column} = {phs}"
    params = {
        "table": sql.Identifier(table),
        "pk_column": sql.Identifier(pk_column),
        "phs": sql.Placeholder(pk_column) if named_phs else sql.Placeholder(),
    }
    if returning:
        stmt += " RETURNING {columns}"
        params["columns"] = csidentifiers(columns)
    return sql.SQL(stmt).format(**params)


def delete_many_by_pk(
    table: str,
    pk_column: str,
    columns: tuple[str, ...],
    returning=False,
    named_phs=True,
) -> sql.Composed:
    stmt = "DELETE FROM {table} WHERE {pk_column} = ANY({phs})"
    params = {
        "table": sql.Identifier(table),
        "pk_column": sql.Identifier(pk_column),
        "phs": sql.Placeholder(pk_column) if named_phs else sql.Placeholder(),
    }
    if returning:
        stmt += " RETURNING {columns}"
        params["columns"] = csidentifiers(columns)
    return sql.SQL(stmt).format(**params)


def delete_by_cpk(
    table: str,
    pk_columns: tuple[str, ...],
    columns: tuple[str, ...],
    returning=False,
    named_phs=True,
) -> sql.Composed:
    stmt = "DELETE FROM {table} WHERE ({pk_columns}) = ({phs})"
    params = {
        "table": sql.Identifier(table),
        "pk_columns": csidentifiers(pk_columns),
        "phs": csplaceholders(pk_columns, named_phs=named_phs),
    }
    if returning:
        stmt += " RETURNING {columns}"
        params["columns"] = csidentifiers(columns)
    return sql.SQL(stmt).format(**params)


def delete_many_by_cpk(
    table: str,
    pk_columns: tuple[str, ...],
    columns: tuple[str, ...],
    qty=1,
    returning=False,
) -> sql.Composed:
    stmt = "DELETE FROM {table} WHERE ({pk_columns}) IN ({phs})"
    params = {
        "table": sql.Identifier(table),
        "pk_columns": csidentifiers(pk_columns),
        "phs": rows_with_phs(qty, len(pk_columns)),
    }
    if returning:
        stmt += " RETURNING {returning}"
        params["returning"] = csidentifiers(columns)
    return sql.SQL(stmt).format(**params)
