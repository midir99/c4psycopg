import functools
from typing import Iterable, Literal, Optional

from psycopg import sql

COLUMNS_ORDER = Literal["ASC", "DESC"]
NULLS_ORDER = Literal["FIRST", "LAST"]


def commas(*composed_list: sql.Composed) -> sql.Composed:
    """Separates psycopg.sql.Composed with commas.

    Args:
      *composed_list: A list with psycopg.sql.Composed items.

    Returns:
      A list of psycopg.sql.Composed items separated with commas.
    """
    return sql.SQL(",").join(composed_list)


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
def rows_with_phs_and_defaults(
    columns: tuple[str, ...],
    defaults_per_entity: tuple[tuple[int, ...], ...],
) -> sql.Composed:
    """Returns PostgreSQL ROWs literals with placeholders and DEFAULT keywords.

    Args:
      columns: A tuple containing the names of the columns.
      defaults_per_entity: A tuple containing tuples, each tuple must contain the column
        names that will be replaced with placeholders, otherwise they will be replaced
        by the DEFAULT keyword.

    Returns:
      A psycopg.sql.Composed, here you can see an example:

      columns = ("customerid", "name", "email")
      defaults_per_entity = (
          ("customerid",),
          ("email", "customerid"),
          (),
      )
      composed = rows_with_phs_and_defaults(columns, defaults_per_entity)
      print(composed.as_string(conn))

      Result:
      (DEFAULT,%s,%s),(DEFAULT,%s,DEFAULT),(%s,%s,%s)
    """
    return sql.SQL(",").join(
        (
            sql.SQL("({})").format(
                sql.SQL(",").join(
                    (
                        sql.SQL("DEFAULT") if column in defaults else sql.Placeholder()
                        for column in columns
                    )
                )
            )
            for defaults in defaults_per_entity
        )
    )


@functools.cache
def columns_with_phs(columns: tuple[str, ...], named_phs=True) -> sql.Composed:
    """Generates a psycopg.sql.Composed like "col0"=%(col0)s,"col1"=%(col1)s,...

    Args:
      columns: A tuple containing the names of the columns that will be used to generate
        the psycopg.sql.Composed.
      named_phs: If True placeholders will be named placeholders (%(col0)s),
        otherwise they will be simple placeholders (%s).

    Returns:
      A psycopg.sql.Composed, here you can see an example:

      columns = ("username", "email", "first_name")
      query1 = columns_with_phs(columns)
      query2 = columns_with_phs(columns, named_phs=False)
      print(query1.as_string(conn))
      print(query2.as_string(conn))

      Result:
      "username"=%(username)s,"email"=%(email)s,"first_name"=%(first_name)s
      "username"=%s,"email"=%s,"first_name"=%s
    """
    return sql.SQL(",").join(
        (
            sql.SQL("{}={}").format(
                sql.Identifier(column),
                sql.Placeholder(column) if named_phs else sql.Placeholder(),
            )
            for column in columns
        )
    )


@functools.cache
def csplaceholders(
    columns: tuple[str],
    defaults: Optional[tuple[str]] = None,
    named_phs=True,
) -> sql.Composed:
    """
    Generates a psycopg.sql.Composed of Comma Separated Placeholders and PostgreSQL
    DEFAULT keywords.

    Args:
      columns: An iterable containing the names of columns.
      defaults: An iterable containing the names of the columns that will be replaced
        with a PostgreSQL DEFAULT keyword instead of a placeholder.
      named_phs: If True placeholders will be named placeholders (%(username)s),
        otherwise they will be simple placeholders (%s).

    Returns:
      A psycopg.sql.Composed with placeholders and DEFAULT keywords, here you can see an
      example:

      ids1 = csidentifiers(("username", "email"), named_phs=True)
      ids2 = csidentifiers(("username", "email"), named_phs=False)
      ids3 = csidentifiers(("username", "email"), defaults=("email",), named_phs=True)
      print(ids1.as_string(conn))
      print(ids2.as_string(conn))
      print(ids3.as_string(conn))

      Result:
      %(username)s,%(email)s
      %s,%s
      %(username)s,DEFAULT
    """
    if defaults is None:
        defaults = ()
    if named_phs:
        return sql.SQL(",").join(
            (
                sql.SQL("DEFAULT") if column in defaults else sql.Placeholder(column)
                for column in columns
            )
        )
    return sql.SQL(",").join(
        (
            sql.SQL("DEFAULT") if column in defaults else sql.Placeholder()
            for column in columns
        )
    )


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


@functools.cache
def insert(
    table: str,
    columns: tuple[str, ...],
    defaults: Optional[tuple[str, ...]] = None,
    returning=False,
    named_phs=True,
) -> sql.Composed:
    stmt = "INSERT INTO {table} ({columns}) VALUES ({phs})"
    params = {
        "table": sql.Identifier(table),
        "columns": csidentifiers(columns),
        "phs": csplaceholders(columns, defaults=defaults, named_phs=named_phs),
    }
    if returning:
        stmt += " RETURNING {columns}"
    return sql.SQL(stmt).format(**params)


def insert_many(
    table: str,
    columns: tuple[str, ...],
    qty=1,
    defaults_per_entity: Optional[tuple[tuple[str, ...], ...]] = None,
    returning=False,
) -> sql.Composed:
    stmt = "INSERT INTO {table} ({columns}) VALUES {phs}"
    params = {
        "table": sql.Identifier(table),
        "columns": csidentifiers(columns),
        "phs": rows_with_phs_and_defaults(columns, defaults_per_entity)
        if defaults_per_entity
        else rows_with_phs(qty, len(columns)),
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


# def select_many(
#     table: str,
#     pk_column: str,
#     columns: tuple[str, ...],
#     order_by: Optional[sql.Composable] = None,
#     limit: Optional[int] = None,
#     offset: Optional[int] = None,
#     named_phs=True,
# ) -> sql.Composed:
#     """
#     It returns more than 1 record, so records must be:
#     - orderable
#     - paginable
#     """


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


def update_by_pk(
    table: str,
    pk_column: str,
    columns: tuple[str, ...],
    returning=False,
    named_phs=True,
) -> sql.Composed:
    stmt = "UPDATE {table} SET {columns_values} WHERE {pk_column} = {phs}"
    params = {
        "table": sql.Identifier(table),
        "columns_values": columns_with_phs(columns, named_phs=named_phs),
        "pk_column": sql.Identifier(pk_column),
        "phs": sql.Placeholder(pk_column) if named_phs else sql.Placeholder(),
    }
    if returning:
        stmt += " RETURNING {returning}"
        params["returning"] = csidentifiers(columns)
    return sql.SQL(stmt).format(**params)


def update_many_by_pk(
    table: str,
    pk_column: str,
    columns: tuple[str, ...],
    returning=False,
    named_phs=True,
) -> sql.Composed:
    stmt = "UPDATE {table} SET {columns_values} WHERE {pk_column} = ANY({phs})"
    params = {
        "table": sql.Identifier(table),
        "columns_values": columns_with_phs(columns, named_phs=named_phs),
        "pk_column": sql.Identifier(pk_column),
        "phs": sql.Placeholder(pk_column) if named_phs else sql.Placeholder(),
    }
    if returning:
        stmt += " RETURNING {returning}"
        params["returning"] = csidentifiers(columns)
    return sql.SQL(stmt).format(**params)


def mupdate_many_by_pk():
    pass


def update_by_cpk():
    pass


def update_many_by_cpk():
    pass


def mupdate_many_by_cpk():
    pass


# You can use the update with to update many entities, to do the mupdate
# WITH update_data AS (
# 	VALUES (3, 'Luisa Tamales', 31) --,
# 	       -- (2, 'Georgina Marrazo', 67)
# )
# UPDATE customer
#    SET name = update_data.column2,
# 	   age = update_data.column3
#    FROM update_data
#   WHERE customerid = update_data.column1
# RETURNING customer.*;


# THERE MUST BE 2 TYPES OF update_many
# INDIVIDUAL update, you pass all the entities, the update is realized using the entity
# pk, individual update methods will be
# NORMAL update, you pass all the ids and only 1 entity, all the identities that have
# those ids must be updated with the values of that body


def delete_by_pk(
    table: str,
    pk_column: str,
    returning=False,
    columns: Optional[tuple[str, ...]] = None,
    named_phs=True,
) -> sql.Composed:
    stmt = "DELETE FROM {table} WHERE {pk_column} = {phs}"
    params = {
        "table": sql.Identifier(table),
        "pk_column": sql.Identifier(pk_column),
        "phs": sql.Placeholder(pk_column) if named_phs else sql.Placeholder(),
    }
    if returning:
        if columns is None:
            raise ValueError("If returning is True, columns must be specified.")
        stmt += " RETURNING {columns}"
        params["columns"] = csidentifiers(columns)
    return sql.SQL(stmt).format(**params)


def delete_many_by_pk(
    table: str,
    pk_column: str,
    returning=False,
    columns: Optional[tuple[str, ...]] = None,
    named_phs=True,
) -> sql.Composed:
    stmt = "DELETE FROM {table} WHERE {pk_column} = ANY({phs})"
    params = {
        "table": sql.Identifier(table),
        "pk_column": sql.Identifier(pk_column),
        "phs": sql.Placeholder(pk_column) if named_phs else sql.Placeholder(),
    }
    if returning:
        if columns is None:
            raise ValueError("If returning is True, columns must be specified.")
        stmt += " RETURNING {columns}"
        params["columns"] = csidentifiers(columns)
    return sql.SQL(stmt).format(**params)


def delete_by_cpk(
    table: str,
    pk_columns: tuple[str, ...],
    returning=False,
    columns: Optional[tuple[str, ...]] = None,
    named_phs=True,
) -> sql.Composed:
    stmt = "DELETE FROM {table} WHERE ({pk_columns}) = ({phs})"
    params = {
        "table": sql.Identifier(table),
        "pk_columns": csidentifiers(pk_columns),
        "phs": csplaceholders(pk_columns, named_phs=named_phs),
    }
    if returning:
        if columns is None:
            raise ValueError("If returning is True, columns must be specified.")
        stmt += " RETURNING {columns}"
        params["columns"] = csidentifiers(columns)
    return sql.SQL(stmt).format(**params)


def delete_many_by_cpk(
    table: str,
    pk_columns: tuple[str, ...],
    qty=1,
    returning=False,
    columns: Optional[tuple[str, ...]] = None,
) -> sql.Composed:
    stmt = "DELETE FROM {table} WHERE ({pk_columns}) IN ({phs})"
    params = {
        "table": sql.Identifier(table),
        "pk_columns": csidentifiers(pk_columns),
        "phs": rows_with_phs(qty, len(pk_columns)),
    }
    if returning:
        if columns is None:
            raise ValueError("If returning is True, columns must be specified.")
        stmt += " RETURNING {returning}"
        params["returning"] = csidentifiers(columns)
    return sql.SQL(stmt).format(**params)
