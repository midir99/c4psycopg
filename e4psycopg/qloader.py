"""Utilities to load psycopg-formatted SQL queries from .sql files.

SQL queries can be written in .sql files following the next rules:

1) The query name must be written in a SQL comment starting with "name:".
2) The query can contain comments, these comments must be under the query name.
3) The actual query must be below the comments (if there are comments), it can contain
   placeholders, these must follow the rule that psycopg states for them, for instance:
   - normal placeholder: %s
   - named placeholder: %(named_argument)s

Please, check how to define queries with arguments in psycopg, this is the
documentation: https://www.psycopg.org/psycopg3/docs/basic/params.html

Here are some examples of how to define queries in .sql files:

-- name: find-customer-by-id
-- This query finds a customer by their id field.
SELECT id, first_name, email FROM customer WHERE id = %(customer_id)s;

-- name: update-store-name-by-id
-- This query updates the field "name" of a store entity.
UPDATE store SET name = %(name)s WHERE id = %(store_id)s;

-- name: insert-customer
INSERT INTO customer (first_name, email) VALUES (%s, %s);
"""

from collections.abc import Iterable
import re


_QUERY_NAME_DEFINITION_PATTERN = re.compile(r"--\s*name\s*:\s*")
_DOC_COMMENT_PATTERN = re.compile(r"\s*--\s*(.*)$")
_VALID_QUERY_NAME_PATTERN = re.compile(r"^\w+$")


def _extract_sql(lines: Iterable[str]) -> str:
    """Extracts the SQL query code from a list of strings containing SQL code.

    Here you can see what part is extracted:

    -- name: find-customer-by-id
    -- This query finds a customer by their id field.
    SELECT id,                                         <--+
           first_name,                                    |
           email                                          +- Extracts this part of the
      FROM customer                                       |  code.
     WHERE id = %(customer_id)s;                       <--+

    Args:
      lines: A list of strings containing the SQL code. The name of the query must not
        be included in these lines.

    Returns:
      A string containing the SQL code.
    """
    return "".join(
        f"{sql_line}\n" for sql_line in lines if not _DOC_COMMENT_PATTERN.match(sql_line)
    )


def _extract_query_name(sql_code: str) -> str:
    """Extracts the query name from a string containing SQL code.

    Here you can see what part is extracted:

             Extracts this part
             of the code.
                      |
             +--------+--------+
             |                 |
             v                 v
    -- name: find-customer-by-id
    -- This query finds a customer by their id field.
    SELECT id,
           first_name,
           email
      FROM customer
     WHERE id = %(customer_id)s;

    Args:
      sql_code: A string containing the name of the query.

    Returns:
        A string formed using the name of the query that can be used as a valid Python
        identifier.

    Raises:
       ValueError: If the query name cannot be translated into a valid Python
         identifier.
    """
    query_name = sql_code.replace("-", "_")
    if not _VALID_QUERY_NAME_PATTERN.match(query_name):
        raise ValueError(
            f'name must convert to valid Python identifier, got "{query_name}".'
        )
    return query_name


def from_str(sql_code: str) -> dict[str, str]:
    """Loads SQL queries from a string.

    Args:
      sql_code: A string containing named SQL queries.

    Returns:
      A dict where the keys are the query names and the values are the SQL code of each
      query. Here is an example:

      sql_stmts = '''
          -- name: find-customer-by-id
          -- This query finds a customer by their id field.
          SELECT id, first_name, email FROM customer WHERE id = %(customer_id)s;

          -- name: update-store-name-by-id
          -- This query updates the field "name" of a store entity.
          UPDATE store SET name = %(name)s WHERE id = %(store_id)s;
      '''
      queries = from_str(sql_stmts)
      print(queries["find_customer_by_id"])  # Note that we are using underscores.
      print(queries["update_store_name_by_id"])

      Result:
      SELECT id, first_name, email FROM customer WHERE id = %(customer_id)s;
      UPDATE store SET name = %(name)s WHERE id = %(store_id)s;
    """
    sql_queries = {}
    stmts = _QUERY_NAME_DEFINITION_PATTERN.split(sql_code)
    for query_str in stmts[1:]:
        lines = [line.strip() for line in query_str.strip().splitlines()]
        query_name = _extract_query_name(lines[0])
        sql_query = _extract_sql(lines[1:])
        sql_queries[query_name] = sql_query

    return sql_queries


def from_file(file_path: str, encoding="UTF-8") -> dict[str, str]:
    """Loads SQL queries from a .sql file.

    Args:
      file_path: The file path of the .sql file.

    Returns:
      A dict where the keys are the query names and the values are the SQL code of each
      query. Here is an example:

      queries.sql:
      -- name: find-customer-by-id
      -- This query finds a customer by their id field.
      SELECT id, first_name, email FROM customer WHERE id = %(customer_id)s;

      -- name: update-store-name-by-id
      -- This query updates the field "name" of a store entity.
      UPDATE store SET name = %(name)s WHERE id = %(store_id)s;

      load_queries.py:
      queries = load_from_file('queries.sql')
      print(queries["find_customer_by_id"])  # Note that we are using underscores.
      print(queries["update_store_name_by_id"])

      Result:
      SELECT id, first_name, email FROM customer WHERE id = %(customer_id)s;
      UPDATE store SET name = %(name)s WHERE id = %(store_id)s;
    """
    with open(file_path, "rt", encoding=encoding) as file:
        sql_text = file.read()
    return from_str(sql_text)


def from_path() -> dict[str, str]:
    # TODO
    ...
