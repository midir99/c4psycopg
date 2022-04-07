"""e4psycopg - entity managers 4 pycopg.

Generate "Entity Managers" for your database entities and load psycopg-formatted SQL
queries from .sql files.
"""

from .gqueries import (
    rows_with_phs,
    csplaceholders,
    csidentifiers,
    insert,
    insert_many,
    select_by_pk,
    select_many_by_pk,
    select_by_cpk,
    select_many_by_cpk,
)
from .qloader import from_str, from_file, from_path


__all__ = (
    "from_str",
    "from_file",
    "from_path",
    "rows_with_phs",
    "csplaceholders",
    "csidentifiers",
    "insert",
    "insert_many",
    "select_by_pk",
    "select_many_by_pk",
    "select_by_cpk",
    "select_many_by_cpk",
)
