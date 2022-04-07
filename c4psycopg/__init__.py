"""CRUDs for psycopg!

Automatically generate CRUDs for your database entities and load psycopg-formatted SQL
queries from .sql files.
"""

from . import qloader, queries

__all__ = (
    "qloader",
    "queries",
)
