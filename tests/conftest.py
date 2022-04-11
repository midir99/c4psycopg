import pathlib

import pytest_postgresql.factories

SCHEMA_PATH = pathlib.Path(__file__).parent / "sql/schema.sql"

cpostgresql = pytest_postgresql.factories.postgresql(
    "postgresql_proc",
    load=[
        str(SCHEMA_PATH),
    ],
)
