import pytest_postgresql.factories

cpostgresql = pytest_postgresql.factories.postgresql(
    "custom_postgresql",
    load=[
        "sql/schema.sql",
    ],
)
