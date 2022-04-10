import psycopg.rows
import pytest

from c4psycopg.ems.utils import (entity2tuple, default_row_factory, async_default_row_factory, missing_columns, missing_values)


@pytest.mark.parametrize(
    "columns,entity,incomplete,expected,exception",
    [
        (
            ("customerid", "email", "first_name", "last_name"),
            {
                "first_name": "Helena",
                "email": "hjimenez@example.com",
                "last_name": "Jimenez",
                "customerid": 7821,
            },
            False,
            (7821, "hjimenez@example.com", "Helena", "Jimenez"),
            False,
        ),
        (
            ("customerid", "email", "first_name", "last_name"),
            {
                "first_name": "Helena",
                "email": "hjimenez@example.com",
                "customerid": 7821,
            },
            False,
            None,
            True,

        ),
        (
            ("customerid", "email", "first_name", "last_name"),
            {
                "last_name": "Jimenez",
                "email": "hjimenez@example.com",
            },
            True,
            ("hjimenez@example.com", "Jimenez"),
            False,
        ),
    ],
)
def test_entity2tuple(columns, entity, incomplete, expected, exception):
    if exception:
        with pytest.raises(KeyError):
            entity2tuple(columns, entity, incomplete=incomplete)
    else:
        result = entity2tuple(columns, entity, incomplete=incomplete)
        assert result == expected


def test_default_row_factory():
    class FooEntityManager:
        def __init__(self):
            self.row_factory = None

        @default_row_factory
        def foo(self, row_factory=None):
            assert row_factory is psycopg.rows.dict_row

    fem = FooEntityManager()
    fem.foo()

    def tuple_row():
        pass

    class BarEntityManager:
        def __init__(self, row_factory):
            self.row_factory = row_factory

        @default_row_factory
        def foo(self, row_factory=None):
            assert row_factory is tuple_row

    bem = BarEntityManager(tuple_row)
    bem.foo()

@pytest.mark.asyncio
async def test_async_default_row_factory():
    class FooEntityManager:
        def __init__(self):
            self.row_factory = None

        @async_default_row_factory
        async def foo(self, row_factory=None):
            assert row_factory is psycopg.rows.dict_row

    fem = FooEntityManager()
    await fem.foo()

    def tuple_row():
        pass

    class BarEntityManager:
        def __init__(self, row_factory):
            self.row_factory = row_factory

        @async_default_row_factory
        async def foo(self, row_factory=None):
            assert row_factory is tuple_row

    bem = BarEntityManager(tuple_row)
    await bem.foo()


@pytest.mark.parametrize(
    "incomplete_entity,entity_columns,expected",
    [
        (
            {
                "customerid": 5413,
                "email": "hjimenez@example.com",
                "first_name": "Helena",
                "last_name": "Jimenez",
                "country": "Mexico",
                "age": 32,
            },
            ("customerid", "email", "first_name", "last_name", "country", "age"),
            (),
        ),
        (
            {
                "customerid": 5413,
                "last_name": "Jimenez",
                "country": "Mexico",
            },
            ("customerid", "email", "first_name", "last_name", "country", "age"),
            ("email", "first_name", "age"),
        ),
        (
            {
                "esclavo": "amo",
            },
            ("customerid", "email", "first_name", "last_name", "country", "age"),
            ("customerid", "email", "first_name", "last_name", "country", "age"),
        ),
    ],
)
def test_missing_columns(incomplete_entity, entity_columns, expected):
    result = missing_columns(incomplete_entity, entity_columns)
    assert result == expected


@pytest.mark.parametrize(
    "incomplete_entity,entity_columns,entity_defaults,expected",
    [
        (
            {
                "customerid": 5413,
                "email": "hjimenez@example.com",
                "first_name": "Helena",
                "last_name": "Jimenez",
                "country": "Mexico",
                "age": 32,
            },
            ("customerid", "email", "first_name", "last_name", "country", "age"),
            {
                "customerid": lambda: 500,
                "email": lambda: "Unknown email",
                "first_name": lambda: "Unknown first name",
                "last_name": lambda: "Unknown last name",
                "country": lambda: "Unknown country",
                "age": lambda: "Unknown age",
            },
            {},
        ),
        (
            {
                "customerid": 5413,
                "email": "hjimenez@example.com",
                "first_name": "Helena",
                "last_name": "Jimenez",
            },
            ("customerid", "email", "first_name", "last_name", "country", "age"),
            {
                "customerid": lambda: 500,
                "email": lambda: "Unknown email",
                "first_name": lambda: "Unknown first name",
                "last_name": lambda: "Unknown last name",
                "country": lambda: "Unknown country",
                "age": lambda: "Unknown age",
            },
            {
                "country": "Unknown country",
                "age": "Unknown age",
            },
        ),
        (
            {
                "first_name": "Helena",
                "last_name": "Jimenez",
                "country": "Mexico",
                "age": 32,
            },
            ("customerid", "email", "first_name", "last_name", "country", "age"),
            {
                "email": lambda: "Unknown email",
                "first_name": lambda: "Unknown first name",
                "last_name": lambda: "Unknown last name",
                "country": lambda: "Unknown country",
                "age": lambda: "Unknown age",
            },
            {
                "email": "Unknown email",
            },
        ),
    ],
)
def test_missing_values(incomplete_entity, entity_columns, entity_defaults, expected):
    result = missing_values(incomplete_entity, entity_columns, entity_defaults)
    assert result == expected
