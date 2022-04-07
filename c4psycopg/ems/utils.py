import functools
from collections.abc import Container
from typing import Any, Callable, TypeVar

from psycopg import sql
from typing_extensions import Concatenate, ParamSpec

from . import base

P = ParamSpec("P")
T = TypeVar("T")


def entity2tuple(columns: tuple[str, ...], entity: base.Entity) -> tuple[Any, ...]:
    return functools.reduce(lambda t, e: t + (entity[e],), columns, tuple())


def default_row_factory(func: Callable[Concatenate[base.EMProto, P], T]):
    """
    Passes the EMProto.row_factory to the keyword argument "row_factory" of the
    decorated function if it is None.
    """

    @functools.wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
        if kwargs.get("row_factory") is None:
            self_ = args[0]
            kwargs["row_factory"] = self_.row_factory
        return func(*args, *kwargs)

    return wrapper


def async_default_row_factory(func: Callable[Concatenate[base.EMProto, P], T]):
    """
    Passes the EMProto.row_factory to the keyword argument "row_factory" of the
    decorated function if it is None.
    """

    @functools.wraps(func)
    async def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
        if kwargs.get("row_factory") is None:
            self_ = args[0]
            kwargs["row_factory"] = self_.row_factory
        return await func(*args, *kwargs)

    return wrapper


def entity_defaults(
    columns: tuple[str, ...],
    entity_columns: Container[str],
) -> dict[str, sql.SQL]:
    """
    Generates a dict, the keys are the entity_columns elements that are not in columns,
    the value for each key is a psycopg.sql.DEFAULT.

    In EMProto methods such as create and create_many, the arguments entity and entities
    can be passed to these methods without some required columns that the generated
    query needs to insert them in the database. E.g.:

    Method create of EMProto:
    EMProto.create(self, entity, conn, *, row_factory=None)

    Generated query:
    INSERTO INTO "customer" ("customerid","name","email") VALUES
    (%(customerid)s,%(name)s,%(email)s)

    How user calls EMProto.create:
    entity = {"name": "Mr. Ditkovich", "email": "dit@example.com"}
    EMProto.create(entity, conn)

    In this case, the user missed the column "customerid" in the dictionary to create
    the entity, this would generate an exception when executing since "customerid" is
    missing and the query requires it, here is where entity_defaults solves the problem:

    defaults = entity_defaults(EMProto.columns, entity.keys())
    print(defaults)

    Result:
    {"customerid": psycopg.sql.DEFAULT}

    Now the entity dict can be updated with defaults dict and it would be ready to be
    used with EMProto.create.

    Args:
      columns: A tuple containing the names of the columns of the entity.
      entity_columns: An Iterable containing some of the names of the colums of the
        entity.

    Returns:
      A dict, the keys are the entity_columns elements that are not in columns,
      the value for each key is a psycopg.sql.DEFAULT.
    """
    return {column: sql.DEFAULT for column in columns if column not in entity_columns}
