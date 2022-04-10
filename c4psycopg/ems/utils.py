import functools
from collections.abc import Awaitable
from typing import Any, Callable, TypeVar

import psycopg
import psycopg.rows
from typing_extensions import Concatenate, ParamSpec

from . import base

P = ParamSpec("P")
T = TypeVar("T")


def entity2tuple(
    columns: tuple[str, ...], entity: base.Entity, incomplete=False
) -> tuple[Any, ...]:
    """Returns a tuple containing the values of the Entity ordered according to columns.

    Args:
      columns: The columns of the Entity.
      entity: An Entity (c4psycopg.em.base.Entity).
      incomplete: If entity does not contain all the columns specified in columns, and
        this incomplete=False this will raise a KeyError exception, but if
        incomplete=True the exception won't be raised but the resulting tuple will not
        contain the values for the columns that did not existed in entity.

    Returns:
      A tuple containing the values of the Entity ordered by columns. Here is an
      example:

      Example 1

        columns = ("customerid", "email", "first_name", "last_name")
        entity = {
            "first_name": "Helena",
            "email": "hjimenez@example.com",
            "last_name": "Jimenez",
            "customerid": 7821,
        }
        values = entity2tuple(columns, entity, incomplete=False)
        print(values)

        Result:
        (7821, 'hjimenez@example.com', 'Helena', 'Jimenez')

      Example 2

        columns = ("customerid", "email", "first_name", "last_name")
        entity = {
            "first_name": "Helena",
            "email": "hjimenez@example.com",
            "customerid": 7821,
        }
        values = entity2tuple(columns, entity, incomplete=False)
        print(values)

        Result:
        A KeyError exception would be thrown, since entity does not contain all the
        required columns.

      Example 3

        columns = ("customerid", "email", "first_name", "last_name")
        entity = {
            "last_name": "Jimenez",
            "email": "hjimenez@example.com",
        }
        values = entity2tuple(columns, entity, incomplete=True)
        print(values)

        Result:
        ('hjimenez@example.com', 'Jimenez')

    Raises:
      KeyError, check description of incomplete argument for more details.
    """
    if incomplete:
        return functools.reduce(
            lambda t, e: t + ((entity[e],) if e in entity else tuple()),
            columns,
            tuple(),
        )
    return functools.reduce(lambda t, e: t + (entity[e],), columns, tuple())


def default_row_factory(
    func: Callable[Concatenate[base.EMProto, P], T],
) -> Callable[Concatenate[base.EMProto, P], T]:
    """
    Passes the EMProto.row_factory to the keyword argument "row_factory" of the
    decorated function if it is None. If EMProto.row_factory is None,
    psycopg.rows.dict_row is passed instead.

    Args:
      func: A method of an EMProto instance.

    Returns:
      A decorated function that ensures that a row_factory is passed in its
      arguments.
    """

    @functools.wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
        if kwargs.get("row_factory") is None:
            self_ = args[0]
            kwargs["row_factory"] = self_.row_factory or psycopg.rows.dict_row
        return func(*args, **kwargs)

    return wrapper


def async_default_row_factory(
    func: Callable[Concatenate[base.EMProto, P], Awaitable[T]],
) -> Callable[Concatenate[base.EMProto, P], Awaitable[T]]:
    """
    Passes the EMProto.row_factory to the keyword argument "row_factory" of the
    decorated function if it is None. If EMProto.row_factory is None,
    psycopg.rows.dict_row is passed instead. This is the asynchronous version.

    Args:
      func: An async method of an EMProto instance.

    Returns:
      A decorated async function that ensures that a row_factory is passed in its
      arguments.
    """

    @functools.wraps(func)
    async def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
        if kwargs.get("row_factory") is None:
            self_ = args[0]
            kwargs["row_factory"] = self_.row_factory or psycopg.rows.dict_row
        return await func(*args, **kwargs)

    return wrapper


def missing_columns(
    incomplete_entity: base.Entity,
    entity_columns: tuple[str, ...],
) -> tuple[str, ...]:
    """Returns the names of the columns that are not in incomplete_entity.

    Args:
      incomplete_entity: An incomplete Entity (c4psycopg.em.base.Entity), by incomplete
        I mean that it does not contain all the columns defined in its EntityManager.
      entity_columns: A tuple containing all the names of the columns of the Entity.

    Returns:
      A tuple containig the names of the columns that are not in the incomplete_entity.
    """
    return tuple(column for column in entity_columns if column not in incomplete_entity)


def missing_values(
    incomplete_entity: base.Entity,
    entity_columns: tuple[str, ...],
    entity_defaults: dict[str, Callable[..., Any]],
) -> base.Entity:
    """
    Returns an Entity (c4psycopg.em.base.Entity) with the missing columns of another
    Entity.

    Args:
      incomplete_entity: An incomplete Entity, by incomplete I mean that it
        does not contain all the columns defined in its EntityManager.
      entity_columns: A tuple containing all the columns of the Entity.
      entity_defaults: A dict containing the names of the columns as keys and callables
        that return the default value that column would have.

    Returns:
      An Entity with the missing columns of incomplete_entity.
    """
    return {
        column: entity_defaults[column]()
        for column in entity_columns
        if column not in incomplete_entity and column in entity_defaults
    }
