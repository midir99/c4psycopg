import functools
from collections.abc import Awaitable, Container
from typing import Any, Callable, TypeVar

import psycopg
import psycopg.rows
from psycopg import sql
from typing_extensions import Concatenate, ParamSpec

from . import base

P = ParamSpec("P")
T = TypeVar("T")


def entity2tuple(
    columns: tuple[str, ...], entity: base.Entity, incomplete=False
) -> tuple[Any, ...]:
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
    decorated function if it is None.
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
    decorated function if it is None.
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
