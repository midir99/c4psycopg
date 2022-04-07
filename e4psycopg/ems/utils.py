import functools
from typing import Any, Callable, TypeVar

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
