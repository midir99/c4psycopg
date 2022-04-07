from collections.abc import Iterable
import functools
from typing import Any, Callable, ClassVar, Optional, Type, TypeVar, Union

from typing_extensions import ParamSpec, Concatenate

try:
    import psycopg
    import psycopg.rows
    from psycopg import sql
except ImportError as e:
    if "No module named: psycopg" in str(e):
        raise ImportError("psycopg must be installed to use this library.") from e
    raise e from e

T = TypeVar("T")
P = ParamSpec('P')


# TODO: add an optional where to delete, update, select
# TODO: "many" queries create many dicts because they need to insert data, maybe we could
# optimi that part using tuples
# TODO: add the row factory as an attribute to each function
# TODO: create and create_many function have 2 responsabilities: adding defaults and saving object,
# create a decorator to add defaults to entities
# TODO: check what is faster execute_many or using a BIG sql query with
# TODO: we will use IN in select many, so query must be generated
# TODO: test if we can use tuples with POSTGRES IN
# TODO: consider using pk instead of id
# TODO: extract static methods from EM
# TODO: consider separating EM pk and EM composite pk, maybe EM composite pk can inherit EM pk
# TODO: think about a way to support COPY to create many entities
# TODO: evaluate generators vs tuples when using many operations
# TODO: add many validations in the init method
# TODO: add a attribute: row_factory, and add it as a parameter in every function,
# if row_facotry in fucntion is None, use the class attribute
# psycopg.rows.RowFactory and psycopg.rows.RowFactory
# TODO: find_many_by_id must offer a where, offset and limit clauses, also order by
# TODO: switch to python 3.10, this library will only work on Python 3.10

EntityData = dict[str, Any]
ID = Union[str, int]


def add_defaults(columns: Iterable[str], entity_keys: Iterable[str]) -> dict[str, sql.SQL]:
    return {column: sql.DEFAULT for column in columns if column not in entity_keys}


def id2dict(
    entity_id: tuple[str, ...],
    id_: Union[ID, tuple[ID, ...]]
) -> dict[str, Union[int, str]]:
    if isinstance(id_, (int, str)):
        new_id = {entity_id[0]: id_}
    if isinstance(id_, tuple):
        new_id = dict(zip(entity_id, id_))
    return new_id


# def prepare_id_arg(_func=None, id_arg: Union[int, str] = 1):
#     """
#     Decorates a "by_id" EM function so users can call it using an ID type or a
#     tuple[ID, ...].

#     Args:
#       id_arg: An int or a str that indicates what is the id_ argument in the function.
#         If id_arg is an INT, it is assumed that the id_ argument of the decorated
#         function is POSITIONAL and that it is in the id_arg position.
#         If id_arg is a STR, it is assumed that the id_ argument of the decorated
#         function is KEYWORD and that id_arg is the name of that argument.

#     Returns:
#       A function
#     """
#     if isinstance(id_arg, int):
#         def decorator(func):
#             @functools.wraps(func)
#             def wrapper(*args, **kwargs):
#                 if isinstance(args[id_arg], int, str, tuple):
#                     self_ = args[0]
#                     args[id_arg] = id2dict(self_.id_, args[id_arg])
#                 return func(*args, **kwargs)
#             return wrapper
#     else:  # id_arg is str
#         def decorator(func):
#             @functools.wraps(func)
#             def wrapper(*args, **kwargs):
#                 if isinstance(kwargs[id_arg], int, str, tuple):
#                     self_ = args[0]
#                     kwargs[id_arg] = id2dict(self_.id_, kwargs[id_arg])
#                 return func(*args, **kwargs)
#             return wrapper

#     if _func is None:
#         return decorator
#     return decorator(_func)


def prepare_id_arg(func: Callable[Concatenate["EM", ID, P], T]):
    @functools.wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
        if isinstance(args[1], (int, str, tuple)):
            em_instance = args[0]  # self
            args[1] = id2dict(em_instance, args[1])
        return func(*args, *kwargs)
    return wrapper


def prepate_id_list_arg(func: Callable[Concatenate["EM", tuple[ID], P], T]):
    @functools.wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:

        map(lambda id_: id2dict(id_) if isinstance(id_, (int, str, tuple)) else id_)


class EM:
    """(E)ntity (M)anager."""

    __slots__ = (
        "table",
        "pk",
        "columns",
        "_select_by_pk_query",
        "_select_many_by_pk_query",
    )

    def __init__(
        self,
        table: str,
        pk: str,
        columns: tuple[str, ...]
    ) -> None:
        self.table = table
        self.pk = pk
        self.columns = (pk,) + columns

        self._select_by_pk_query =

    def describe(self) -> str:
        """Returns a string that describes the queries used to operate the database."""

    def create(
        self,
        entity: EntityData,
        conn: psycopg.Connection,
        row_factory=psycopg.rows.dict_row,
    ) -> EntityData:
        missing_columns = add_defaults(self.columns, entity.keys())
        entity.update(missing_columns)
        with conn.cursor(row_factory=row_factory) as cur:
            cur.execute(self._insert_returning_query, entity)
            result = cur.fetchone()
            conn.commit()
        return result

    def create_many(
        self,
        entities: Iterable[EntityData],
        conn: psycopg.Connection,
        returning=False,
        row_factory=psycopg.rows.dict_row,
    ) -> Union[int, Iterable[EntityData]]:
        entities = map(
            lambda e: e.update(add_defaults(self.columns, e.keys())), entities
        )
        with conn.cursor(row_factory=row_factory) as cur:
            if returning:
                cur.executemany(self._insert_returning_query, entities)
                result = cur.fetchall()
            else:
                cur.executemany(self._insert_query, entities)
                result = cur.rowcount
            conn.commit()
        return result

    def find_by_id(
        self,
        id_: Union[ID, tuple[ID, ...], dict[str, ID]],
        conn: psycopg.Connection,
        row_factory=psycopg.rows.dict_row,
    ) -> Optional[EntityData]:
        with conn.cursor(row_factory=row_factory) as cur:
            cur.execute(self._select_by_id_query, id_)
            result = cur.fetchone()
        return result

    def find_many_by_id(
        self,
        id_list: Iterable[ID],
        conn: psycopg.Connection,
        row_factory=psycopg.rows.dict_row,
    ) -> Iterable[EntityData]:
        with conn.cursor(row_factory=row_factory) as cur:
            ...



class CPEM(EM):

    def dict2pk(self, dict_: dict[str, Any]) -> tuple[str, ...]:
        for pkc in self.pk:
            dict_[pkc]
        return ()


class Entity(abc.ABC):

    _INSERT_QUERY: ClassVar[Optional[sql.Composed]] = None
    _INSERT_RETURNING_QUERY: ClassVar[Optional[sql.Composed]] = None
    _SELECT_QUERY: ClassVar[Optional[sql.Composed]] = None
    _DELETE_QUERY: ClassVar[Optional[sql.Composed]] = None

    _TABLE: ClassVar[str]
    """The name of the table in the database."""
    _PK: ClassVar[Union[str, tuple[str, ...]]]
    """
    The name of the primary key column, if the primary key is composited, this must be a
    list with the name of the columns that compose the primary key.
    """
    _COLUMNS: ClassVar[tuple[str, ...]]
    """A tuple with the names of the columns of the table in the database."""

    @classmethod
    def count(cls: Type[T]) -> int:
        pass

    @classmethod
    def _init_inserts(cls: Type[T]) -> None:
        if cls._INSERT_QUERY is None:
            stmt = sql.SQL("INSERT INTO {table} ({columns}) VALUES ({placeholders})")
            params = {
                "table": sql.Identifier(cls._TABLE),
                "columns": sql.SQL(", ").join(map(sql.Identifier, cls._COLUMNS)),
                "placeholders": sql.SQL(", ").join(map(sql.Placeholder, cls._COLUMNS)),
            }
            cls._INSERT_QUERY = stmt.format(**params)
        if cls._INSERT_RETURNING_QUERY is None:
            stmt = sql.SQL(
                "INSERT INTO {table} ({columns}) VALUES ({placeholders}) RETURNING "
                "{returning}"
            )
            columns = sql.SQL(", ").join(map(sql.Identifier, cls._COLUMNS))
            params = {
                "table": sql.Identifier(cls._TABLE),
                "columns": columns,
                "placeholders": sql.SQL(", ").join(map(sql.Placeholder, cls._COLUMNS)),
                "returning": columns,
            }
            cls._INSERT_RETURNING_QUERY = stmt.format(**params)

    @classmethod
    def _defaults(cls: Type[T], entity_keys: Iterable[str]) -> dict[str, sql.SQL]:
        return {
            column: sql.DEFAULT for column in cls._COLUMNS if column not in entity_keys
        }

    @classmethod
    def create(cls: Type[T], conn: psycopg.Connection, entity: dict[str, Any]) -> T:
        cls._init_inserts()
        entity.update(cls._defaults(entity.keys()))







    @classmethod
    def create_many(cls: Type[T], entities: Iterable[dict[str, Any]], returning=False) -> Union[Iterable[T], int]:
        pass

    @classmethod
    def find_by_id(cls: Type[T], id_: Any) -> T:
        pass

    @classmethod
    def find_many_by_id(cls: Type[T], id_list: Iterable[Any], returning=False) -> Union[Iterable[T], int]:
        pass

    @classmethod
    def update_by_id(cls: Type[T], id_: Any, entity: dict[str, Any]) -> T:
        pass

    @classmethod
    def update_many_by_id(cls: Type[T], entities: Iterable[dict[str, Any]], returning=False) -> Union[Iterable[T], int]:
        pass

    @classmethod
    def delete_by_id(cls: Type[T], id_: Any) -> T:
        pass

    @classmethod
    def delete_many_by_id(cls: Type[T], id_list: Iterable[Any], returning=False) -> Union[Iterable[T], int]:
        pass
