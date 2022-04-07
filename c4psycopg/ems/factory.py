from typing import Union

from . import base, cpkem, em


def entitymanager(
    table: str, pk: Union[str, tuple[str, ...]], columns: tuple[str, ...], async_=False
) -> base.EMProto:
    ...
